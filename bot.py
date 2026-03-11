import re, os, glob, time, threading, traceback
from datetime import datetime
import requests

from config import OWNER_ID, BOT_NAME, TG_API, TOP_N, URL_LIVE, URL_NUMBERS, log
from database import db_get, db_update, db_set, db_all

_sess = requests.Session()
_sess.headers.update({"Content-Type": "application/json"})

_state:      dict           = {}
_state_lock: threading.Lock = threading.Lock()

def esc(t) -> str:
    return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def tg_post(ep: str, data: dict, timeout: int = 12):
    for i in range(3):
        try:
            r = _sess.post(f"{TG_API}/{ep}", json=data, timeout=timeout)
            if r.ok:
                return r.json()
            if r.status_code == 429:
                time.sleep(r.json().get("parameters", {}).get("retry_after", 3))
                continue
            return r.json()
        except Exception:
            if i < 2:
                time.sleep(0.5 * (i + 1))
    return None

def send_msg(cid, text: str, markup=None) -> int | None:
    p = {"chat_id": str(cid), "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if markup:
        p["reply_markup"] = markup
    r = tg_post("sendMessage", p)
    return r["result"]["message_id"] if r and r.get("ok") else None

def edit_msg(cid, mid: int, text: str, markup=None) -> bool:
    p = {"chat_id": str(cid), "message_id": mid, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if markup is not None:
        p["reply_markup"] = markup
    r = tg_post("editMessageText", p)
    return bool(r and (r.get("ok") or "not modified" in str(r).lower()))

def delete_msg(cid, mid: int):
    threading.Thread(target=tg_post, args=("deleteMessage", {"chat_id": str(cid), "message_id": mid}), daemon=True).start()

def answer_cb(cb_id: str, text: str = "", alert: bool = False):
    threading.Thread(target=tg_post, args=("answerCallbackQuery", {"callback_query_id": cb_id, "text": text, "show_alert": alert}), daemon=True).start()

def send_file(cid, path: str, caption: str = "") -> bool:
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        return False
    cap = caption[:1024] if caption else ""
    for i in range(3):
        try:
            with open(path, "rb") as fh:
                fdata = fh.read()
            r = requests.post(
                f"{TG_API}/sendDocument",
                data={"chat_id": str(cid), "caption": cap, "parse_mode": "HTML"},
                files={"document": (os.path.basename(path), fdata, "text/plain")},
                timeout=120)
            if r.ok:
                return True
            if r.status_code == 429:
                time.sleep(r.json().get("parameters", {}).get("retry_after", 5))
                continue
            if r.status_code == 400 and "caption" in r.json().get("description", "").lower():
                cap = ""
                continue
            break
        except Exception as e:
            log.error(f"send_file #{i+1}: {e}")
            if i < 2:
                time.sleep(3)
    return False

def broadcast_all(text: str) -> int:
    count = 0
    for ucid, u in db_all().items():
        if not u.get("banned") and send_msg(ucid, text):
            count += 1
    return count

def check_group_membership(cid) -> tuple[bool, list]:
    from config import REQUIRED_GROUPS
    if not REQUIRED_GROUPS:
        return True, []
    missing = []
    for gid in REQUIRED_GROUPS:
        try:
            r = tg_post("getChatMember", {"chat_id": gid, "user_id": int(cid)})
            if not r or not r.get("ok") or r["result"].get("status", "") in ("left", "kicked", "restricted"):
                missing.append(gid)
        except Exception:
            missing.append(gid)
    return len(missing) == 0, missing

def _get_dash_mid(cid) -> int | None:
    from core import sess_get
    s = sess_get(cid)
    return s.get("last_dash_id") if s else None

def _set_dash_mid(cid, mid):
    from core import sess_get
    s = sess_get(cid)
    if s:
        s["last_dash_id"] = mid

def smart_send(cid, text: str, markup=None) -> int | None:
    mid = _get_dash_mid(cid)
    if mid and edit_msg(cid, mid, text, markup):
        return mid
    _set_dash_mid(cid, None)
    new_mid = send_msg(cid, text, markup)
    if new_mid:
        _set_dash_mid(cid, new_mid)
    return new_mid

def fresh_send(cid, text: str, markup=None) -> int | None:
    mid = _get_dash_mid(cid)
    if mid:
        delete_msg(cid, mid)
        _set_dash_mid(cid, None)
    new_mid = send_msg(cid, text, markup)
    if new_mid:
        _set_dash_mid(cid, new_mid)
    return new_mid

def state_get(cid) -> dict | None:
    with _state_lock:
        return _state.get(str(cid))

def state_set(cid, val: dict):
    with _state_lock:
        _state[str(cid)] = val

def state_del(cid):
    with _state_lock:
        _state.pop(str(cid), None)

def kb(rows: list) -> dict:
    return {"inline_keyboard": [[{"text": lbl, "callback_data": d} for lbl, d in row] for row in rows]}

def kb_main(engine_on: bool, ar_on: bool, fwd_on: bool) -> dict:
    eng_row = [("🛑 Stop Engine", "engine:stop"), ("🔄 Refresh", "m:home")] if engine_on else [("▶️ Start Engine", "engine:start"), ("🔄 Refresh", "m:home")]
    return kb([
        [("📊 Traffic", "m:traffic"), ("💉 Inject", "m:inject")],
        [("📱 Nomor Aktif", "m:numbers"), ("⚙️ Pengaturan", "m:settings")],
        eng_row,
    ])

def kb_settings(ar_on: bool, fwd_on: bool) -> dict:
    return kb([
        [("🤖 AutoRange " + ("✅ ON" if ar_on else "❌ OFF"), "toggle:ar"),
         ("📤 Forward " + ("✅ ON" if fwd_on else "❌ OFF"), "toggle:fwd")],
        [("🗑 Hapus Semua Nomor", "m:deletenum"), ("♻️ Reset Stats", "action:reset")],
        [("◀️ Kembali", "m:home")],
    ])

def kb_forward(fwd_on: bool) -> dict:
    return kb([
        [("🔴 Matikan Forward" if fwd_on else "🟢 Aktifkan Forward", "toggle:fwd")],
        [("✏️ Ganti Target Grup", "fwd:setgroup")],
        [("◀️ Kembali", "m:settings")],
    ])

def kb_inject_qty(rn: str) -> dict:
    return kb([
        [("100", f"inj:{rn}:100"), ("200", f"inj:{rn}:200")],
        [("300", f"inj:{rn}:300"), ("500", f"inj:{rn}:500")],
        [("✏️ Custom", f"inj_custom:{rn}")],
        [("◀️ Kembali", "m:inject")],
    ])

def kb_confirm(yes: str, no: str = "m:home") -> dict:
    return kb([[("✅ Lanjutkan", yes), ("❌ Batal", no)]])

def kb_back(to: str = "m:home") -> dict:
    return kb([[("◀️ Kembali", to)]])

def kb_admin() -> dict:
    return kb([
        [("👥 Users", "adm:users"), ("📊 Stats", "adm:stats")],
        [("📢 Broadcast", "adm:broadcast")],
        [("◀️ Kembali", "m:home")],
    ])

def kb_user_detail(tcid: str, banned: bool) -> dict:
    return kb([
        [("✅ Unban" if banned else "🚫 Ban", f"adm:{'unban' if banned else 'ban'}:{tcid}"),
         ("⛔ Kick Engine", f"adm:kick:{tcid}")],
        [("◀️ Kembali", "adm:users")],
    ])

def _sep() -> str:
    return "━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

def _h(icon: str, title: str) -> str:
    return f"{icon} <b>{title.upper()}</b>\n{_sep()}"

def page_home(cid: str) -> tuple[str, dict]:
    from core import sess_get
    from config import IVASMS_EMAIL
    cid  = str(cid)
    user = db_get(cid)
    s    = sess_get(cid)

    if not user:
        if IVASMS_EMAIL.strip():
            return (
                f"⚡ <b>{BOT_NAME}</b>\n{_sep()}\n\n"
                f"📧 <code>{esc(IVASMS_EMAIL.strip())}</code>\n\n"
                "Tap <b>Start Engine</b> untuk memulai.",
                kb([[("▶️ Start Engine", "engine:start")]])
            )
        return (f"⚡ <b>{BOT_NAME}</b>\n{_sep()}\n\n⚠️ Belum ada akun. Ketik /start untuk setup.", kb([[ ("⚙️ Setup Akun", "m:setup") ]]))

    engine_on = bool(s and s.get("is_logged_in"))
    ar_on     = bool(s and s.get("auto_range_enabled", True))
    fwd_on    = bool(s and s.get("fwd_enabled") and s.get("fwd_group_id"))

    uptime = "─"
    if s and s.get("start_time"):
        delta  = datetime.now() - s["start_time"]
        h, rem = divmod(int(delta.total_seconds()), 3600)
        m      = rem // 60
        uptime = f"{h}j {m}m"

    wa_today = total_msg = 0
    top_rng_name = "─"
    if s:
        with s["data_lock"]:
            wa_today  = sum(s["wa_harian"].values())
            total_msg = sum(s["traffic_counter"].values())
            if s["traffic_counter"]:
                top_rng_name = s["traffic_counter"].most_common(1)[0][0]

    dot     = "🟢" if engine_on else "🔴"
    eng_txt = "ONLINE" if engine_on else "OFFLINE"
    display_email = IVASMS_EMAIL.strip() or user.get("email", "─")

    text = (
        f"⚡ <b>{BOT_NAME}</b>\n"
        f"{_sep()}\n\n"
        f"📧 <code>{esc(display_email)}</code>\n"
        f"{dot} <b>STATUS: {eng_txt}</b> (⏱ {uptime})\n\n"
        f"📊 <b>STATISTIK HARI INI</b>\n"
        f" ├ 💬 Pesan Masuk : <code>{total_msg}</code>\n"
        f" ├ 📱 WA OTP      : <code>{wa_today}</code>\n"
        f" └ 📡 Top Range   : <code>{esc(top_rng_name)}</code>\n\n"
        f"⚙️ <b>MODUL AKTIF</b>\n"
        f" ├ 🤖 AutoRange : {'✅' if ar_on else '❌'}\n"
        f" └ 📤 Forward   : {'✅' if fwd_on else '❌'}"
    )
    return text, kb_main(engine_on, ar_on, fwd_on)

def page_traffic(cid: str) -> tuple[str, dict]:
    from core import sess_get
    s = sess_get(cid)
    if not s:
        return f"{_h('📈', 'TRAFFIC')}\n\n❌ Engine offline.", kb_back()

    with s["data_lock"]:
        counter = dict(s["traffic_counter"])
        harian  = dict(s["wa_harian"])

    if not counter:
        return f"{_h('📈', 'TRAFFIC')}\n\n📭 Belum ada data. Menunggu pesan masuk...", kb_back()

    top      = sorted(counter.items(), key=lambda x: x[1], reverse=True)[:TOP_N]
    total    = sum(counter.values())
    wa_total = sum(harian.values())

    lines = [
        _h("📈", "TRAFFIC MONITOR"),
        f"💬 Total: <code>{total}</code> | 📱 WA OTP: <code>{wa_total}</code>\n",
        f"🏆 <b>TOP {len(top)} RANGE</b>"
    ]
    for i, (rng, cnt) in enumerate(top, 1):
        pct    = int(cnt / total * 100) if total else 0
        filled = round(pct / 10)
        bar    = "■" * filled + "□" * (10 - filled)
        lines.append(
            f"{i}. <b>{esc(rng)}</b>\n"
            f"   └ <code>{bar}</code> {pct}% ({cnt}x)"
        )

    rows = []
    if top:
        rows.append([(f"💉 Inject {top[0][0][:18]}", f"inj_top:{top[0][0]}")])
    rows.append([("🔄 Refresh", "m:traffic"), ("◀️ Kembali", "m:home")])
    return "\n".join(lines), kb(rows)

def page_settings(cid: str) -> tuple[str, dict]:
    from core import sess_get
    from config import IVASMS_EMAIL
    user   = db_get(cid)
    s      = sess_get(cid)
    ar_on  = bool(s and s.get("auto_range_enabled", True))
    fwd_on = bool(s and s.get("fwd_enabled") and s.get("fwd_group_id"))
    fwd_id = (s.get("fwd_group_id") or "─") if s else "─"
    u_apppass     = "••••••••••••••••" if (user and user.get("app_password")) else "─"
    display_email = IVASMS_EMAIL.strip() or (user.get("email", "─") if user else "─")

    text = (
        f"{_h('⚙️', 'PENGATURAN')}\n"
        f"📧 Akun : <code>{esc(display_email)}</code>\n\n"
        f"🤖 AutoRange   : {'✅ ON' if ar_on else '❌ OFF'}\n"
        f"📤 Forward     : {'✅ ON' if fwd_on else '❌ OFF'}\n"
        f"📌 Target Grup : <code>{esc(fwd_id)}</code>"
    )
    return text, kb_settings(ar_on, fwd_on)

def page_forward(cid: str) -> tuple[str, dict]:
    from core import sess_get
    s     = sess_get(cid)
    fwd_on= bool(s and s.get("fwd_enabled") and s.get("fwd_group_id"))
    fwd_id= (s.get("fwd_group_id") or "─") if s else "─"

    text = (
        f"{_h('📤', 'FORWARD OTP')}\n"
        f"Status      : {'✅ <b>AKTIF</b>' if fwd_on else '❌ <b>MATI</b>'}\n"
        f"Target Grup : <code>{esc(fwd_id)}</code>\n\n"
        "<i>Semua OTP WA yang masuk diteruskan otomatis ke grup target.</i>"
    )
    return text, kb_forward(fwd_on)

def page_admin_stats() -> tuple[str, dict]:
    from core import sess_all
    users   = db_all()
    sesi    = sess_all()
    total   = len(users)
    banned  = sum(1 for u in users.values() if u.get("banned"))
    online  = sum(1 for s in sesi.values() if s.get("is_logged_in"))

    text = (
        f"{_h('👑', 'ADMIN PANEL')}\n"
        f"👥 Total User : <code>{total}</code>\n"
        f"🟢 Online     : <code>{online}</code>\n"
        f"✅ Aktif      : <code>{total - banned}</code>\n"
        f"🚫 Suspended  : <code>{banned}</code>"
    )
    return text, kb_admin()

def page_user_list() -> tuple[str, dict]:
    from core import sess_all
    users = db_all()
    sesi  = sess_all()

    if not users:
        return f"{_h('👥', 'DAFTAR USER')}\n\nBelum ada user.", kb_back("adm:stats")

    lines = [_h("👥", "DAFTAR USER")]
    rows  = []
    for i, (cid, u) in enumerate(users.items(), 1):
        s     = sesi.get(cid)
        on    = s and s.get("is_logged_in")
        dot   = "🟢" if on else ("🚫" if u.get("banned") else "🔴")
        name  = esc(u.get("name", cid))[:18]
        lines.append(f"{i}. {dot} <b>{name}</b> (<code>{cid}</code>)")
        rows.append([(f"{dot} {name}", f"adm:detail:{cid}")])
    rows.append([("◀️ Kembali", "adm:stats")])
    return "\n".join(lines), kb(rows)

def page_user_detail(tcid: str) -> tuple[str, dict]:
    from core import sess_all
    tcid = str(tcid)
    u    = db_get(tcid)
    s    = sess_all().get(tcid)

    if not u:
        return f"{_h('🔍', 'DETAIL USER')}\n\n❌ User tidak ditemukan.", kb_back("adm:users")

    on      = s and s.get("is_logged_in")
    banned  = bool(u.get("banned"))
    dot     = "🚫" if banned else ("🟢" if on else "🔴")
    status  = "BANNED" if banned else ("ONLINE" if on else "OFFLINE")
    wa_day  = sum(s["wa_harian"].values()) if s else 0

    text = (
        f"{_h('🔍', 'DETAIL USER')}\n"
        f"{dot} <b>{esc(u.get('name', tcid))}</b> ({status})\n\n"
        f"🆔 ID       : <code>{tcid}</code>\n"
        f"📧 Email    : <code>{esc(u.get('email','─'))}</code>\n"
        f"📅 Join     : <code>{u.get('join_date','─')}</code>\n"
        f"🕒 Terakhir : <code>{u.get('last_active','─')}</code>\n"
        f"📱 WA Today : <code>{wa_day}</code>"
    )
    return text, kb_user_detail(tcid, banned)

HELP_TEXT = (
    f"⚡ <b>PANDUAN {BOT_NAME}</b>\n{_sep()}\n\n"
    "<b>Command List:</b>\n"
    " ├ /start    — Buka dashboard\n"
    " ├ /stop     — Matikan engine\n"
    " ├ /id       — Lihat Chat ID\n"
    " └ /addrange — Inject range manual\n\n"
    "<i>💡 Semua fitur utama dapat diakses melalui tombol interaktif pada dashboard.</i>"
)

def _clean_nomor(val):
    if val is None:
        return None
    s = str(val).strip()
    if "." in s:
        try:
            s = str(int(float(s)))
        except Exception:
            s = s.split(".")[0]
    if s and s not in ("None", "nan", "") and s.lstrip("+-").isdigit() and len(s) >= 6:
        return s.lstrip("+")
    return None

def _parse_xlsx(xl_path):
    import openpyxl
    wb   = openpyxl.load_workbook(xl_path, read_only=True, data_only=True)
    ws   = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    if not rows:
        return []
    header  = [str(c).strip().lower() if c is not None else "" for c in rows[0]]
    num_col = next(
        (i for i, h in enumerate(header)
         if any(x in h for x in ["number","nomor","phone","msisdn","num","tel","hp","mobile"])),
        None
    )
    if num_col is None:
        for row in rows[1:8]:
            for idx, val in enumerate(row):
                if _clean_nomor(val):
                    num_col = idx
                    break
            if num_col is not None:
                break
    if num_col is None:
        num_col = 0
    return [n for row in rows[1:]
            for n in [_clean_nomor(row[num_col] if len(row) > num_col else None)] if n]

def _scrape_table_numbers(driver):
    from selenium.webdriver.common.by import By
    all_nums = []
    while True:
        rows = driver.execute_script(
            "var o=[];"
            "document.querySelectorAll('table tbody tr').forEach(function(tr){"
            "  var td=tr.querySelectorAll('td');if(!td.length)return;"
            "  var r=[];for(var i=0;i<td.length;i++)r.push(td[i].innerText.trim());"
            "  o.push(r);"
            "});"
            "return o;"
        ) or []
        found = 0
        for cols in rows:
            if not cols or (len(cols) == 1 and ("no data" in cols[0].lower() or "processing" in cols[0].lower())):
                continue
            for v in cols:
                sv = v.strip().split(".")[0]
                if sv.lstrip("+-").isdigit() and len(sv) >= 6:
                    all_nums.append(sv.lstrip("+"))
                    found += 1
                    break
        if not found:
            break
        try:
            nxt = driver.find_element(
                By.CSS_SELECTOR,
                "a.paginate_button.next:not(.disabled),li.next:not(.disabled) a")
            if nxt.is_displayed():
                driver.execute_script("arguments[0].click();", nxt)
                time.sleep(1)
            else:
                break
        except Exception:
            break
    return all_nums

def do_export(cid: str, s: dict, mid: int):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    s["busy"].set()
    driver   = s["driver"]
    xl       = None
    txt_path = None
    try:
        edit_msg(cid, mid, f"{_h('📥', 'EXPORT NOMOR')}\n⏳ Membuka portal numbers...", kb_back())

        with s["driver_lock"]:
            try:
                driver.execute_cdp_cmd("Page.setDownloadBehavior",
                    {"behavior": "allow", "downloadPath": s["download_dir"]})
            except Exception:
                pass

            driver.get(URL_NUMBERS)
            time.sleep(2.5)

            for f in glob.glob(os.path.join(s["download_dir"], "*.xls*")):
                try:
                    os.remove(f)
                except Exception:
                    pass

            try:
                sel = WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "select[name*='DataTables_Table'],select[name*='length']")))
                driver.execute_script(
                    "var s=arguments[0],best=null;"
                    "for(var i=0;i<s.options.length;i++){"
                    "  var v=parseInt(s.options[i].value);"
                    "  if(!isNaN(v)&&v<0){best=s.options[i].value;break;}"
                    "  if(!isNaN(v)&&(best===null||v>parseInt(best)))best=s.options[i].value;"
                    "}"
                    "if(best){s.value=best;s.dispatchEvent(new Event('change',{bubbles:true}));}",
                    sel)
                time.sleep(1.5)
            except Exception:
                pass

            btn_export = None
            for by, selector in [
                (By.XPATH, "//a[contains(translate(normalize-space(text()),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export number excel')]"),
                (By.XPATH, "//button[contains(translate(normalize-space(text()),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export number excel')]"),
                (By.XPATH, "//a[contains(@href,'export')]"),
            ]:
                try:
                    for el in driver.find_elements(by, selector):
                        t = (el.text or el.get_attribute("innerText") or "").lower()
                        if "export" in t or "excel" in t:
                            btn_export = el
                            break
                    if btn_export:
                        break
                except Exception:
                    pass

            if btn_export:
                edit_msg(cid, mid, f"{_h('📥', 'EXPORT NOMOR')}\n⏳ Mengunduh file Excel...", kb_back())
                driver.execute_script("arguments[0].scrollIntoView(true);", btn_export)
                time.sleep(0.3)
                driver.execute_script("arguments[0].click();", btn_export)
                deadline = time.time() + 45
                while time.time() < deadline:
                    time.sleep(0.5)
                    fs = [f for f in glob.glob(os.path.join(s["download_dir"], "*.xls*"))
                          if not f.endswith((".crdownload", ".part", ".tmp"))]
                    if fs:
                        xl = max(fs, key=os.path.getmtime)
                        if os.path.getsize(xl) > 0:
                            break
                        xl = None

            numbers = []
            if xl:
                try:
                    numbers = _parse_xlsx(xl)
                except Exception as e:
                    log.warning(f"parse xlsx: {e}")
            if not numbers:
                edit_msg(cid, mid, f"{_h('📥', 'EXPORT NOMOR')}\n⏳ Scrape dari tabel web...", kb_back())
                numbers = _scrape_table_numbers(driver)

            try:
                driver.get(URL_LIVE)
                s["last_reload"] = time.time()
            except Exception:
                pass

        if not numbers:
            edit_msg(cid, mid, f"{_h('⚠️', 'DATA KOSONG')}\nTidak ada nomor aktif di portal.", kb_back())
            return

        unique   = list(dict.fromkeys(numbers))
        ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
        txt_path = os.path.join(s["download_dir"], f"WALZ_NUMS_{ts}.txt")
        with open(txt_path, "w") as f:
            f.write("\n".join(unique))

        edit_msg(cid, mid,
            f"{_h('📥', 'EXPORT NOMOR')}\n"
            f"✅ Ditemukan <code>{len(unique)}</code> nomor\n"
            "⏳ Mengirim file...", kb_back())

        cap = (
            f"📱 <b>NOMOR AKTIF</b>\n{_sep()}\n\n"
            f" ├ Total   : <code>{len(unique)} nomor</code>\n"
            f" ├ Metode  : <code>{'Excel' if xl else 'Table Scrape'}</code>\n"
            f" └ Tanggal : <code>{datetime.now().strftime('%d %b %Y %H:%M')}</code>"
        )
        if send_file(cid, txt_path, cap):
            edit_msg(cid, mid, f"{_h('✅', 'SELESAI')}\nFile nomor aktif sudah dikirim.", kb_back())
        else:
            edit_msg(cid, mid, f"{_h('❌', 'GAGAL')}\nGagal mengirim file.", kb_back())

    except Exception as ex:
        log.error(f"do_export [{cid}]: {ex}\n{traceback.format_exc()}")
        edit_msg(cid, mid, f"{_h('💥', 'ERROR')}\n<code>{esc(str(ex)[:300])}</code>", kb_back())
    finally:
        for p in [xl, txt_path]:
            if p:
                try:
                    os.remove(p)
                except Exception:
                    pass
        s["busy"].clear()

def do_delete_numbers(cid: str, s: dict, mid: int):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    s["busy"].set()
    driver = s["driver"]
    try:
        edit_msg(cid, mid, f"{_h('🗑', 'HAPUS NOMOR')}\n⏳ Membuka panel numbers...", kb_back())

        with s["driver_lock"]:
            driver.get(URL_NUMBERS)
            time.sleep(2.5)
            try:
                btn = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((
                    By.XPATH,
                    "//button[contains(normalize-space(text()),'Bulk return all numbers')]"
                    "|//a[contains(normalize-space(text()),'Bulk return all numbers')]"
                )))
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(1.5)
            except Exception as e:
                edit_msg(cid, mid, f"{_h('❌', 'GAGAL')}\n<code>{esc(str(e))}</code>", kb_back())
                return

            try:
                driver.switch_to.alert.accept()
                time.sleep(1.5)
            except Exception:
                pass

            for sel in ["button.confirm", "button.swal-button--confirm",
                        ".modal-footer button.btn-danger",
                        "//button[contains(text(),'Yes')]", "//button[contains(text(),'OK')]"]:
                try:
                    el = (driver.find_element(By.XPATH, sel)
                          if sel.startswith("//") else driver.find_element(By.CSS_SELECTOR, sel))
                    if el.is_displayed():
                        driver.execute_script("arguments[0].click();", el)
                        time.sleep(2)
                        break
                except Exception:
                    pass

            ok   = False
            dead = time.time() + 20
            while time.time() < dead:
                time.sleep(0.8)
                try:
                    pt = driver.execute_script("return document.body.innerText.toLowerCase();")
                    if any(x in pt for x in ["no data", "no entries", "showing 0", "success", "returned"]):
                        ok = True
                        break
                except Exception:
                    pass

            try:
                driver.get(URL_LIVE)
                s["last_reload"] = time.time()
            except Exception:
                pass

        edit_msg(cid, mid,
            f"{_h('✅' if ok else '⚠️', 'SELESAI')}\n"
            + ("Semua nomor berhasil dihapus." if ok else "Perintah dikirim. Cek portal untuk verifikasi."),
            kb_back())

    except Exception as ex:
        log.error(f"do_delete_numbers [{cid}]: {ex}")
        edit_msg(cid, mid, f"{_h('💥', 'ERROR')}\n<code>{esc(str(ex))}</code>", kb_back())
    finally:
        s["busy"].clear()

def handle_message(msg: dict):
    from core import sess_get, sess_new, start_engine, stop_engine, do_inject

    cid      = str(msg["chat"]["id"])
    msg_id   = msg["message_id"]
    text     = msg.get("text", "").strip()
    from_    = msg.get("from", {})
    fname    = from_.get("first_name", "")
    lname    = from_.get("last_name", "")
    uname    = from_.get("username", "")
    fullname = (fname + " " + lname).strip() or uname or cid

    if not text:
        return

    st = state_get(cid)
    if st:
        step = st.get("step")
        smid = st.get("mid")
        delete_msg(cid, msg_id)

        if step == "email":
            if not re.match(r"[^@]+@[^@]+\.[^@]+", text):
                if smid:
                    edit_msg(cid, smid,
                        f"{_h('⚙️', 'SETUP AKUN')}\n"
                        "❌ Format email tidak valid!\n\n"
                        "Langkah 1/2 — Kirim ulang <b>email Gmail</b> yang benar:",
                        kb_back())
                return
            st["email"] = text
            st["step"]  = "app_password"
            state_set(cid, st)
            if smid:
                edit_msg(cid, smid,
                    f"{_h('⚙️', 'SETUP AKUN')}\n"
                    f"✅ Email : <code>{esc(text)}</code>\n\n"
                    "Langkah 2/2 — Kirim <b>App Password</b> Google kamu:\n\n"
                    "<b>Cara mendapatkan App Password:</b>\n"
                    "  1. Buka myaccount.google.com\n"
                    "  2. Keamanan → Verifikasi 2 langkah\n"
                    "  3. App Password → buat baru\n"
                    "  4. Salin 16 karakter (tanpa spasi)\n\n"
                    "<i>⚠️ Pesan ini otomatis terhapus setelah dikirim</i>",
                    kb_back())
            return

        if step == "app_password":
            pw = text
            state_del(cid)
            _email_cap = st.get("email", "")
            _fn_cap    = fullname
            if smid:
                edit_msg(cid, smid,
                    f"{_h('🔍', 'VERIFIKASI AKUN')}\n"
                    f"📧 <code>{esc(_email_cap)}</code>\n\n"
                    "⏳ Memeriksa akun ke server iVAS...", None)
            def _verify(_cid=cid, _email=_email_cap, _pw=pw, _fn=_fn_cap, _smid=smid):
                from core import verify_credentials_fast
                ok, reason = verify_credentials_fast(_email, _pw)

                if ok is False:
                    state_set(_cid, {"step": "app_password", "email": _email, "mid": _smid})
                    if _smid:
                        edit_msg(_cid, _smid,
                            f"{_h('❌', 'LOGIN GAGAL')}\n"
                            f"⚠️ <b>{esc(reason)}</b>\n\n"
                            f"Email : <code>{esc(_email)}</code>\n\n"
                            "Kirim ulang <b>App Password</b> yang benar\n"
                            "atau tap Ganti Email:",
                            kb([
                                [("✏️ Ganti Email", "m:setup")],
                                [("◀️ Kembali", "m:home")],
                            ]))
                    return

                db_update(_cid, {
                    "email":        _email,
                    "app_password": _pw,
                    "chat_name":    _fn,
                    "name":        _fn,
                    "status":      "active",
                    "join_date":   datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "last_active": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "banned":      False,
                })

                if ok is None:
                    msg_ok = (
                        f"{_h('⚠️', 'SETUP TERSIMPAN')}\n"
                        f"📧 Email : <code>{esc(_email)}</code>\n\n"
                        f"<i>Verifikasi otomatis tidak tersedia.\n"
                        f"Akun disimpan, engine akan coba login.\n"
                        f"Jika salah, engine akan berhenti.</i>\n\n"
                        "⏳ Memulai engine..."
                    )
                else:
                    msg_ok = (
                        f"{_h('✅', 'AKUN TERVERIFIKASI')}\n"
                        f"✅ Email : <code>{esc(_email)}</code>\n"
                        f"✅ Login : <b>Valid</b>\n\n"
                        "⏳ Memulai engine..."
                    )

                if _smid:
                    edit_msg(_cid, _smid, msg_ok, None)
                stop_engine(_cid)
                start_engine(_cid)
            threading.Thread(target=_verify, daemon=True).start()
            return

        if step == "inject_range":
            rn = text.strip()
            state_del(cid)
            if smid:
                edit_msg(cid, smid,
                    f"{_h('💉', 'INJECT')}\n"
                    f"🎯 Range : <code>{esc(rn)}</code>\n\n"
                    "Pilih jumlah nomor:",
                    kb_inject_qty(rn))
                _set_dash_mid(cid, smid)
                state_set(cid, {"step": None, "range": rn, "mid": smid})
            return

        if step == "inject_custom":
            rn = st.get("range", "")
            state_del(cid)
            try:
                qty = max(10, min(int(text.strip()), 9999))
            except Exception:
                if smid:
                    edit_msg(cid, smid,
                        f"{_h('❌', 'INPUT SALAH')}\nMasukkan angka valid (10–9999).",
                        kb_back("m:inject"))
                return
            s = sess_get(cid)
            if not s or not s.get("is_logged_in"):
                if smid:
                    edit_msg(cid, smid, f"{_h('🔒', 'ENGINE OFFLINE')}\nStart engine dulu.", kb_back())
                return
            if smid:
                edit_msg(cid, smid,
                    f"{_h('⚡', 'INJECT')}\n"
                    f" ├ 🎯 Range  : <code>{esc(rn)}</code>\n"
                    f" └ 📦 Target : <code>{qty} nomor</code>\n\n"
                    "⏳ Menyiapkan...", None)
                _set_dash_mid(cid, smid)
            threading.Thread(target=do_inject, args=(cid, s, rn, qty, smid), daemon=True).start()
            return

        if step == "fwd_group":
            gid = text.strip()
            state_del(cid)
            s = sess_get(cid)
            if s:
                s["fwd_group_id"] = gid
                s["fwd_enabled"]  = True
            db_update(cid, {"fwd_group_id": gid})
            if smid:
                txt, markup = page_forward(cid)
                edit_msg(cid, smid, txt, markup)
                _set_dash_mid(cid, smid)
            return

        if step == "broadcast":
            state_del(cid)
            n = broadcast_all(f"📢 <b>BROADCAST</b>\n{_sep()}\n\n{text}")
            if smid:
                edit_msg(cid, smid,
                    f"{_h('📢', 'BROADCAST TERKIRIM')}\n✅ Dikirim ke <b>{n}</b> user.",
                    kb_back("adm:stats"))
            return

    parts = text.split()
    if not parts:
        return
    cmd  = parts[0].lower().split("@")[0]
    args = " ".join(parts[1:]).strip()

    delete_msg(cid, msg_id)

    user = db_get(cid)
    s    = sess_get(cid)

    if user and user.get("banned"):
        send_msg(cid, f"{_h('🚫', 'AKSES DITOLAK')}\nAkun kamu ditangguhkan.")
        return

    if cmd == "/start":
        ok_grp, missing = check_group_membership(cid)
        if not ok_grp:
            links = "\n".join(f"  {i+1}. <code>{g}</code>" for i, g in enumerate(missing))
            send_msg(cid, f"{_h('⚠️', 'PERLU JOIN GRUP')}\nJoin dulu:\n\n{links}\n\nLalu /start lagi.")
            return

        from config import IVASMS_EMAIL, IVASMS_APP_PASSWORD
        has_creds = bool(IVASMS_EMAIL.strip() and IVASMS_APP_PASSWORD.strip())

        if not user and not has_creds:
            if not s:
                s = sess_new(cid)
            mid = send_msg(cid,
                f"⚡ <b>{BOT_NAME}</b>\n{_sep()}\n\n"
                "👋 Selamat datang!\n\n"
                "Daftarkan akun <b>iVAS SMS</b> kamu untuk memulai.\n"
                "Kamu hanya perlu <b>email Gmail</b> dan <b>App Password</b>.",
                kb([[ ("⚙️ Setup Akun", "m:setup") ]]))
            if mid and s:
                s["last_dash_id"] = mid
        else:
            if not s or not s.get("thread") or not s["thread"].is_alive():
                start_engine(cid)
            else:
                txt, markup = page_home(cid)
                fresh_send(cid, txt, markup)

    elif cmd == "/stop":
        if stop_engine(cid):
            txt, markup = page_home(cid)
            fresh_send(cid, txt, markup)
        else:
            send_msg(cid, f"{_h('⚠️', 'INFO')}\nEngine tidak aktif.")

    elif cmd == "/id":
        send_msg(cid, f"{_h('🆔', 'CHAT ID')}\n<code>{cid}</code>")

    elif cmd in ("/bantuan", "/help"):
        mid = _get_dash_mid(cid)
        if mid:
            edit_msg(cid, mid, HELP_TEXT, kb_back())
        else:
            new_mid = send_msg(cid, HELP_TEXT, kb_back())
            _set_dash_mid(cid, new_mid)

    elif cmd == "/addrange":
        if not user:
            send_msg(cid, "Ketik /start untuk memulai.")
            return
        if not (s and s.get("is_logged_in")):
            send_msg(cid, f"{_h('🔒', 'ENGINE OFFLINE')}\nStart engine dulu via /start.")
            return
        if s and s["busy"].is_set():
            send_msg(cid, f"{_h('⏳', 'SIBUK')}\nEngine sedang proses. Coba lagi nanti.")
            return
        if not args:
            mid = send_msg(cid, f"{_h('💉', 'INJECT')}\nKetik nama range:", kb_back())
            state_set(cid, {"step": "inject_range", "mid": mid})
            _set_dash_mid(cid, mid)
            return
        arg_parts = args.rsplit(None, 1)
        if len(arg_parts) == 2 and arg_parts[1].isdigit():
            rn, qty = arg_parts[0], max(10, min(int(arg_parts[1]), 9999))
        else:
            rn, qty = args, 100
        mid = smart_send(cid,
            f"{_h('⚡', 'INJECT')}\n"
            f" ├ 🎯 Range  : <code>{esc(rn)}</code>\n"
            f" └ 📦 Target : <code>{qty} nomor</code>\n\n"
            "⏳ Menyiapkan...")
        if mid:
            threading.Thread(target=do_inject, args=(cid, s, rn, qty, mid), daemon=True).start()

    elif cmd == "/admin" and cid == OWNER_ID:
        txt, markup = page_admin_stats()
        fresh_send(cid, txt, markup)

    elif cmd == "/broadcast" and cid == OWNER_ID:
        if args:
            n = broadcast_all(f"📢 <b>BROADCAST</b>\n{_sep()}\n\n{args}")
            send_msg(cid, f"{_h('📢', 'BROADCAST')}\n✅ Terkirim ke <b>{n}</b> user.")
        else:
            mid = send_msg(cid, f"{_h('📢', 'BROADCAST')}\nKirim pesan broadcast:", kb_back("adm:stats"))
            state_set(cid, {"step": "broadcast", "mid": mid})
            _set_dash_mid(cid, mid)

def handle_callback(cb: dict):
    from core import sess_get, start_engine, stop_engine, do_inject, check_auto_range

    cb_id  = cb["id"]
    data   = cb.get("data", "")
    msg    = cb["message"]
    cid    = str(msg["chat"]["id"])
    mid    = msg["message_id"]

    user = db_get(cid)
    s    = sess_get(cid)

    if user and user.get("banned"):
        answer_cb(cb_id, "🚫 Akun ditangguhkan", alert=True)
        return

    _set_dash_mid(cid, mid)
    answer_cb(cb_id)

    engine_on = bool(s and s.get("is_logged_in"))
    ar_on     = bool(s and s.get("auto_range_enabled", True))
    fwd_on    = bool(s and s.get("fwd_enabled") and s.get("fwd_group_id"))

    if data in ("m:home", "m:refresh"):
        txt, markup = page_home(cid)
        edit_msg(cid, mid, txt, markup)

    elif data == "m:traffic":
        txt, markup = page_traffic(cid)
        edit_msg(cid, mid, txt, markup)

    elif data == "m:settings":
        txt, markup = page_settings(cid)
        edit_msg(cid, mid, txt, markup)

    elif data == "m:forward":
        txt, markup = page_forward(cid)
        edit_msg(cid, mid, txt, markup)

    elif data == "m:setup":
        state_del(cid)
        edit_msg(cid, mid,
            f"{_h('⚙️', 'SETUP AKUN iVAS')}\n"
            "Langkah 1/2 — Kirim <b>email Gmail</b> akun iVAS kamu:\n\n"
            "<i>Contoh: user@gmail.com</i>",
            kb_back())
        state_set(cid, {"step": "email", "mid": mid})

    elif data == "m:inject":
        if not engine_on:
            answer_cb(cb_id, "❌ Engine offline, start dulu!", alert=True)
            return
        edit_msg(cid, mid, f"{_h('💉', 'INJECT')}\nKetik nama range yang ingin di-inject:", kb_back())
        state_set(cid, {"step": "inject_range", "mid": mid})

    elif data == "m:numbers":
        if not engine_on:
            answer_cb(cb_id, "❌ Engine offline!", alert=True)
            return
        if s and s["busy"].is_set():
            answer_cb(cb_id, "⏳ Engine sedang sibuk", alert=True)
            return
        edit_msg(cid, mid,
            f"{_h('📱', 'NOMOR AKTIF')}\n"
            "Yakin ingin mengekspor semua nomor aktif?\n\n"
            "<i>Proses ini memerlukan beberapa saat.</i>",
            kb_confirm("export:confirm", "m:home"))

    elif data == "m:deletenum":
        if not engine_on:
            answer_cb(cb_id, "❌ Engine offline!", alert=True)
            return
        edit_msg(cid, mid,
            f"{_h('🗑', 'HAPUS SEMUA NOMOR')}\n"
            "⚠️ Yakin ingin menghapus <b>SEMUA</b> nomor aktif?\n\n"
            "<i>Tindakan ini tidak bisa dibatalkan!</i>",
            kb_confirm("delete:confirm", "m:settings"))

    elif data == "export:confirm":
        if not engine_on:
            answer_cb(cb_id, "❌ Engine offline!", alert=True)
            return
        if s and s["busy"].is_set():
            answer_cb(cb_id, "⏳ Engine sedang sibuk", alert=True)
            return
        edit_msg(cid, mid, f"{_h('📥', 'EXPORT NOMOR')}\n⏳ Memulai proses export...", kb_back())
        threading.Thread(target=do_export, args=(cid, s, mid), daemon=True).start()

    elif data == "delete:confirm":
        if not engine_on:
            answer_cb(cb_id, "❌ Engine offline!", alert=True)
            return
        if s and s["busy"].is_set():
            answer_cb(cb_id, "⏳ Engine sedang sibuk", alert=True)
            return
        edit_msg(cid, mid, f"{_h('🗑', 'MENGHAPUS NOMOR')}\n⏳ Mengirim instruksi ke portal...", None)
        threading.Thread(target=do_delete_numbers, args=(cid, s, mid), daemon=True).start()

    elif data == "engine:start":
        if engine_on:
            answer_cb(cb_id, "Engine sudah aktif!")
            return
        edit_msg(cid, mid, f"{_h('⚙️', 'MEMULAI ENGINE')}\n⏳ Menginisialisasi browser...")
        start_engine(cid)

    elif data == "engine:stop":
        if not engine_on:
            answer_cb(cb_id, "Engine sudah mati!")
            return
        stop_engine(cid)
        txt, markup = page_home(cid)
        edit_msg(cid, mid, txt, markup)

    elif data == "toggle:ar":
        if not s:
            answer_cb(cb_id, "❌ Tidak ada sesi aktif", alert=True)
            return
        new_ar = not ar_on
        s["auto_range_enabled"] = new_ar
        if new_ar:
            threading.Thread(target=check_auto_range, args=(cid, s, s.get("driver")), daemon=True).start()
        msg_txt = msg.get("text") or ""
        if "PENGATURAN" in msg_txt:
            txt, markup = page_settings(cid)
        else:
            txt, markup = page_home(cid)
        edit_msg(cid, mid, txt, markup)

    elif data == "toggle:fwd":
        if not s:
            answer_cb(cb_id, "❌ Tidak ada sesi aktif", alert=True)
            return
        new_fwd      = not fwd_on
        s["fwd_enabled"] = new_fwd
        if not new_fwd:
            s["fwd_group_id"] = None
            db_set(cid, "fwd_group_id", None)
        txt, markup = page_forward(cid)
        edit_msg(cid, mid, txt, markup)

    elif data == "fwd:setgroup":
        edit_msg(cid, mid,
            f"{_h('📤', 'SET TARGET GRUP')}\n"
            "Kirim <b>Group ID</b> tujuan forward OTP.\n\n"
            "Cara dapat Group ID:\n"
            "  1. Tambah @userinfobot ke grup\n"
            "  2. Ketik /id di dalam grup\n\n"
            "<i>Format: <code>-1001234567890</code></i>",
            kb_back("m:forward"))
        state_set(cid, {"step": "fwd_group", "mid": mid})

    elif data == "action:reset":
        if s:
            with s["data_lock"]:
                s["wa_harian"].clear()
                s["seen_ids"].clear()
                s["traffic_counter"].clear()
        answer_cb(cb_id, "✅ Counter harian direset!")
        txt, markup = page_home(cid)
        edit_msg(cid, mid, txt, markup)

    elif data.startswith("inj:"):
        parts = data.split(":", 2)
        if len(parts) < 3:
            return
        rn, qs = parts[1], parts[2]
        try:
            qty = int(qs)
        except Exception:
            return
        if not engine_on:
            answer_cb(cb_id, "❌ Engine offline!", alert=True)
            return
        if s and s["busy"].is_set():
            answer_cb(cb_id, "⏳ Engine sedang sibuk", alert=True)
            return
        edit_msg(cid, mid,
            f"{_h('⚡', 'INJECT')}\n"
            f" ├ 🎯 Range  : <code>{esc(rn)}</code>\n"
            f" └ 📦 Target : <code>{qty} nomor</code>\n\n"
            "⏳ Menyiapkan...")
        threading.Thread(target=do_inject, args=(cid, s, rn, qty, mid), daemon=True).start()

    elif data.startswith("inj_top:"):
        rn = data.split(":", 1)[1]
        edit_msg(cid, mid,
            f"{_h('💉', 'INJECT')}\n"
            f"🎯 Range : <code>{esc(rn)}</code>\n\nPilih jumlah nomor:",
            kb_inject_qty(rn))
        state_set(cid, {"step": None, "range": rn, "mid": mid})

    elif data.startswith("inj_custom:"):
        rn = data.split(":", 1)[1]
        edit_msg(cid, mid,
            f"{_h('💉', 'CUSTOM JUMLAH')}\n"
            f"🎯 Range : <code>{esc(rn)}</code>\n\nKetik jumlah nomor (10–9999):",
            kb_back("m:inject"))
        state_set(cid, {"step": "inject_custom", "range": rn, "mid": mid})

    elif data == "adm:stats" and cid == OWNER_ID:
        txt, markup = page_admin_stats()
        edit_msg(cid, mid, txt, markup)

    elif data == "adm:users" and cid == OWNER_ID:
        txt, markup = page_user_list()
        edit_msg(cid, mid, txt, markup)

    elif data.startswith("adm:detail:") and cid == OWNER_ID:
        txt, markup = page_user_detail(data.split(":", 2)[2])
        edit_msg(cid, mid, txt, markup)

    elif data.startswith("adm:ban:") and cid == OWNER_ID:
        tcid = data.split(":", 2)[2]
        db_set(tcid, "banned", True)
        stop_engine(tcid)
        send_msg(tcid, f"{_h('🚫', 'AKUN SUSPENDED')}\nAkun kamu ditangguhkan oleh Admin.")
        txt, markup = page_user_detail(tcid)
        edit_msg(cid, mid, txt, markup)

    elif data.startswith("adm:unban:") and cid == OWNER_ID:
        tcid = data.split(":", 2)[2]
        db_set(tcid, "banned", False)
        send_msg(tcid, f"{_h('✅', 'AKUN DIPULIHKAN')}\nKetik /start untuk melanjutkan.")
        txt, markup = page_user_detail(tcid)
        edit_msg(cid, mid, txt, markup)

    elif data.startswith("adm:kick:") and cid == OWNER_ID:
        tcid = data.split(":", 2)[2]
        ok   = stop_engine(tcid)
        answer_cb(cb_id, "✅ Engine dihentikan" if ok else "❌ Tidak ada sesi aktif")
        txt, markup = page_user_detail(tcid)
        edit_msg(cid, mid, txt, markup)

    elif data == "adm:broadcast" and cid == OWNER_ID:
        edit_msg(cid, mid,
            f"{_h('📢', 'BROADCAST')}\nKirim pesan untuk di-blast ke semua user:",
            kb_back("adm:stats"))
        state_set(cid, {"step": "broadcast", "mid": mid})
