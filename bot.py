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

def _fmt_num(n: int) -> str:
    return f"{n:,}".replace(",", ".")

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
    p = {"chat_id": str(cid), "text": text, "parse_mode": "HTML",
         "disable_web_page_preview": True}
    if markup:
        p["reply_markup"] = markup
    r = tg_post("sendMessage", p)
    return r["result"]["message_id"] if r and r.get("ok") else None

def edit_msg(cid, mid: int, text: str, markup=None) -> bool:
    p = {"chat_id": str(cid), "message_id": mid, "text": text,
         "parse_mode": "HTML", "disable_web_page_preview": True}
    if markup is not None:
        p["reply_markup"] = markup
    r = tg_post("editMessageText", p)
    return bool(r and (r.get("ok") or "not modified" in str(r).lower()))

def delete_msg(cid, mid: int):
    threading.Thread(
        target=tg_post,
        args=("deleteMessage", {"chat_id": str(cid), "message_id": mid}),
        daemon=True).start()

def answer_cb(cb_id: str, text: str = "", alert: bool = False):
    threading.Thread(
        target=tg_post,
        args=("answerCallbackQuery",
              {"callback_query_id": cb_id, "text": text, "show_alert": alert}),
        daemon=True).start()

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
            if not r or not r.get("ok") or \
               r["result"].get("status", "") in ("left", "kicked", "restricted"):
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


def _bar(pct: int, w: int = 8) -> str:
    filled = round(pct / 100 * w)
    return "█" * filled + "░" * (w - filled)

def _div(label: str = "", width: int = 28) -> str:
    if not label:
        return "─" * width
    pad = width - len(label) - 2
    left = pad // 2
    right = pad - left
    return "─" * left + f" {label} " + "─" * right


def kb(rows: list) -> dict:
    return {"inline_keyboard": [
        [{"text": lbl, "callback_data": d} for lbl, d in row]
        for row in rows
    ]}

def kb_main(engine_on: bool) -> dict:
    if engine_on:
        eng_row = [("■ Stop", "engine:stop"), ("⟳ Refresh", "m:home")]
    else:
        eng_row = [("▶ Start", "engine:start"), ("⟳ Refresh", "m:home")]
    return kb([
        [("📡 Monitor",     "m:traffic"),  ("📤 Inject Range", "m:inject")],
        [("📋 Nomor Aktif", "m:numbers"),  ("⚙ Pengaturan",   "m:settings")],
        eng_row,
    ])

def kb_settings(ar_on: bool, fwd_on: bool) -> dict:
    ar_txt  = "🤖 Auto Inject  ✓" if ar_on  else "🤖 Auto Inject  ✗"
    fwd_txt = "📨 Terusan OTP  ✓" if fwd_on else "📨 Terusan OTP  ✗"
    return kb([
        [(ar_txt, "toggle:ar"),  (fwd_txt, "toggle:fwd")],
        [("🗑 Hapus Nomor", "m:deletenum"), ("↺ Reset Counter", "action:reset")],
        [("◀ Ganti Email", "m:saveivas"), ("◀ Kembali", "m:home")],
    ])

def kb_forward(fwd_on: bool) -> dict:
    toggle = "■ Matikan" if fwd_on else "▶ Aktifkan"
    return kb([
        [(toggle, "toggle:fwd")],
        [("✏ Ganti Target Grup", "fwd:setgroup")],
        [("◀ Kembali", "m:settings")],
    ])

def kb_inject_qty(rn: str) -> dict:
    return kb([
        [(" 100 ", f"inj:{rn}:100"),  (" 200 ", f"inj:{rn}:200")],
        [(" 300 ", f"inj:{rn}:300"),  (" 500 ", f"inj:{rn}:500")],
        [("✏ Jumlah lain...", f"inj_custom:{rn}")],
        [("◀ Kembali", "m:inject")],
    ])

def kb_confirm(yes: str, no: str = "m:home") -> dict:
    return kb([[(f"✓ Lanjutkan", yes), ("✗ Batal", no)]])

def kb_back(to: str = "m:home") -> dict:
    return kb([[(f"◀ Kembali", to)]])

def kb_admin() -> dict:
    return kb([
        [("👥 Pengguna", "adm:users"), ("📊 Statistik", "adm:stats")],
        [("📢 Broadcast", "adm:broadcast")],
        [("◀ Kembali", "m:home")],
    ])

def kb_user_detail(tcid: str, banned: bool) -> dict:
    toggle = "✓ Aktifkan" if banned else "✗ Suspend"
    return kb([
        [(toggle, f"adm:{'unban' if banned else 'ban'}:{tcid}"),
         ("⊗ Hentikan", f"adm:kick:{tcid}")],
        [("◀ Kembali", "adm:users")],
    ])


def page_home(cid: str) -> tuple[str, dict]:
    from core import sess_get
    cid  = str(cid)
    user = db_get(cid)
    s    = sess_get(cid)

    if not user or not user.get("email"):
        txt = (
            f"<b>{BOT_NAME}</b>\n"
            f"{_div()}\n\n"
            "  Belum ada email terdaftar.\n\n"
            f"{_div()}\n"
            "  Kirim email kamu dengan perintah:\n"
            f"  <code>/saveivas email@gmail.com</code>"
        )
        return txt, kb([
            [("⚙ Simpan Email", "m:saveivas")],
        ])

    engine_on = bool(s and s.get("is_logged_in"))
    ar_on     = bool(s and s.get("auto_range_enabled", True))
    fwd_on    = bool(s and s.get("fwd_enabled") and s.get("fwd_group_id"))

    u_email = user.get("email", "─")

    uptime = "─"
    if s and s.get("start_time"):
        delta  = datetime.now() - s["start_time"]
        h, rem = divmod(int(delta.total_seconds()), 3600)
        m_val  = rem // 60
        uptime = f"{h}j {m_val}m"

    wa_today = total_msg = 0
    top_range = "─"
    if s:
        with s["data_lock"]:
            wa_today  = sum(s["wa_harian"].values())
            total_msg = sum(s["traffic_counter"].values())
            if s["traffic_counter"]:
                top_range = s["traffic_counter"].most_common(1)[0][0]

    engine_status = "🟢 Online" if engine_on else "🔴 Offline"

    txt = (
        f"<b>{BOT_NAME}</b>\n"
        f"{_div()}\n\n"
        f"  📧  <code>{esc(u_email)}</code>\n\n"
        f"{_div('STATUS')}\n"
        f"  Engine       {engine_status}\n"
        f"  Uptime       {uptime}\n\n"
        f"{_div('STATISTIK')}\n"
        f"  WA OTP       {_fmt_num(wa_today)} pesan\n"
        f"  Total SMS    {_fmt_num(total_msg)} pesan\n"
        f"  Top Range    {esc(top_range)}\n\n"
        f"{_div()}\n"
        "  Auto Inject  " + ("[✓]" if ar_on else "[✗]") + "\n"
        "  Terusan OTP  " + ("[✓]" if fwd_on else "[✗]")
    )
    return txt, kb_main(engine_on)

def page_traffic(cid: str) -> tuple[str, dict]:
    from core import sess_get
    s = sess_get(cid)
    if not s:
        return (
            f"<b>MONITOR LIVE</b>\n{_div()}\n\n"
            "  Engine belum aktif."
        ), kb_back()

    with s["data_lock"]:
        counter = dict(s["traffic_counter"])
        harian  = dict(s["wa_harian"])

    if not counter:
        return (
            f"<b>MONITOR LIVE</b>\n{_div()}\n\n"
            "  Belum ada SMS yang tertangkap.\n"
            "  <i>Menunggu pesan masuk...</i>"
        ), kb_back()

    top      = sorted(counter.items(), key=lambda x: x[1], reverse=True)[:TOP_N]
    total    = sum(counter.values())
    wa_total = sum(harian.values())

    lines = [
        f"<b>MONITOR LIVE</b>",
        f"{_div()}",
        f"",
        f"  SMS Masuk   <b>{_fmt_num(total)}</b>",
        f"  WA OTP      <b>{_fmt_num(wa_total)}</b>",
        f"",
        f"{_div(f'TOP {len(top)} RANGE')}",
    ]

    for i, (rng, cnt) in enumerate(top, 1):
        pct = int(cnt / total * 100) if total else 0
        bar = _bar(pct)
        num = f"({_fmt_num(cnt)}×)"
        lines.append(f"  <code>{i:>2}.</code> <b>{esc(rng)}</b>")
        lines.append(f"       <code>{bar}</code> {pct}% {num}")
        if i < len(top):
            lines.append("")

    rows = []
    if top:
        rows.append([(f"📤 Inject: {top[0][0][:20]}", f"inj_top:{top[0][0]}")])
    rows.append([("⟳ Refresh", "m:traffic"), ("◀ Kembali", "m:home")])
    return "\n".join(lines), kb(rows)

def page_settings(cid: str) -> tuple[str, dict]:
    from core import sess_get
    user   = db_get(cid)
    s      = sess_get(cid)
    ar_on  = bool(s and s.get("auto_range_enabled", True))
    fwd_on = bool(s and s.get("fwd_enabled") and s.get("fwd_group_id"))
    fwd_id = (s.get("fwd_group_id") or "─") if s else "─"
    u_email = (user.get("email", "─") if user else "─")

    txt = (
        f"<b>PENGATURAN</b>\n"
        f"{_div()}\n\n"
        f"{_div('AKUN')}\n"
        f"  Email       <code>{esc(u_email)}</code>\n\n"
        f"{_div('MODUL')}\n"
        f"  Auto Inject   [{'✓' if ar_on  else '✗'}]\n"
        f"  Terusan OTP   [{'✓' if fwd_on else '✗'}]\n"
        f"  Target Grup   <code>{esc(fwd_id)}</code>"
    )
    return txt, kb_settings(ar_on, fwd_on)

def page_forward(cid: str) -> tuple[str, dict]:
    from core import sess_get
    s      = sess_get(cid)
    fwd_on = bool(s and s.get("fwd_enabled") and s.get("fwd_group_id"))
    fwd_id = (s.get("fwd_group_id") or "─") if s else "─"
    status = "▶ Aktif" if fwd_on else "■ Nonaktif"
    txt = (
        f"<b>TERUSAN OTP</b>\n"
        f"{_div()}\n\n"
        f"  Status        <b>{status}</b>\n"
        f"  Target Grup   <code>{esc(fwd_id)}</code>\n\n"
        f"{_div()}\n"
        "  <i>Semua OTP WhatsApp yang masuk\n"
        "  diteruskan otomatis ke grup target.</i>"
    )
    return txt, kb_forward(fwd_on)

def page_admin_stats() -> tuple[str, dict]:
    from core import sess_all
    users  = db_all()
    active = sum(1 for u in users.values() if u.get("status") == "active")
    banned = sum(1 for u in users.values() if u.get("banned"))
    online = len(sess_all())
    txt = (
        f"<b>ADMIN PANEL</b>\n"
        f"{_div()}\n\n"
        f"  Total User   {len(users)}\n"
        f"  Aktif        {active}\n"
        f"  Online       {online}\n"
        f"  Suspended    {banned}\n\n"
        f"  <i>{datetime.now().strftime('%d %b %Y  %H:%M')}</i>"
    )
    return txt, kb_admin()

def page_user_list() -> tuple[str, dict]:
    users = db_all()
    if not users:
        return f"<b>PENGGUNA</b>\n{_div()}\n\n  Belum ada pengguna.", kb_back("adm:stats")
    lines = [f"<b>PENGGUNA</b>", f"{_div()}", ""]
    rows  = []
    for cid, u in list(users.items())[:20]:
        em     = u.get("email", "─")[:25]
        status = "🔴" if u.get("banned") else ("🟢" if u.get("status") == "active" else "⚪")
        lines.append(f"  {status} <code>{esc(em)}</code>")
        rows.append([(f"Detail: {em[:18]}", f"adm:detail:{cid}")])
    rows.append([("◀ Kembali", "adm:stats")])
    return "\n".join(lines), kb(rows)

def page_user_detail(tcid: str) -> tuple[str, dict]:
    u = db_get(tcid)
    if not u:
        return f"<b>USER NOT FOUND</b>", kb_back("adm:users")
    banned  = bool(u.get("banned"))
    status  = "🔴 Suspended" if banned else ("🟢 Active" if u.get("status") == "active" else "⚪ Inactive")
    txt = (
        f"<b>DETAIL USER</b>\n"
        f"{_div()}\n\n"
        f"  CID          <code>{tcid}</code>\n"
        f"  Email        <code>{esc(u.get('email','─'))}</code>\n"
        f"  Status       {status}\n"
        f"  Bergabung    {u.get('join_date','─')}\n"
        f"  Terakhir     {u.get('last_active','─')}"
    )
    return txt, kb_user_detail(tcid, banned)

HELP_TEXT = (
    f"<b>BANTUAN — {BOT_NAME}</b>\n"
    f"{'─'*28}\n\n"
    f"{_div('PERINTAH')}\n"
    "  /start       Mulai / dashboard\n"
    "  /stop        Hentikan engine\n"
    "  /saveivas    Simpan email iVAS\n"
    "  /addrange    Inject nomor ke range\n"
    "  /id          Lihat Chat ID\n\n"
    f"{_div('CARA PAKAI')}\n"
    "  1. /saveivas email@gmail.com\n"
    "  2. /start → ▶ Start engine\n"
    "  3. /addrange NAMA_RANGE 100\n\n"
    f"{_div('CONTOH')}\n"
    "  <code>/saveivas user@gmail.com</code>\n"
    "  <code>/addrange EGYPT 7822 200</code>"
)


def _clean_nomor(val):
    s = str(val).strip().split(".")[0]
    return s.lstrip("+").replace(" ", "") if s.lstrip("+-").isdigit() else None

def _parse_xlsx(xl_path):
    import openpyxl
    nums = []
    wb = openpyxl.load_workbook(xl_path, read_only=True, data_only=True)
    for ws in wb.worksheets:
        for row in ws.iter_rows(values_only=True):
            for cell in row:
                if cell is None:
                    continue
                n = _clean_nomor(cell)
                if n and len(n) >= 6:
                    nums.append(n)
    return nums

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
            if not cols or (len(cols) == 1 and
               ("no data" in cols[0].lower() or "processing" in cols[0].lower())):
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

    def _upd(msg):
        edit_msg(cid, mid,
            f"<b>EXPORT NOMOR</b>\n{_div()}\n\n  {msg}",
            kb_back())

    try:
        _upd("⟳ Membuka halaman nomor aktif...")

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
                    (By.CSS_SELECTOR,
                     "select[name*='DataTables_Table'],select[name*='length']")))
                driver.execute_script(
                    "var s=arguments[0],best=null;"
                    "for(var i=0;i<s.options.length;i++){"
                    "  var v=parseInt(s.options[i].value);"
                    "  if(!isNaN(v)&&v<0){best=s.options[i].value;break;}"
                    "  if(!isNaN(v)&&(best===null||v>parseInt(best)))best=s.options[i].value;"
                    "}"
                    "if(best){s.value=best;"
                    "s.dispatchEvent(new Event('change',{bubbles:true}));}",
                    sel)
                time.sleep(1.5)
            except Exception:
                pass

            btn_export = None
            for by, selector in [
                (By.XPATH, "//a[contains(translate(normalize-space(text()),"
                           "'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),"
                           "'export number excel')]"),
                (By.XPATH, "//button[contains(translate(normalize-space(text()),"
                           "'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),"
                           "'export number excel')]"),
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
                _upd("⟳ Mengunduh file Excel...")
                driver.execute_script("arguments[0].scrollIntoView(true);", btn_export)
                time.sleep(0.3)
                driver.execute_script("arguments[0].click();", btn_export)
                deadline = time.time() + 45
                while time.time() < deadline:
                    time.sleep(0.5)
                    fs = [f for f in glob.glob(
                              os.path.join(s["download_dir"], "*.xls*"))
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
                _upd("⟳ Membaca dari tabel halaman...")
                numbers = _scrape_table_numbers(driver)

            try:
                driver.get(URL_LIVE)
                s["last_reload"] = time.time()
            except Exception:
                pass

        if not numbers:
            edit_msg(cid, mid,
                f"<b>EXPORT NOMOR</b>\n{_div()}\n\n"
                "  Tidak ada nomor aktif di portal.",
                kb_back())
            return

        unique   = list(dict.fromkeys(numbers))
        ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
        txt_path = os.path.join(s["download_dir"], f"IVAS_NUMS_{ts}.txt")
        with open(txt_path, "w") as f:
            f.write("\n".join(unique))

        _upd(f"✓ {_fmt_num(len(unique))} nomor ditemukan. Mengirim file...")

        cap = (
            f"<b>NOMOR AKTIF</b>\n{_div()}\n\n"
            f"  Total     {_fmt_num(len(unique))} nomor\n"
            f"  Metode    {'Excel' if xl else 'Table Scrape'}\n"
            f"  Tanggal   {datetime.now().strftime('%d %b %Y  %H:%M')}"
        )
        if send_file(cid, txt_path, cap):
            edit_msg(cid, mid,
                f"<b>EXPORT NOMOR</b>\n{_div()}\n\n"
                "  ✓ File berhasil dikirim.",
                kb_back())
        else:
            edit_msg(cid, mid,
                f"<b>EXPORT NOMOR</b>\n{_div()}\n\n"
                "  ✗ Gagal mengirim file.",
                kb_back())

    except Exception as ex:
        log.error(f"do_export [{cid}]: {ex}\n{traceback.format_exc()}")
        edit_msg(cid, mid,
            f"<b>EXPORT NOMOR</b>\n{_div()}\n\n"
            f"  Error:\n  <code>{esc(str(ex)[:250])}</code>",
            kb_back())
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

    def _upd(msg):
        edit_msg(cid, mid,
            f"<b>HAPUS NOMOR</b>\n{_div()}\n\n  {msg}",
            kb_back())

    try:
        _upd("⟳ Membuka panel nomor...")

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
                edit_msg(cid, mid,
                    f"<b>HAPUS NOMOR</b>\n{_div()}\n\n"
                    f"  ✗ Gagal:\n  <code>{esc(str(e))}</code>",
                    kb_back())
                return

            try:
                driver.switch_to.alert.accept()
                time.sleep(1.5)
            except Exception:
                pass

            for sel in ["button.confirm", "button.swal-button--confirm",
                        ".modal-footer button.btn-danger",
                        "//button[contains(text(),'Yes')]",
                        "//button[contains(text(),'OK')]"]:
                try:
                    el = (driver.find_element(By.XPATH, sel)
                          if sel.startswith("//")
                          else driver.find_element(By.CSS_SELECTOR, sel))
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
                    pt = driver.execute_script(
                        "return document.body.innerText.toLowerCase();")
                    if any(x in pt for x in
                           ["no data", "no entries", "showing 0", "success", "returned"]):
                        ok = True
                        break
                except Exception:
                    pass

            try:
                driver.get(URL_LIVE)
                s["last_reload"] = time.time()
            except Exception:
                pass

        msg_result = "✓ Semua nomor berhasil dihapus." if ok \
            else "⚠ Perintah dikirim. Cek portal untuk verifikasi."
        edit_msg(cid, mid,
            f"<b>HAPUS NOMOR</b>\n{_div()}\n\n  {msg_result}",
            kb_back())

    except Exception as ex:
        log.error(f"do_delete_numbers [{cid}]: {ex}")
        edit_msg(cid, mid,
            f"<b>HAPUS NOMOR</b>\n{_div()}\n\n"
            f"  Error:\n  <code>{esc(str(ex)[:250])}</code>",
            kb_back())
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

        if step == "inject_range":
            rn = text.strip()
            state_del(cid)
            if smid:
                edit_msg(cid, smid,
                    f"<b>INJECT NOMOR</b>\n{_div()}\n\n"
                    f"  Range   <code>{esc(rn)}</code>\n\n"
                    "  Pilih jumlah nomor:",
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
                        f"<b>INPUT TIDAK VALID</b>\n{_div()}\n\n"
                        "  Masukkan angka (10–9999).",
                        kb_back("m:inject"))
                return
            s = sess_get(cid)
            if not s or not s.get("is_logged_in"):
                if smid:
                    edit_msg(cid, smid,
                        f"<b>ENGINE OFFLINE</b>\n{_div()}\n\n"
                        "  Start engine terlebih dahulu.",
                        kb_back())
                return
            if smid:
                edit_msg(cid, smid,
                    f"<b>INJECT NOMOR</b>\n{_div()}\n\n"
                    f"  Range   <code>{esc(rn)}</code>\n"
                    f"  Target  {_fmt_num(qty)} nomor\n\n"
                    "  ⟳ Menyiapkan...", None)
                _set_dash_mid(cid, smid)
            threading.Thread(
                target=do_inject, args=(cid, s, rn, qty, smid), daemon=True).start()
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
                txt_pg, markup = page_forward(cid)
                edit_msg(cid, smid, txt_pg, markup)
                _set_dash_mid(cid, smid)
            return

        if step == "broadcast":
            state_del(cid)
            n = broadcast_all(f"<b>BROADCAST</b>\n{_div()}\n\n{text}")
            if smid:
                edit_msg(cid, smid,
                    f"<b>BROADCAST TERKIRIM</b>\n{_div()}\n\n"
                    f"  ✓ Dikirim ke <b>{n}</b> pengguna.",
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
        send_msg(cid,
            f"<b>AKSES DITOLAK</b>\n{_div()}\n\n"
            "  Akun kamu telah disuspend.")
        return

    if cmd == "/start":
        ok_grp, missing = check_group_membership(cid)
        if not ok_grp:
            links = "\n".join(f"  {i+1}. <code>{g}</code>" for i, g in enumerate(missing))
            send_msg(cid,
                f"<b>JOIN GRUP DIPERLUKAN</b>\n{_div()}\n\n"
                f"  Silakan join dulu:\n{links}\n\n"
                "  Lalu ketik /start lagi.")
            return

        if not user or not user.get("email"):
            mid = send_msg(cid,
                f"<b>{BOT_NAME}</b>\n{_div()}\n\n"
                f"  Halo, <b>{esc(fullname)}</b>!\n\n"
                f"{_div()}\n"
                "  Simpan email iVAS kamu dulu:\n\n"
                "  <code>/saveivas email@gmail.com</code>",
                kb([[("⚙ Simpan Email", "m:saveivas")]]))
            if mid and not s:
                s = sess_new(cid)
                s["last_dash_id"] = mid
            return

        if s and s.get("is_logged_in"):
            txt_pg, markup = page_home(cid)
            fresh_send(cid, txt_pg, markup)
        elif s and s.get("thread") and s["thread"].is_alive():
            pass
        else:
            u_email = user.get("email", "─")
            mid = fresh_send(cid,
                f"<b>{BOT_NAME}</b>\n{_div()}\n\n"
                f"  <code>{esc(u_email)}</code>\n\n"
                f"{_div('MENGHUBUNGKAN')}\n"
                "  ⟳ Browser        memulai...\n"
                "  ◌ Login iVAS     menunggu\n"
                "  ◌ Hub Socket     menunggu")
            start_engine(cid, initial_msg_id=mid)

    elif cmd == "/stop":
        if stop_engine(cid):
            txt_pg, markup = page_home(cid)
            fresh_send(cid, txt_pg, markup)
        else:
            send_msg(cid,
                f"<b>INFO</b>\n{_div()}\n\n"
                "  Engine tidak sedang aktif.")

    elif cmd == "/id":
        send_msg(cid,
            f"<b>CHAT ID</b>\n{_div()}\n\n"
            f"  <code>{cid}</code>")

    elif cmd in ("/bantuan", "/help"):
        mid = _get_dash_mid(cid)
        if mid:
            edit_msg(cid, mid, HELP_TEXT, kb_back())
        else:
            new_mid = send_msg(cid, HELP_TEXT, kb_back())
            _set_dash_mid(cid, new_mid)

    elif cmd == "/saveivas":
        if not args:
            send_msg(cid,
                f"<b>❌ FORMAT SALAH!</b>\n\n"
                f"Gunakan: /saveivas EMAIL\n\n"
                f"Contoh: <code>/saveivas email@gmail.com</code>",
                kb([[(f"◀ Kembali", "m:home")]]) if user else None)
            return

        email_input = args.strip().lower()
        if not re.match(r"[^@]+@[^@]+\.[^@]+", email_input):
            send_msg(cid,
                f"<b>❌ FORMAT SALAH!</b>\n\n"
                f"Email tidak valid.\n\n"
                f"Contoh: <code>/saveivas email@gmail.com</code>",
                kb([[(f"◀ Kembali", "m:home")]]) if user else None)
            return

        parts_at = email_input.split("@")
        local    = parts_at[0]
        domain   = parts_at[1] if len(parts_at) > 1 else ""
        if len(local) <= 2:
            masked_local = local[0] + "***"
        else:
            masked_local = local[0] + "***" + local[-1]
        masked_email = f"{masked_local}@{domain}"

        db_update(cid, {
            "email":       email_input,
            "chat_name":   fullname,
            "name":        fullname,
            "status":      "active",
            "join_date":   datetime.now().strftime("%Y-%m-%d %H:%M"),
            "last_active": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "banned":      False,
        })

        if s:
            s_hub = s.get("hub", {})
            if s_hub:
                s_hub["email"] = email_input

        send_msg(cid,
            f"<b>✅ EMAIL TERSIMPAN!</b>\n\n"
            f"📧 Email: <code>{masked_email}</code>\n\n"
            f"Sekarang bisa gunakan:\n"
            f"<code>/addrange NAMA_RANGE JUMLAH</code>",
            kb([[(f"◀ Kembali", "m:home")]]))

    elif cmd == "/addrange":
        saved_email = (user.get("email", "") if user else "").strip()
        if not saved_email:
            send_msg(cid,
                f"<b>❌ BELUM ADA EMAIL!</b>\n\n"
                f"Simpan email dulu:\n"
                f"<code>/saveivas email@gmail.com</code>")
            return
        if not (s and s.get("is_logged_in")):
            send_msg(cid,
                f"<b>ENGINE OFFLINE</b>\n{_div()}\n\n"
                f"  Email   <code>{esc(saved_email)}</code>\n\n"
                "  Start engine dulu via /start.")
            return
        if s and s["busy"].is_set():
            send_msg(cid,
                f"<b>SEDANG PROSES</b>\n{_div()}\n\n"
                "  Engine sedang sibuk. Coba lagi nanti.")
            return

        if not args:
            mid = send_msg(cid,
                f"<b>INJECT NOMOR</b>\n{_div()}\n\n"
                f"  Akun    <code>{esc(saved_email)}</code>\n\n"
                "  Ketik nama range:",
                kb_back())
            state_set(cid, {"step": "inject_range", "mid": mid})
            _set_dash_mid(cid, mid)
            return

        arg_parts = args.rsplit(None, 1)
        if len(arg_parts) == 2 and arg_parts[1].isdigit():
            rn, qty = arg_parts[0], max(10, min(int(arg_parts[1]), 9999))
        else:
            rn, qty = args, 100

        mid = smart_send(cid,
            f"<b>INJECT NOMOR</b>\n{_div()}\n\n"
            f"  Akun    <code>{esc(saved_email)}</code>\n"
            f"  Range   <code>{esc(rn)}</code>\n"
            f"  Target  {_fmt_num(qty)} nomor\n\n"
            "  ⟳ Menyiapkan...")
        if mid:
            threading.Thread(
                target=do_inject, args=(cid, s, rn, qty, mid), daemon=True).start()

    elif cmd == "/admin" and cid == OWNER_ID:
        txt_pg, markup = page_admin_stats()
        fresh_send(cid, txt_pg, markup)

    elif cmd == "/broadcast" and cid == OWNER_ID:
        if args:
            n = broadcast_all(f"<b>BROADCAST</b>\n{_div()}\n\n{args}")
            send_msg(cid,
                f"<b>BROADCAST</b>\n{_div()}\n\n"
                f"  ✓ Terkirim ke <b>{n}</b> pengguna.")
        else:
            mid = send_msg(cid,
                f"<b>BROADCAST</b>\n{_div()}\n\n"
                "  Kirim pesan yang akan di-blast:",
                kb_back("adm:stats"))
            state_set(cid, {"step": "broadcast", "mid": mid})
            _set_dash_mid(cid, mid)


def handle_callback(cb: dict):
    from core import sess_get, start_engine, stop_engine, do_inject, check_auto_range

    cb_id = cb["id"]
    data  = cb.get("data", "")
    msg   = cb["message"]
    cid   = str(msg["chat"]["id"])
    mid   = msg["message_id"]

    user = db_get(cid)
    s    = sess_get(cid)

    if user and user.get("banned"):
        answer_cb(cb_id, "Akun kamu disuspend", alert=True)
        return

    _set_dash_mid(cid, mid)
    answer_cb(cb_id)

    engine_on = bool(s and s.get("is_logged_in"))
    ar_on     = bool(s and s.get("auto_range_enabled", True))
    fwd_on    = bool(s and s.get("fwd_enabled") and s.get("fwd_group_id"))

    if data in ("m:home", "m:refresh"):
        txt_pg, markup = page_home(cid)
        edit_msg(cid, mid, txt_pg, markup)

    elif data == "m:traffic":
        txt_pg, markup = page_traffic(cid)
        edit_msg(cid, mid, txt_pg, markup)

    elif data == "m:settings":
        txt_pg, markup = page_settings(cid)
        edit_msg(cid, mid, txt_pg, markup)

    elif data == "m:forward":
        txt_pg, markup = page_forward(cid)
        edit_msg(cid, mid, txt_pg, markup)

    elif data == "m:saveivas":
        state_del(cid)
        edit_msg(cid, mid,
            f"<b>SIMPAN EMAIL iVAS</b>\n{_div()}\n\n"
            "  Kirim email Gmail akun iVAS kamu.\n\n"
            "  <i>Format: user@gmail.com</i>\n\n"
            f"  Atau ketik langsung:\n"
            f"  <code>/saveivas email@gmail.com</code>",
            kb_back())

    elif data == "m:inject":
        if not engine_on:
            answer_cb(cb_id, "Engine offline, start dulu!", alert=True)
            return
        saved_email = (user.get("email", "") if user else "").strip()
        edit_msg(cid, mid,
            f"<b>INJECT NOMOR</b>\n{_div()}\n\n"
            f"  Akun    <code>{esc(saved_email)}</code>\n\n"
            "  Ketik nama range yang ingin di-inject:",
            kb_back())
        state_set(cid, {"step": "inject_range", "mid": mid})

    elif data == "m:numbers":
        if not engine_on:
            answer_cb(cb_id, "Engine offline!", alert=True)
            return
        if s and s["busy"].is_set():
            answer_cb(cb_id, "Engine sedang sibuk", alert=True)
            return
        edit_msg(cid, mid,
            f"<b>EXPORT NOMOR</b>\n{_div()}\n\n"
            "  Ekspor semua nomor aktif ke file?\n\n"
            "  <i>Proses ini memerlukan beberapa saat.</i>",
            kb_confirm("export:confirm", "m:home"))

    elif data == "m:deletenum":
        if not engine_on:
            answer_cb(cb_id, "Engine offline!", alert=True)
            return
        edit_msg(cid, mid,
            f"<b>HAPUS NOMOR</b>\n{_div()}\n\n"
            "  ⚠ Hapus <b>SEMUA</b> nomor aktif?\n\n"
            "  <i>Tindakan ini tidak bisa dibatalkan!</i>",
            kb_confirm("delete:confirm", "m:settings"))

    elif data == "export:confirm":
        if not engine_on:
            answer_cb(cb_id, "Engine offline!", alert=True)
            return
        if s and s["busy"].is_set():
            answer_cb(cb_id, "Engine sedang sibuk", alert=True)
            return
        edit_msg(cid, mid,
            f"<b>EXPORT NOMOR</b>\n{_div()}\n\n"
            "  ⟳ Memulai proses export...",
            kb_back())
        threading.Thread(
            target=do_export, args=(cid, s, mid), daemon=True).start()

    elif data == "delete:confirm":
        if not engine_on:
            answer_cb(cb_id, "Engine offline!", alert=True)
            return
        if s and s["busy"].is_set():
            answer_cb(cb_id, "Engine sedang sibuk", alert=True)
            return
        edit_msg(cid, mid,
            f"<b>HAPUS NOMOR</b>\n{_div()}\n\n"
            "  ⟳ Mengirim instruksi ke portal...", None)
        threading.Thread(
            target=do_delete_numbers, args=(cid, s, mid), daemon=True).start()

    elif data == "engine:start":
        if engine_on:
            answer_cb(cb_id, "Engine sudah aktif!")
            return
        u_email = (user.get("email", "") if user else "").strip()
        if not u_email:
            answer_cb(cb_id, "Simpan email dulu dengan /saveivas", alert=True)
            return
        edit_msg(cid, mid,
            f"<b>{BOT_NAME}</b>\n{_div()}\n\n"
            f"  <code>{esc(u_email)}</code>\n\n"
            f"{_div('MENGHUBUNGKAN')}\n"
            "  ⟳ Browser        memulai...\n"
            "  ◌ Login iVAS     menunggu\n"
            "  ◌ Hub Socket     menunggu")
        start_engine(cid, initial_msg_id=mid)

    elif data == "engine:stop":
        if not engine_on:
            answer_cb(cb_id, "Engine sudah mati!")
            return
        stop_engine(cid)
        txt_pg, markup = page_home(cid)
        edit_msg(cid, mid, txt_pg, markup)

    elif data == "toggle:ar":
        if not s:
            answer_cb(cb_id, "Tidak ada sesi aktif", alert=True)
            return
        new_ar = not ar_on
        s["auto_range_enabled"] = new_ar
        if new_ar:
            threading.Thread(
                target=check_auto_range,
                args=(cid, s, s.get("driver")), daemon=True).start()
        txt_pg, markup = page_settings(cid)
        edit_msg(cid, mid, txt_pg, markup)

    elif data == "toggle:fwd":
        if not s:
            answer_cb(cb_id, "Tidak ada sesi aktif", alert=True)
            return
        new_fwd = not fwd_on
        s["fwd_enabled"] = new_fwd
        if not new_fwd:
            s["fwd_group_id"] = None
            db_set(cid, "fwd_group_id", None)
        txt_pg, markup = page_forward(cid)
        edit_msg(cid, mid, txt_pg, markup)

    elif data == "fwd:setgroup":
        edit_msg(cid, mid,
            f"<b>SET TARGET GRUP</b>\n{_div()}\n\n"
            "  Kirim <b>Group ID</b> tujuan terusan OTP.\n\n"
            f"{_div('CARA MENDAPATKAN')}\n"
            "  1. Tambahkan @userinfobot ke grup\n"
            "  2. Ketik /id di dalam grup\n"
            "  3. Salin angka yang muncul\n\n"
            "  <i>Format: <code>-1001234567890</code></i>",
            kb_back("m:forward"))
        state_set(cid, {"step": "fwd_group", "mid": mid})

    elif data == "action:reset":
        if s:
            with s["data_lock"]:
                s["wa_harian"].clear()
                s["seen_ids"].clear()
                s["traffic_counter"].clear()
        answer_cb(cb_id, "Counter harian direset!")
        txt_pg, markup = page_home(cid)
        edit_msg(cid, mid, txt_pg, markup)

    elif data.startswith("inj:"):
        parts_d = data.split(":", 2)
        if len(parts_d) < 3:
            return
        rn, qs = parts_d[1], parts_d[2]
        try:
            qty = int(qs)
        except Exception:
            return
        if not engine_on:
            answer_cb(cb_id, "Engine offline!", alert=True)
            return
        if s and s["busy"].is_set():
            answer_cb(cb_id, "Engine sedang sibuk", alert=True)
            return
        saved_email = (user.get("email", "") if user else "").strip()
        edit_msg(cid, mid,
            f"<b>INJECT NOMOR</b>\n{_div()}\n\n"
            f"  Akun    <code>{esc(saved_email)}</code>\n"
            f"  Range   <code>{esc(rn)}</code>\n"
            f"  Target  {_fmt_num(qty)} nomor\n\n"
            "  ⟳ Menyiapkan...")
        threading.Thread(
            target=do_inject, args=(cid, s, rn, qty, mid), daemon=True).start()

    elif data.startswith("inj_top:"):
        rn = data.split(":", 1)[1]
        edit_msg(cid, mid,
            f"<b>INJECT NOMOR</b>\n{_div()}\n\n"
            f"  Range   <code>{esc(rn)}</code>\n\n"
            "  Pilih jumlah nomor:",
            kb_inject_qty(rn))
        state_set(cid, {"step": None, "range": rn, "mid": mid})

    elif data.startswith("inj_custom:"):
        rn = data.split(":", 1)[1]
        edit_msg(cid, mid,
            f"<b>INJECT NOMOR</b>\n{_div()}\n\n"
            f"  Range   <code>{esc(rn)}</code>\n\n"
            "  Ketik jumlah nomor (10–9999):",
            kb_back("m:inject"))
        state_set(cid, {"step": "inject_custom", "range": rn, "mid": mid})

    elif data == "adm:stats" and cid == OWNER_ID:
        txt_pg, markup = page_admin_stats()
        edit_msg(cid, mid, txt_pg, markup)

    elif data == "adm:users" and cid == OWNER_ID:
        txt_pg, markup = page_user_list()
        edit_msg(cid, mid, txt_pg, markup)

    elif data.startswith("adm:detail:") and cid == OWNER_ID:
        txt_pg, markup = page_user_detail(data.split(":", 2)[2])
        edit_msg(cid, mid, txt_pg, markup)

    elif data.startswith("adm:ban:") and cid == OWNER_ID:
        tcid = data.split(":", 2)[2]
        db_set(tcid, "banned", True)
        stop_engine(tcid)
        send_msg(tcid,
            f"<b>AKUN DISUSPEND</b>\n{_div()}\n\n"
            "  Akun kamu telah ditangguhkan oleh Admin.")
        txt_pg, markup = page_user_detail(tcid)
        edit_msg(cid, mid, txt_pg, markup)

    elif data.startswith("adm:unban:") and cid == OWNER_ID:
        tcid = data.split(":", 2)[2]
        db_set(tcid, "banned", False)
        send_msg(tcid,
            f"<b>AKUN DIAKTIFKAN</b>\n{_div()}\n\n"
            "  Akun kamu sudah aktif kembali.\n"
            "  Ketik /start untuk melanjutkan.")
        txt_pg, markup = page_user_detail(tcid)
        edit_msg(cid, mid, txt_pg, markup)

    elif data.startswith("adm:kick:") and cid == OWNER_ID:
        tcid = data.split(":", 2)[2]
        ok_k = stop_engine(tcid)
        answer_cb(cb_id, "Engine dihentikan" if ok_k else "Tidak ada sesi aktif")
        txt_pg, markup = page_user_detail(tcid)
        edit_msg(cid, mid, txt_pg, markup)

    elif data == "adm:broadcast" and cid == OWNER_ID:
        edit_msg(cid, mid,
            f"<b>BROADCAST</b>\n{_div()}\n\n"
            "  Kirim pesan yang akan di-blast\n"
            "  ke semua pengguna aktif:",
            kb_back("adm:stats"))
        state_set(cid, {"step": "broadcast", "mid": mid})
