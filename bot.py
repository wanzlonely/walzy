import re, os, time, threading
from datetime import datetime, date, timedelta
import requests

from engine import (
    BOT_NAME, BOT_TOKEN, TG_API, DL_DIR, OWNER_ID,
    send, edit, delete_msg, answer, send_doc,
    get_account, save_account, is_online, get_drv,
    sess, set_s, start_engine, stop_engine,
    do_inject_hub, inject_top_range_with_id,
    do_export_excel, parse_xlsx,
    scrape_numbers_page, do_bulk_return,
    do_get_sms, do_get_sms_today, server_today,
    get_groups, add_group, remove_group,
    TOP_N, AUTO_RANGE_QTY, MAX_GROUPS, _get_us,
    kb, div, esc, fmt, flag, log,
)

_tg = requests.Session()
_tg.headers.update({"Content-Type": "application/json"})

_state    = {}
_state_lk = threading.Lock()

def tg(ep, data, timeout=10):
    for _ in range(3):
        try:
            r = _tg.post(f"{TG_API}/{ep}", json=data, timeout=timeout)
            if r.ok: return r.json()
        except: time.sleep(0.3)
    return None

def state_get(cid):
    with _state_lk: return _state.get(str(cid))

def state_set(cid, v):
    with _state_lk: _state[str(cid)] = v

def state_del(cid):
    with _state_lk: _state.pop(str(cid), None)

def kb_back(to="m:home"):
    return kb([[(f"◀ Kembali", to)]])

def page_home(cid=None):
    acc     = get_account(cid)
    online  = is_online(cid)
    em      = acc.get("email", "─")
    has_acc = bool(acc.get("email") and acc.get("password"))
    if not has_acc:
        txt = (
            f"<b>{BOT_NAME}</b>\n{div()}\n\n"
            f"  ⚠️ <b>Akun belum diatur!</b>\n\n"
            f"  Set akun iVAS terlebih dahulu:\n"
            f"  <code>/login email password</code>\n\n"
            f"  Contoh:\n"
            f"  <code>/login user@gmail.com Pass123</code>"
        )
        return txt, kb([
            [("⚙️ Set Akun Login", "m:login")],
            [("📖 Bantuan", "m:help")],
        ])
    us       = _get_us(cid)
    live_otp = us.get("live_otp_active", False)
    status   = "🟢 Online"   if online             else "🔴 Offline"
    otp_stat = "🟢 Aktif"    if (online and live_otp) else "🔴 Nonaktif"
    txt = (
        f"<b>{BOT_NAME}</b>\n{div()}\n\n"
        f"  📧 <code>{esc(em)}</code>\n\n"
        f"{div('STATUS')}\n"
        f"  ⚡ Engine     {status}\n"
        f"  📨 Live OTP  {otp_stat}\n\n"
        f"{div()}\n"
        f"  <i>Pilih menu di bawah ↓</i>"
    )
    eng = [("■ Stop Engine", "engine:stop")] if online else [("▶ Start Engine", "engine:start")]
    return txt, kb([
        [("📊 Live Monitor", "m:monitor"), ("📤 Add Range", "m:addrange")],
        [("📩 Get SMS", "m:getsms"),        ("📋 Export Nomor", "m:export")],
        [("🗑 Hapus Nomor", "m:hapus"),     ("📢 Grup Forward", "m:grup")],
        eng + [("⚙️ Login", "m:login")],
        [("📖 Bantuan", "m:help")],
    ])

def page_monitor(cid=None):
    from collections import Counter
    s = sess(cid)
    if not is_online(cid):
        return (
            f"<b>📊 LIST RANGE HIGH TRAFFIC</b>\n{div()}\n\n"
            "  Engine belum aktif.\n"
            "  Tekan ▶ Start Engine terlebih dahulu."
        ), kb([
            [("▶ Start Engine", "engine:start")],
            [("◀ Kembali", "m:home")],
        ])
    counter  = Counter(s.get("traffic_counter", {}))
    top      = counter.most_common(TOP_N)
    total_wa = sum(counter.values())
    lines = [
        f"<b>📊 LIST RANGE HIGH TRAFFIC</b>",
        f"{div()}",
        f"",
        f" 👑 <b>{BOT_NAME}</b>",
        f"📱 WA OTP      <b>{fmt(total_wa)}</b>",
        f"",
    ]
    if not top:
        lines += [f"{div()}", "  ⏳ Menunggu SMS WhatsApp masuk...", "  <i>Monitor aktif</i>"]
    else:
        lines.append(f"{div(f'TOP {len(top)} TRAFFIC')}")
        for i, (country, cnt) in enumerate(top, 1):
            medal = "🥇" if i == 1 else ("🥈" if i == 2 else ("🥉" if i == 3 else f"  {i}."))
            lines.append(f"  {medal} {flag(country)} <b>{esc(country)}</b> 👉 <b>{fmt(cnt)}</b> SMS")

    rows = []
    if top:
        best_ranges = s.get("best_ranges", {})
        for i2, (country, cnt) in enumerate(top[:4], 1):
            medal      = "🥇" if i2 == 1 else ("🥈" if i2 == 2 else ("🥉" if i2 == 3 else "🏅"))
            full_range = best_ranges.get(country, country)
            short      = full_range[:18]
            rows.append([(f"{medal} {short}", f"inj_direct:{full_range}")])
    rows.append([("⟳ Refresh", "m:monitor"), ("◀ Kembali", "m:home")])
    return "\n".join(lines), kb(rows)

def page_addrange():
    txt = (
        f"<b>📤 ADD RANGE</b>\n{div()}\n\n"
        f"  Tambahkan nomor ke range tertentu\n"
        f"  melalui Hub OrangeCarrier.\n\n"
        f"{div('CONTOH RANGE')}\n"
        f"  <code>NIGERIA 14603</code>\n"
        f"  <code>NEPAL 4930</code>\n"
        f"  <code>CAMBODIA 4326</code>"
    )
    return txt, kb([
        [("✏️ Ketik Range", "addrange:input")],
        [("◀ Kembali", "m:home")],
    ])

def page_confirm(title, desc, yes_cb, back_cb="m:home", yes_label="✅  Ya, Lanjutkan", back_label="❌  Batal"):
    txt = (
        f"<b>{title}</b>\n{div()}\n\n"
        f"{desc}\n\n"
        f"{div()}\n"
        f"  <i>Lanjutkan?</i>"
    )
    return txt, kb([
        [(yes_label, yes_cb)],
        [(back_label, back_cb)],
    ])

def page_export():
    txt = (
        f"<b>📋 EXPORT NOMOR</b>\n{div()}\n\n"
        f"  Export semua nomor aktif dari\n"
        f"  portal iVAS ke file .txt\n\n"
        f"{div('PROSES')}\n"
        f"  1️⃣ Klik Export Number Excel\n"
        f"  2️⃣ Parse file → kirim .txt ke sini\n"
        f"  3️⃣ Fallback: scrape tabel manual"
    )
    return txt, kb([
        [("📥 Export Sekarang", "confirm:export")],
        [("◀ Kembali", "m:home")],
    ])

def page_hapus():
    txt = (
        f"<b>🗑 HAPUS SEMUA NOMOR</b>\n{div()}\n\n"
        f"  Bot akan klik tombol:\n"
        f"  <b>「 Bulk return all numbers 」</b>\n\n"
        f"  ⚠️ Semua nomor aktif akan dikembalikan.\n"
        f"  <b>Tidak bisa dibatalkan!</b>"
    )
    return txt, kb([
        [("🗑 Hapus Sekarang", "confirm:hapus")],
        [("◀ Kembali", "m:home")],
    ])

def page_login(cid=None):
    acc    = get_account(cid)
    em     = acc.get("email", "─")
    pw     = acc.get("password", "")
    masked = (pw[0] + "●" * (len(pw) - 1)) if pw else "─"
    txt = (
        f"<b>⚙️ LOGIN AKUN iVAS</b>\n{div()}\n\n"
        f"{div('AKUN TERSIMPAN')}\n"
        f"  📧 Email  <code>{esc(em)}</code>\n"
        f"  🔑 Pass   <code>{masked}</code>\n\n"
        f"{div('GANTI AKUN')}\n"
        f"  <code>/login email password</code>\n\n"
        f"  Contoh:\n"
        f"  <code>/login user@gmail.com Pass123</code>"
    )
    return txt, kb([
        [("✏️ Ganti Akun", "login:input")],
        [("◀ Kembali", "m:home")],
    ])

def page_help():
    txt = (
        f"<b>📖 PANDUAN {BOT_NAME}</b>\n{div()}\n\n"
        f"{div('PERTAMA KALI')}\n"
        f"  1️⃣ Set akun: <code>/login email password</code>\n"
        f"  2️⃣ Tekan ▶ Start Engine\n"
        f"  3️⃣ Tunggu Engine Online 🟢\n\n"
        f"{div('MENU')}\n"
        f"  📊 <b>Live Monitor</b>\n"
        f"     TOP {TOP_N} negara WA OTP real-time\n\n"
        f"  📤 <b>Add Range</b>\n"
        f"     Inject nomor via Hub OrangeCarrier\n\n"
        f"  📩 <b>Get SMS</b>\n"
        f"     Laporan SMS hari ini — 1 klik!\n\n"
        f"  📋 <b>Export Nomor</b>\n"
        f"     Download semua nomor aktif (.txt)\n\n"
        f"  🗑 <b>Hapus Nomor</b>\n"
        f"     Bulk return all numbers\n\n"
        f"{div('PERINTAH')}\n"
        f"  /start    Dashboard utama\n"
        f"  /login    Set akun iVAS kamu sendiri\n"
        f"  /stop     Hentikan engine\n"
        f"  /id       Lihat Chat ID kamu\n"
        f"  /help     Panduan ini\n\n"
        f"{div('TIPS')}\n"
        f"  • Engine harus Online untuk semua fitur\n"
        f"  • Top 4 range tampil tombol inject langsung\n"
        f"  • Monitor pakai DataTables API — menangkap semua baris"
    )
    return txt, kb([[(f"◀ Kembali", "m:home")]])

def page_grup(cid=None):
    groups = get_groups()
    lines  = [
        "<b>📢 GRUP FORWARD SMS</b>",
        div(), "",
        "  SMS baru otomatis dikirim ke semua grup ini.", "",
        div(f"GRUP TERDAFTAR ({len(groups)}/{MAX_GROUPS})"),
    ]
    btn_rows = []
    if not groups:
        lines.append("  <i>Belum ada grup. Kirim /addgrup di grup target.</i>")
    else:
        for i, g in enumerate(groups, 1):
            gid   = g["id"]
            title = esc(g.get("title", gid))
            link  = g.get("invite_link", "")
            link_status = f"  🔗 <code>{link}</code>" if link else "  <i>— belum ada link</i>"
            lines.append(f"  {i}. <b>{title}</b>  <code>{gid}</code>")
            lines.append(link_status)
            btn_rows.append([(f"🗑 Hapus: {title[:16]}", f"delgrup:{gid}")])
    lines   += ["", "  <i>Kirim /addgrup dari dalam grup untuk mendaftar</i>"]
    btn_rows.append([("ℹ️ Cara Daftar", "grup:howto")])
    btn_rows.append([("◄ Kembali", "m:home")])
    return "\n".join(lines), kb(btn_rows)

def handle_message(msg):
    cid       = str(msg["chat"]["id"])
    mid       = msg["message_id"]
    text      = msg.get("text", "").strip()
    chat_type = msg.get("chat", {}).get("type", "private")
    from_id   = str(msg.get("from", {}).get("id", ""))
    if not text: return

    is_owner = (from_id == str(OWNER_ID))
    is_group = chat_type in ("group", "supergroup")
    cmd_bare = text.split()[0].lower().split("@")[0] if text.split() else ""

    if not is_owner:
        if is_group and cmd_bare == "/addgrup": pass
        else: return

    def _del_if_private():
        if chat_type == "private": delete_msg(cid, mid)

    st = state_get(cid) if chat_type == "private" else None
    if st:
        step = st.get("step")
        smid = st.get("mid")
        _del_if_private()

        if step == "addrange_name":
            rng = text.strip()
            state_del(cid)
            if smid:
                edit(cid, smid,
                    f"<b>📤 ADD RANGE</b>\n{div()}\n\n"
                    f"  Range   <code>{esc(rng)}</code>\n\n"
                    f"{div('PILIH JUMLAH')}",
                    kb([
                        [(" 100 ", f"inj:{rng}:100"), (" 200 ", f"inj:{rng}:200")],
                        [(" 300 ", f"inj:{rng}:300"), (" 400 ", f"inj:{rng}:400")],
                        [(" 500 ", f"inj:{rng}:500"), ("✏️ Custom", f"inj_custom:{rng}")],
                        [("◀ Kembali", "m:addrange")],
                    ]))
            return

        if step == "inject_custom":
            rng = st.get("range", "")
            state_del(cid)
            try:
                qty = max(10, min(int(text.strip()), 9999))
            except:
                if smid: edit(cid, smid, f"<b>❌ Input tidak valid</b>\n\n  Masukkan angka (10–9999)", kb_back("m:addrange"))
                return
            if smid:
                cname = rng.replace("__top__:", "")
                back  = "m:addrange" if not rng.startswith("__top__:") else "m:monitor"
                txt, markup = page_confirm(
                    "📤 INJECT RANGE",
                    f"  {flag(cname)} <b>{esc(cname)}</b>\n  Jumlah  : <b>{qty}</b> nomor",
                    yes_cb=f"inj_go:{rng}:{qty}",
                    back_cb=back,
                )
                edit(cid, smid, txt, markup)
            return

        if step == "login_input":
            state_del(cid)
            parts = text.strip().split(None, 1)
            if len(parts) < 2:
                if smid: edit(cid, smid, f"<b>❌ Format salah!</b>\n\n  Kirim: <code>email password</code>", kb_back("m:login"))
                return
            em_in, pw_in = parts[0].lower(), parts[1]
            if not re.match(r"[^@]+@[^@]+\.[^@]+", em_in):
                if smid: edit(cid, smid, f"<b>❌ Email tidak valid!</b>", kb_back("m:login"))
                return
            save_account(em_in, pw_in, cid)
            _saved_account_msg(cid, smid, em_in, pw_in)
            return

    parts = text.split()
    if not parts: return
    cmd  = parts[0].lower().split("@")[0]
    args = " ".join(parts[1:]).strip()

    if is_group and cmd != "/addgrup": return
    _del_if_private()

    if cmd == "/start":
        txt, markup = page_home(cid)
        send(cid, txt, markup)

    elif cmd in ("/login", "/setlogin"):
        if not args:
            send(cid,
                f"<b>⚙️ SET AKUN iVAS</b>\n{div()}\n\n"
                f"  Format:\n  <code>/login email password</code>\n\n"
                f"  Contoh:\n  <code>/login user@gmail.com Pass123</code>")
            return
        p = args.split(None, 1)
        if len(p) < 2:
            send(cid, f"<b>❌ Format salah!</b>\n\n  <code>/login email password</code>")
            return
        save_account(p[0].lower(), p[1], cid)
        _saved_account_msg(cid, None, p[0].lower(), p[1])

    elif cmd == "/stop":
        stop_engine(cid)
        time.sleep(0.3)
        txt, markup = page_home(cid)
        send(cid, txt, markup)

    elif cmd == "/id":
        send(cid, f"<b>🪪 CHAT ID</b>\n{div()}\n\n  <code>{cid}</code>")

    elif cmd in ("/help", "/bantuan"):
        txt, markup = page_help()
        send(cid, txt, markup)

    elif cmd == "/addgrup":
        chat      = msg.get("chat", {})
        chat_type = chat.get("type", "private")
        title     = chat.get("title") or chat.get("first_name") or cid
        if chat_type == "private":
            send(cid,
                f"<b>⚠️ Kirim perintah ini dari dalam Grup!</b>\n{div()}\n\n"
                f"  1. Tambahkan bot ke grup kamu\n"
                f"  2. Kirim <code>/addgrup</code> di dalam grup tsb\n\n"
                f"  Chat ID kamu (private): <code>{cid}</code>")
            return
        result = add_group(cid, title)
        if result == "ok":
            send(cid,
                f"<b>✅ GRUP BERHASIL DITAMBAHKAN!</b>\n{div()}\n\n"
                f"  📢 <b>{esc(title)}</b>\n"
                f"  🆔 <code>{cid}</code>\n\n"
                f"  SMS baru akan langsung di-forward ke sini.")
            send(str(OWNER_ID),
                f"<b>📢 Grup baru didaftarkan!</b>\n{div()}\n\n"
                f"  <b>{esc(title)}</b>\n  <code>{cid}</code>")
        elif result == "exists":
            send(cid, f"<b>ℹ️ Grup ini sudah terdaftar.</b>\n  <code>{cid}</code>")
        elif result == "full":
            send(cid, f"<b>❌ Sudah {MAX_GROUPS} grup!</b>\n\n  Hapus dulu lewat menu 📢 Grup Forward.")

    elif cmd == "/delgrup":
        if str(cid) != str(OWNER_ID):
            send(cid, "⛔ Hanya owner yang bisa menghapus grup.")
            return
        if not args:
            groups = get_groups()
            if not groups:
                send(cid, "Belum ada grup terdaftar.")
                return
            lines = ["<b>Kirim:</b> <code>/delgrup CHAT_ID</code>\n"]
            for i, g in enumerate(groups, 1):
                lines.append(f"  {i}. {esc(g.get('title','?'))} — <code>{g['id']}</code>")
            send(cid, "\n".join(lines))
            return
        ok = remove_group(args.strip())
        send(cid, f"<b>✅ Dihapus.</b>" if ok else f"<b>❌ Tidak ditemukan: <code>{args.strip()}</code></b>")

    elif cmd == "/listgrup":
        groups = get_groups()
        if not groups:
            send(cid, "Belum ada grup terdaftar.")
            return
        lines = [f"<b>📢 GRUP FORWARD ({len(groups)}/{MAX_GROUPS})</b>\n{div()}"]
        for i, g in enumerate(groups, 1):
            lines.append(f"  {i}. <b>{esc(g.get('title','?'))}</b>\n     <code>{g['id']}</code>")
        send(cid, "\n".join(lines))

    else:
        txt, markup = page_home(cid)
        send(cid, txt, markup)

def _saved_account_msg(cid, smid, em_in, pw_in):
    local  = em_in.split("@")[0]
    domain = em_in.split("@")[1] if "@" in em_in else ""
    me     = (local[0] + "***" + local[-1] if len(local) > 2 else local[0] + "***") + f"@{domain}"
    mp     = pw_in[0] + "●" * (len(pw_in) - 1) if pw_in else "●●●"
    txt    = (
        f"<b>✅ AKUN TERSIMPAN!</b>\n{div()}\n\n"
        f"  📧 Email  <code>{me}</code>\n"
        f"  🔑 Pass   <code>{mp}</code>\n\n"
        f"  Tekan ▶ Start Engine untuk mulai:"
    )
    markup = kb([[(f"▶ Start Engine", "engine:start")], [(f"◀ Kembali", "m:home")]])
    if smid: edit(cid, smid, txt, markup)
    else:    send(cid, txt, markup)

def handle_callback(cb):
    cb_id   = cb["id"]
    data    = cb.get("data", "")
    msg     = cb["message"]
    cid     = str(msg["chat"]["id"])
    mid     = msg["message_id"]
    from_id = str(cb.get("from", {}).get("id", ""))

    if from_id != str(OWNER_ID):
        answer(cb_id, "⛔ Hanya owner yang bisa menggunakan bot ini.", alert=True)
        return

    answer(cb_id)
    online = is_online(cid)

    if data == "m:home":
        txt, markup = page_home(cid)
        edit(cid, mid, txt, markup)

    elif data == "m:help":
        txt, markup = page_help()
        edit(cid, mid, txt, markup)

    elif data == "m:grup":
        txt, markup = page_grup(cid)
        edit(cid, mid, txt, markup)

    elif data == "grup:howto":
        edit(cid, mid,
            f"<b>ℹ️ CARA DAFTARKAN GRUP</b>\n{div()}\n\n"
            f"  1️⃣ Tambahkan bot ke grup Telegram kamu\n"
            f"  2️⃣ Buka grup tersebut\n"
            f"  3️⃣ Ketik <code>/addgrup</code> di dalam grup\n"
            f"  4️⃣ Bot langsung terdaftar ✅\n\n"
            f"  Maksimal <b>{MAX_GROUPS} grup</b>.",
            kb([[("◄ Kembali", "m:grup")]]))

    elif data.startswith("delgrup:"):
        gid = data.split(":", 1)[1]
        ok  = remove_group(gid)
        answer(cb_id, "✅ Grup dihapus" if ok else "❌ Tidak ditemukan", alert=not ok)
        txt, markup = page_grup(cid)
        edit(cid, mid, txt, markup)

    elif data.startswith("copy:"):
        otp_val = data.split(":", 1)[1]
        answer(cb_id, f"✅ OTP: {otp_val}", alert=True)

    elif data.startswith("num:"):
        num_val = data.split(":", 1)[1]
        answer(cb_id, f"📱 {num_val}", alert=True)

    elif data.startswith("rev:"):
        answer(cb_id)

    elif data.startswith("cpnum:"):
        answer(cb_id)

    elif data.startswith("ch:"):
        try:
            payload_str = data[3:]
            parts       = payload_str.split("|", 3)
            gid   = parts[0] if len(parts) > 0 else ""
            phone = parts[1] if len(parts) > 1 else ""
            otp   = parts[2] if len(parts) > 2 else ""
            rng   = parts[3] if len(parts) > 3 else ""
            fl    = flag(rng)
            from engine import _mask_phone
            masked = _mask_phone(phone, with_cc=True)
            if not gid:
                answer(cb_id, "❌ Channel tidak valid", alert=True)
                return
            fwd_text = f"<b>WS</b> | <code>{masked}</code> | {fl}"
            if otp: fwd_text += f"\n\n<b><code>{otp}</code></b>"
            r = _tg.post(f"{TG_API}/sendMessage", json={
                "chat_id": gid, "text": fwd_text, "parse_mode": "HTML"
            }, timeout=10)
            if r and r.json().get("ok"):
                answer(cb_id, "✅ OTP dikirim ke channel!", alert=False)
            else:
                err = r.json().get("description", "") if r else "timeout"
                answer(cb_id, f"❌ Gagal: {err[:40]}", alert=True)
        except Exception as ex:
            answer(cb_id, f"❌ Error: {str(ex)[:40]}", alert=True)

    elif data == "m:monitor":
        if not online:
            answer(cb_id, "⚠️ Engine offline — start dulu!", alert=True)
            return
        txt, markup = page_monitor(cid)
        edit(cid, mid, txt, markup)

    elif data == "m:addrange":
        if not online:
            answer(cb_id, "⚠️ Engine offline — start dulu!", alert=True)
            return
        txt, markup = page_addrange()
        edit(cid, mid, txt, markup)

    elif data == "m:export":
        if not online:
            answer(cb_id, "⚠️ Engine offline — start dulu!", alert=True)
            return
        txt, markup = page_export()
        edit(cid, mid, txt, markup)

    elif data == "m:hapus":
        if not online:
            answer(cb_id, "⚠️ Engine offline — start dulu!", alert=True)
            return
        txt, markup = page_hapus()
        edit(cid, mid, txt, markup)

    elif data == "m:getsms":
        if not online:
            answer(cb_id, "⚠️ Engine offline — start dulu!", alert=True)
            return
        today = server_today()
        txt, markup = page_confirm(
            "📩 GET SMS",
            f"  Ambil laporan SMS hari ini\n  dari portal iVAS.\n\n  📅 <b>{today}</b>  <i>(UTC)</i>",
            yes_cb="confirm:getsms",
            back_cb="m:home",
        )
        edit(cid, mid, txt, markup)

    elif data == "m:login":
        txt, markup = page_login(cid)
        edit(cid, mid, txt, markup)

    elif data == "engine:start":
        if online:
            answer(cb_id, "Engine sudah aktif!")
            return
        acc = get_account(cid)
        if not acc.get("email") or not acc.get("password"):
            edit(cid, mid,
                f"<b>⚠️ AKUN BELUM DIATUR</b>\n{div()}\n\n"
                f"  Set akun iVAS terlebih dahulu:\n\n"
                f"  <code>/login email password</code>",
                kb([[(f"✏️ Set Akun", "login:input")], [(f"◀ Kembali", "m:home")]]))
            return
        txt, markup = page_confirm(
            "▶ START ENGINE",
            f"  📧 <code>{esc(acc.get('email',''))}</code>\n\n"
            f"  Bot akan login ke iVAS\n  dan mulai monitor OTP.",
            yes_cb="engine:start:go",
            back_cb="m:home",
        )
        edit(cid, mid, txt, markup)

    elif data == "engine:start:go":
        if online:
            answer(cb_id, "Engine sudah aktif!")
            return
        acc = get_account(cid)
        edit(cid, mid,
            f"<b>{BOT_NAME}</b>\n{div()}\n\n"
            f"  📧 <code>{esc(acc.get('email',''))}</code>\n\n"
            f"{div('MENGHUBUNGKAN')}\n"
            f"  ◌ Browser    menunggu\n"
            f"  ◌ Login iVAS menunggu\n"
            f"  ◌ Hub Socket menunggu")
        start_engine(cid, msg_id=mid)

    elif data == "engine:stop":
        txt, markup = page_confirm(
            "■ STOP ENGINE",
            f"  Engine akan dihentikan.\n"
            f"  Live OTP dan auto-forward\n  akan berhenti.",
            yes_cb="engine:stop:go",
            back_cb="m:home",
            yes_label="■  Ya, Stop Engine",
        )
        edit(cid, mid, txt, markup)

    elif data == "engine:stop:go":
        stop_engine(cid)
        time.sleep(0.3)
        txt, markup = page_home(cid)
        edit(cid, mid, txt, markup)

    elif data == "addrange:input":
        edit(cid, mid,
            f"<b>📤 ADD RANGE</b>\n{div()}\n\n"
            f"  Ketik nama range:\n\n"
            f"  <code>NIGERIA 14603</code>",
            kb_back("m:addrange"))
        state_set(cid, {"step": "addrange_name", "mid": mid})

    elif data.startswith("inj:"):
        parts = data.split(":", 2)
        if len(parts) < 3: return
        rng, qty = parts[1], parts[2]
        if not online:
            answer(cb_id, "Engine offline!", alert=True)
            return
        txt, markup = page_confirm(
            "📤 INJECT RANGE",
            f"  {flag(rng)} <b>{esc(rng)}</b>\n  Jumlah  : <b>{qty}</b> nomor",
            yes_cb=f"inj_go:{rng}:{qty}",
            back_cb=f"inj_direct:{rng}",
        )
        edit(cid, mid, txt, markup)

    elif data.startswith("inj_go:"):
        parts = data.split(":", 2)
        if len(parts) < 3: return
        rng, qty = parts[1], int(parts[2])
        if not online:
            answer(cb_id, "Engine offline!", alert=True)
            return
        threading.Thread(target=_do_inject, args=(cid, mid, rng, qty), daemon=True).start()

    elif data.startswith("inj_custom:"):
        rng = data.split(":", 1)[1]
        edit(cid, mid,
            f"<b>📤 ADD RANGE</b>\n{div()}\n\n"
            f"  Range  <code>{esc(rng)}</code>\n\n"
            f"  Ketik jumlah nomor (10–9999):",
            kb_back("m:addrange"))
        state_set(cid, {"step": "inject_custom", "range": rng, "mid": mid})

    elif data.startswith("inj_direct:"):
        full_range = data.split(":", 1)[1]
        if not online:
            answer(cb_id, "Engine offline!", alert=True)
            return
        edit(cid, mid,
            f"<b>📤 ADD RANGE</b>\n{div()}\n\n"
            f"  {flag(full_range)} <code>{esc(full_range)}</code>\n\n"
            f"{div('PILIH JUMLAH')}",
            kb([
                [(" 100 ", f"inj:{full_range}:100"), (" 200 ", f"inj:{full_range}:200")],
                [(" 300 ", f"inj:{full_range}:300"), (" 400 ", f"inj:{full_range}:400")],
                [(" 500 ", f"inj:{full_range}:500"), ("✏️ Custom", f"inj_custom:{full_range}")],
                [("◀ Kembali", "m:monitor")],
            ]))

    elif data.startswith("inj_top:"):
        country = data.split(":", 1)[1]
        if not online:
            answer(cb_id, "Engine offline!", alert=True)
            return
        edit(cid, mid,
            f"<b>📤 ADD RANGE</b>\n{div()}\n\n"
            f"  {flag(country)} <b>{esc(country)}</b>\n\n"
            f"{div('PILIH JUMLAH')}",
            kb([
                [(" 100 ", f"inj_top_qty:{country}:100"), (" 200 ", f"inj_top_qty:{country}:200")],
                [(" 300 ", f"inj_top_qty:{country}:300"), (" 400 ", f"inj_top_qty:{country}:400")],
                [(" 500 ", f"inj_top_qty:{country}:500"), ("✏️ Custom", f"inj_top_custom:{country}")],
                [("◀ Kembali", "m:monitor")],
            ]))

    elif data.startswith("inj_top_qty:"):
        parts = data.split(":", 2)
        if len(parts) < 3: return
        country, qty = parts[1], parts[2]
        if not online:
            answer(cb_id, "Engine offline!", alert=True)
            return
        txt, markup = page_confirm(
            "📤 INJECT TOP RANGE",
            f"  {flag(country)} <b>{esc(country)}</b>\n  Jumlah  : <b>{qty}</b> nomor",
            yes_cb=f"inj_top_go:{country}:{qty}",
            back_cb=f"inj_top:{country}",
        )
        edit(cid, mid, txt, markup)

    elif data.startswith("inj_top_go:"):
        parts = data.split(":", 2)
        if len(parts) < 3: return
        country, qty = parts[1], int(parts[2])
        if not online:
            answer(cb_id, "Engine offline!", alert=True)
            return
        threading.Thread(target=_do_inject_top, args=(cid, mid, country, qty), daemon=True).start()

    elif data.startswith("inj_top_custom:"):
        country = data.split(":", 1)[1]
        edit(cid, mid,
            f"<b>📤 ADD RANGE</b>\n{div()}\n\n"
            f"  {flag(country)} <b>{esc(country)}</b>\n\n"
            f"  Ketik jumlah nomor (10–9999):",
            kb_back("m:monitor"))
        state_set(cid, {"step": "inject_custom", "range": f"__top__:{country}", "mid": mid})

    elif data == "export:go":
        us = _get_us(cid)
        if us.get("busy") and us["busy"].is_set():
            answer(cb_id, "Engine sedang sibuk!", alert=True)
            return
        edit(cid, mid, f"<b>📋 EXPORT NOMOR</b>\n{div()}\n\n  ⟳ Memulai export...", kb_back())
        threading.Thread(target=_do_export, args=(cid, mid), daemon=True).start()

    elif data == "hapus:go":
        us = _get_us(cid)
        if us.get("busy") and us["busy"].is_set():
            answer(cb_id, "Engine sedang sibuk!", alert=True)
            return
        edit(cid, mid,
            f"<b>🗑 HAPUS NOMOR</b>\n{div()}\n\n"
            f"  ⟳ Membuka halaman My Numbers...\n"
            f"  ⟳ Mencari tombol Bulk return all numbers...", None)
        threading.Thread(target=_do_hapus, args=(cid, mid), daemon=True).start()

    elif data.startswith("confirm:"):
        action = data.split(":", 1)[1]
        if action == "getsms":
            if not online:
                answer(cb_id, "⚠️ Engine offline!", alert=True)
                return
            today = server_today()
            edit(cid, mid,
                f"<b>📩 GET SMS</b>\n{div()}\n\n"
                f"  ⟳ Mengambil data SMS hari ini...\n"
                f"  📅 {today}  <i>(UTC)</i>\n\n"
                f"  <i>Mohon tunggu...</i>", None)
            threading.Thread(target=_do_getsms, args=(cid, mid, today, today), daemon=True).start()
        elif action == "export":
            us = _get_us(cid)
            if us.get("busy") and us["busy"].is_set():
                answer(cb_id, "Engine sedang sibuk!", alert=True)
                return
            edit(cid, mid, f"<b>📋 EXPORT NOMOR</b>\n{div()}\n\n  ⟳ Memulai export...", None)
            threading.Thread(target=_do_export, args=(cid, mid), daemon=True).start()
        elif action == "hapus":
            us = _get_us(cid)
            if us.get("busy") and us["busy"].is_set():
                answer(cb_id, "Engine sedang sibuk!", alert=True)
                return
            edit(cid, mid,
                f"<b>🗑 HAPUS NOMOR</b>\n{div()}\n\n"
                f"  ⟳ Membuka halaman My Numbers...\n"
                f"  ⟳ Mencari tombol Bulk return all numbers...", None)
            threading.Thread(target=_do_hapus, args=(cid, mid), daemon=True).start()

    elif data == "login:input":
        edit(cid, mid,
            f"<b>⚙️ GANTI AKUN</b>\n{div()}\n\n  Kirim: <code>email password</code>",
            kb_back("m:login"))
        state_set(cid, {"step": "login_input", "mid": mid})

def _do_inject(cid, mid, rng, qty):
    if rng.startswith("__top__:"):
        country = rng.split(":", 1)[1]
        _do_inject_top(cid, mid, country, qty)
        return
    us   = _get_us(cid)
    busy = us.get("busy")
    if busy: busy.set()
    try:
        drv = get_drv(cid)
        if not drv:
            edit(cid, mid,
                f"<b>❌ ENGINE OFFLINE</b>\n{div()}\n\n  Start engine terlebih dahulu.",
                kb([[("▶ Start Engine", "engine:start")], [("◀ Kembali", "m:addrange")]]))
            return
        edit(cid, mid,
            f"<b>📤 ADD RANGE</b>\n{div()}\n\n"
            f"  Range   <code>{esc(rng)}</code>\n"
            f"  Target  {fmt(qty)} nomor\n\n"
            f"  ⟳ Menghubungi Hub...", kb_back("m:addrange"))
        acc   = get_account(cid)
        em    = acc.get("email", "")
        drv.get("https://hub.orangecarrier.com?system=ivas")
        time.sleep(1)
        from engine import hub_info, init_hub
        info  = hub_info(drv)
        if not info.get("email"): init_hub(drv, em)

        def on_progress(pct, ok, fail, done):
            edit(cid, mid,
                f"<b>📤 ADD RANGE</b>\n{div()}\n\n"
                f"  Range   <code>{esc(rng)}</code>\n"
                f"  Target  {fmt(qty)} nomor\n\n"
                f"{div('PROGRESS')}\n"
                f"  {pct}% — ✅ {ok} req  ❌ {fail} req\n"
                f"  ~{fmt(done)} nomor ditambahkan",
                kb_back("m:addrange"))

        ok, fail, done = do_inject_hub(drv, rng, qty, on_progress, em)
        status = "✅ SELESAI" if fail == 0 else ("⚠️ SEBAGIAN" if ok > 0 else "❌ GAGAL")
        edit(cid, mid,
            f"<b>📤 INJECT {status}</b>\n{div()}\n\n"
            f"  Range      <code>{esc(rng)}</code>\n"
            f"  Target     {fmt(qty)} nomor\n\n"
            f"{div('HASIL')}\n"
            f"  ✅ Berhasil  {ok} req  (~{fmt(done)} nomor)\n"
            f"  ❌ Gagal     {fail} req",
            kb([[("📤 Inject Lagi", "m:addrange")], [("◀ Kembali", "m:home")]]))
    except Exception as e:
        log.error(f"_do_inject: {e}")
        edit(cid, mid,
            f"<b>❌ INJECT ERROR</b>\n{div()}\n\n  <code>{esc(str(e)[:300])}</code>",
            kb_back("m:addrange"))
    finally:
        if busy: busy.clear()

def _do_inject_top(cid, mid, country, qty):
    us   = _get_us(cid)
    busy = us.get("busy")
    if busy: busy.set()
    try:
        drv = get_drv(cid)
        if not drv:
            edit(cid, mid,
                f"<b>❌ ENGINE OFFLINE</b>\n{div()}\n\n  Start engine terlebih dahulu.",
                kb([[("▶ Start Engine", "engine:start")], [("◀ Kembali", "m:monitor")]]))
            return
        edit(cid, mid,
            f"<b>📤 INJECT TOP RANGE</b>\n{div()}\n\n"
            f"  {flag(country)} <b>{esc(country)}</b>\n"
            f"  Target  {fmt(qty)} nomor\n\n"
            f"  ⟳ Mencari Range ID...",
            kb_back("m:monitor"))
        acc = get_account(cid)
        em  = acc.get("email", "")

        def on_progress(pct, ok, fail, done):
            edit(cid, mid,
                f"<b>📤 INJECT TOP RANGE</b>\n{div()}\n\n"
                f"  {flag(country)} <b>{esc(country)}</b>\n"
                f"  Target  {fmt(qty)} nomor\n\n"
                f"{div('PROGRESS')}\n"
                f"  {pct}% — ✅ {ok} req  ❌ {fail} req\n"
                f"  ~{fmt(done)} nomor ditambahkan",
                kb_back("m:monitor"))

        ok, fail, done = inject_top_range_with_id(drv, country, qty, on_progress, em)
        status = "✅ SELESAI" if fail == 0 else ("⚠️ SEBAGIAN" if ok > 0 else "❌ GAGAL")
        edit(cid, mid,
            f"<b>📤 INJECT TOP {status}</b>\n{div()}\n\n"
            f"  {flag(country)} <b>{esc(country)}</b>\n"
            f"  Target     {fmt(qty)} nomor\n\n"
            f"{div('HASIL')}\n"
            f"  ✅ Berhasil  {ok} req  (~{fmt(done)} nomor)\n"
            f"  ❌ Gagal     {fail} req",
            kb([[("📊 Monitor", "m:monitor")], [("◀ Kembali", "m:home")]]))
    except Exception as e:
        log.error(f"_do_inject_top: {e}")
        edit(cid, mid,
            f"<b>❌ INJECT ERROR</b>\n{div()}\n\n  <code>{esc(str(e)[:300])}</code>",
            kb_back("m:monitor"))
    finally:
        if busy: busy.clear()

def _do_export(cid, mid):
    us   = _get_us(cid)
    busy = us.get("busy")
    if busy: busy.set()
    xl = txt_path = None
    try:
        drv = get_drv(cid)
        if not drv:
            edit(cid, mid, f"<b>❌ ENGINE OFFLINE</b>", kb_back())
            return
        edit(cid, mid, f"<b>📋 EXPORT NOMOR</b>\n{div()}\n\n  ⟳ Membuka portal nomor...", kb_back())
        xl      = do_export_excel(drv)
        numbers = []
        if xl:
            edit(cid, mid, f"<b>📋 EXPORT NOMOR</b>\n{div()}\n\n  ✅ File Excel diterima\n  ⟳ Membaca data...", kb_back())
            try: numbers = parse_xlsx(xl)
            except: pass
        if not numbers:
            edit(cid, mid, f"<b>📋 EXPORT NOMOR</b>\n{div()}\n\n  ⟳ Scraping tabel halaman...", kb_back())
            numbers = scrape_numbers_page(drv)
        if not numbers:
            edit(cid, mid, f"<b>📋 EXPORT NOMOR</b>\n{div()}\n\n  ⚠️ Tidak ada nomor aktif di portal.",
                 kb([[("◀ Kembali", "m:home")]]))
            return
        ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
        txt_path = os.path.join(DL_DIR, f"IVAS_NOMOR_{ts}.txt")
        with open(txt_path, "w") as f:
            f.write("\n".join(numbers))
        edit(cid, mid, f"<b>📋 EXPORT NOMOR</b>\n{div()}\n\n  ✅ {fmt(len(numbers))} nomor\n  ⟳ Mengirim file...", kb_back())
        cap = (
            f"<b>📋 NOMOR AKTIF iVAS</b>\n{div()}\n\n"
            f"  Total     {fmt(len(numbers))} nomor\n"
            f"  Tanggal   {datetime.now().strftime('%d %b %Y  %H:%M')}"
        )
        if send_doc(cid, txt_path, cap):
            edit(cid, mid,
                f"<b>📋 EXPORT SELESAI</b>\n{div()}\n\n  ✅ {fmt(len(numbers))} nomor berhasil dikirim.",
                kb([[("📋 Export Lagi", "export:go")], [("◀ Kembali", "m:home")]]))
        else:
            edit(cid, mid, f"<b>❌ GAGAL KIRIM FILE</b>", kb_back())
    except Exception as e:
        log.error(f"_do_export: {e}")
        edit(cid, mid, f"<b>❌ EXPORT ERROR</b>\n{div()}\n\n  <code>{esc(str(e)[:300])}</code>", kb_back())
    finally:
        for p in [xl, txt_path]:
            if p:
                try: os.remove(p)
                except: pass
        if busy: busy.clear()

def _do_hapus(cid, mid):
    us   = _get_us(cid)
    busy = us.get("busy")
    if busy: busy.set()
    try:
        drv = get_drv(cid)
        if not drv:
            edit(cid, mid, f"<b>❌ ENGINE OFFLINE</b>", kb_back())
            return
        ok = do_bulk_return(drv)
        if ok:
            edit(cid, mid,
                f"<b>✅ HAPUS BERHASIL</b>\n{div()}\n\n"
                f"  Semua nomor berhasil dikembalikan ke sistem.",
                kb([[("◀ Kembali", "m:home")]]))
        else:
            edit(cid, mid,
                f"<b>⚠️ PROSES TERKIRIM</b>\n{div()}\n\n"
                f"  Perintah hapus sudah dikirim.\n"
                f"  Cek halaman My Numbers untuk verifikasi.",
                kb([[("◀ Kembali", "m:home")]]))
    except Exception as e:
        log.error(f"_do_hapus: {e}")
        edit(cid, mid, f"<b>❌ HAPUS ERROR</b>\n{div()}\n\n  <code>{esc(str(e)[:300])}</code>", kb_back())
    finally:
        if busy: busy.clear()

def _do_getsms(cid, mid, start_date, end_date):
    us   = _get_us(cid)
    busy = us.get("busy")
    if busy: busy.set()
    try:
        drv = get_drv(cid)
        if not drv:
            edit(cid, mid, f"<b>❌ ENGINE OFFLINE</b>", kb_back("m:getsms"))
            return
        edit(cid, mid,
            f"<b>📩 GET SMS</b>\n{div()}\n\n"
            f"  ⟳ Membuka SMS Received...\n"
            f"  📅 {start_date} → {end_date}",
            kb_back("m:getsms"))
        result  = do_get_sms(drv, start_date, end_date)
        summary = result.get("summary", [])
        stats   = result.get("stats", {})
        if not summary:
            edit(cid, mid,
                f"<b>📩 GET SMS</b>\n{div()}\n\n"
                f"  ℹ️ Tidak ada SMS dalam periode ini.\n\n"
                f"  📅 {start_date} → {end_date}",
                kb([[("📩 Coba Lagi", "m:getsms")], [("◀ Kembali", "m:home")]]))
            return

        def _num(v):
            try: return int(str(v).replace(",", "").replace(".", ""))
            except: return 0

        total_sms  = _num(stats.get("total", "0")) or sum(_num(r.get("count", "0")) for r in summary)
        total_paid = _num(stats.get("paid", "0"))  or sum(_num(r.get("paid", "0"))  for r in summary)
        revenue_s  = stats.get("revenue", "─")

        from engine import _fetch_sms_detail_selenium, _fetch_sms_detail, _mask_phone, _extract_otp
        import re as _re
        import html as _html
        import re as _re2

        detail_map = {}
        for r in summary:
            rng_name = r.get("range", "").strip()
            rng_id   = (r.get("range_id") or "").strip()
            if not rng_id and rng_name:
                rng_id = _re.sub(r"[^A-Z0-9_]", "_", rng_name.upper().strip())
            if rng_name:
                try:
                    sms_list = _fetch_sms_detail_selenium(drv, rng_id, rng_name)
                    if not sms_list:
                        sms_list = _fetch_sms_detail(drv, start_date, rng_id, rng_name)
                    detail_map[rng_name] = sms_list
                except Exception as _e:
                    log.warning(f"detail fetch {rng_name}: {_e}")
                    detail_map[rng_name] = []

        def _split_range(name):
            parts = name.rsplit(" ", 1)
            if len(parts) == 2 and parts[1].isdigit(): return parts[0], parts[1]
            return name, ""

        _NUM_EMOJI = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
        def _num_emoji(n): return _NUM_EMOJI[n-1] if 1 <= n <= 10 else f"{n}."

        from datetime import datetime, timezone
        utc_label = datetime.now(timezone.utc).strftime("%Y-%m-%d  %H:%M UTC")

        lines = [
            f"<b>📩 SMS RECEIVED</b>",
            f"{div()}",
            f"",
            f"  📅 {start_date}  <i>({utc_label})</i>",
            f"",
            f"  📊 Total SMS : {fmt(total_sms)}",
            f"  ✅ Paid      : {fmt(total_paid)}",
        ]
        if revenue_s and revenue_s != "─":
            lines.append(f"  💰 Revenue   : {esc(revenue_s)}")

        for r in summary:
            rng_name         = r.get("range", "?")
            cnt, paid, rev   = r.get("count", "0"), r.get("paid", "0"), r.get("revenue", "─")
            fl               = flag(rng_name)
            cname, cnum      = _split_range(rng_name)
            rng_label        = f"{cname} ({cnum})" if cnum else rng_name
            lines += [f"", f"{div()}", f"  {fl} <b>{esc(rng_label)}</b>",
                      f"  📊 {cnt} SMS | ✅ {paid} Paid | 💰 {esc(rev)}", f""]
            sms_list = detail_map.get(rng_name, [])
            if not sms_list:
                lines.append(f"  <i>∙ detail tidak tersedia</i>")
                continue
            phone_order  = []
            phone_groups = {}
            for sms in sms_list:
                ph = sms.get("phone", "?")
                if ph not in phone_groups:
                    phone_groups[ph] = []
                    phone_order.append(ph)
                phone_groups[ph].append(sms)
            for idx, phone in enumerate(phone_order):
                msgs   = phone_groups[phone]
                masked = _mask_phone(phone, with_cc=True)
                badge  = "WS" if "whatsapp" in (msgs[0].get("sender","")).lower() else (msgs[0].get("sender","SMS")[:2].upper() or "SMS")
                nem    = _num_emoji(idx + 1)
                lines.append(f"  {nem} 📱 {badge} | <code>{masked}</code> | {fl}")
                otps = []
                for sms in msgs:
                    msg_clean = _re2.sub(r'<[^>]+>', '', _html.unescape(sms.get("message",""))).strip()
                    otp       = _extract_otp(msg_clean)
                    if otp and otp not in otps: otps.append(otp)
                if not otps:
                    for sms in msgs:
                        msg_clean = _re2.sub(r'<[^>]+>', '', _html.unescape(sms.get("message",""))).strip()
                        msg_cut   = (msg_clean[:80] + "…") if len(msg_clean) > 80 else msg_clean
                        lines.append(f"   └ 💬 <i>{esc(msg_cut)}</i>")
                elif len(otps) == 1:
                    lines.append(f"   └ 🔑 OTP: <b><code>{otps[0]}</code></b>")
                else:
                    for i, otp in enumerate(otps):
                        prefix = "  ├" if i < len(otps) - 1 else "   └"
                        lines.append(f"  {prefix} 🔑 OTP {i+1}: <b><code>{otp}</code></b>")

        edit(cid, mid, "\n".join(lines),
            kb([[("📩 Get SMS Lagi", "m:getsms")], [("◀ Kembali", "m:home")]]))
    except Exception as e:
        log.error(f"_do_getsms: {e}")
        edit(cid, mid, f"<b>❌ GET SMS ERROR</b>\n{div()}\n\n  <code>{esc(str(e)[:300])}</code>",
             kb_back("m:getsms"))
    finally:
        if busy: busy.clear()
