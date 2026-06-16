import subprocess
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from config import DOMAIN
from modules import storage

_BTN_HOME = [[InlineKeyboardButton("🔙 Menu Utama", callback_data="home")]]


async def create_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if storage.has_email(user_id):
        stored    = storage.get_email(user_id)
        tmpl_idx  = storage.get_template_index(user_id)
        await update.message.reply_text(
            "╔══════════════════╗\n"
            "║   ⚠️  AKUN AKTIF   ║\n"
            "╚══════════════════╝\n\n"
            f"📧 *Email:* `{stored['email']}`\n"
            f"🎨 *Template:* `#{tmpl_idx + 1}`\n\n"
            "Hapus akun lama terlebih dahulu\n"
            "sebelum membuat yang baru.",
            reply_markup=InlineKeyboardMarkup(_BTN_HOME),
            parse_mode="Markdown",
        )
        return

    if len(context.args) < 2:
        await update.message.reply_text(
            "╔══════════════════╗\n"
            "║   📋  FORMAT SALAH  ║\n"
            "╚══════════════════╝\n\n"
            "Ketik perintah:\n"
            "`/create <user> <password>`\n\n"
            "Contoh:\n"
            "`/create support rahasia123`\n\n"
            "📌 Username: huruf kecil & angka saja.",
            reply_markup=InlineKeyboardMarkup(_BTN_HOME),
            parse_mode="Markdown",
        )
        return

    email_user = context.args[0].lower().strip()
    email_pass = context.args[1].strip()
    full_email = f"{email_user}@{DOMAIN}"

    if not email_user.replace(".", "").replace("_", "").isalnum():
        await update.message.reply_text(
            "⚠️ *Username tidak valid!*\n"
            "Gunakan huruf, angka, titik, atau underscore saja.",
            reply_markup=InlineKeyboardMarkup(_BTN_HOME),
            parse_mode="Markdown",
        )
        return

    msg = await update.message.reply_text(
        f"⚙️ Membuat akun `{full_email}`\nMohon tunggu...",
        parse_mode="Markdown",
    )

    command = [
        "cyberpanel", "createEmail",
        "--domainName", DOMAIN,
        "--userName",   email_user,
        "--password",   email_pass,
    ]

    try:
        process = subprocess.run(command, capture_output=True, text=True, timeout=30)
        if process.returncode == 0:
            storage.set_email(user_id, full_email, email_pass)
            tmpl_idx  = storage.assign_template(user_id)
            from modules.sender import LANG_LABELS
            lang_label = LANG_LABELS[tmpl_idx]

            await msg.edit_text(
                "╔══════════════════╗\n"
                "║  ✅  AKUN DIBUAT!  ║\n"
                "╚══════════════════╝\n\n"
                f"📧 *Email:* `{full_email}`\n"
                f"🔑 *Password:* `{email_pass}`\n"
                f"🎨 *Template:* `#{tmpl_idx + 1}` — {lang_label}\n\n"
                "Kamu siap mengirim pesan!\n"
                "Kembali ke menu utama untuk mulai.",
                reply_markup=InlineKeyboardMarkup(_BTN_HOME),
                parse_mode="Markdown",
            )
        else:
            error_output = (process.stdout or process.stderr or "Unknown error").strip()
            await msg.edit_text(
                "╔══════════════════╗\n"
                "║  ❌  GAGAL DIBUAT  ║\n"
                "╚══════════════════╝\n\n"
                f"`{error_output}`",
                reply_markup=InlineKeyboardMarkup(_BTN_HOME),
                parse_mode="Markdown",
            )
    except subprocess.TimeoutExpired:
        await msg.edit_text(
            "⏱️ *Timeout!*\nServer tidak merespons. Coba lagi.",
            reply_markup=InlineKeyboardMarkup(_BTN_HOME),
            parse_mode="Markdown",
        )
    except Exception as e:
        await msg.edit_text(
            f"❌ *Error VPS!*\n`{e}`",
            reply_markup=InlineKeyboardMarkup(_BTN_HOME),
            parse_mode="Markdown",
        )


async def delete_email_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    stored  = storage.get_email(user_id)

    if not stored:
        from main import render_home
        await render_home(update, context)
        return

    email_user = stored["email"].split("@")[0]
    msg_ui     = await query.edit_message_text(
        f"🗑️ Menghapus `{stored['email']}` dari server...",
        parse_mode="Markdown",
    )

    command = [
        "cyberpanel", "deleteEmail",
        "--domainName", DOMAIN,
        "--userName",   email_user,
    ]

    try:
        process = subprocess.run(command, capture_output=True, text=True, timeout=30)
        storage.delete_email(user_id)

        if process.returncode == 0:
            await msg_ui.edit_text(
                "╔══════════════════╗\n"
                "║  🗑️  AKUN DIHAPUS  ║\n"
                "╚══════════════════╝\n\n"
                f"📧 `{stored['email']}` berhasil dihapus\ndari server CyberPanel.\n\n"
                "Ketik `/create` untuk membuat akun baru.",
                reply_markup=InlineKeyboardMarkup(_BTN_HOME),
                parse_mode="Markdown",
            )
        else:
            await msg_ui.edit_text(
                "⚠️ *Akun dihapus dari bot* — mungkin gagal di server.\n"
                "Hapus manual via CyberPanel jika perlu.\n\n"
                f"Error: `{(process.stdout or process.stderr or '').strip()}`",
                reply_markup=InlineKeyboardMarkup(_BTN_HOME),
                parse_mode="Markdown",
            )
    except Exception as e:
        storage.delete_email(user_id)
        await msg_ui.edit_text(
            f"⚠️ *Error saat hapus di server.*\n`{e}`\n\nAkun sudah dihapus dari bot.",
            reply_markup=InlineKeyboardMarkup(_BTN_HOME),
            parse_mode="Markdown",
        )
