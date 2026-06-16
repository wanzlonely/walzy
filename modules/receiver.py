import imaplib
import email
import asyncio
import ssl
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from config import IMAP_SERVER, IMAP_PORT
from modules import storage

USE_STORED, CHECK_EMAIL, CHECK_PASS = range(3)

_BTN_HOME = [[InlineKeyboardButton("🔙 Menu Utama", callback_data="home")]]


def _check_inbox(check_email: str, check_pass: str) -> str:
    # Buat konteks SSL yang mengabaikan pengecekan hostname
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    # Masukkan ctx ke dalam koneksi IMAP lokal
    mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT, ssl_context=ctx)
    mail.login(check_email, check_pass)
    mail.select("INBOX")

    status, messages = mail.search(None, "ALL")
    email_ids        = messages[0].split()

    if not email_ids:
        mail.logout()
        return (
            "╔══════════════════╗\n"
            "║   📭  INBOX KOSONG   ║\n"
            "╚══════════════════╝\n\n"
            "Belum ada email masuk."
        )

    latest_ids  = email_ids[-3:]
    result_text = (
        "╔══════════════════╗\n"
        "║   📥  3 PESAN TERBARU  ║\n"
        "╚══════════════════╝\n\n"
        f"📧 `{check_email}`\n"
        "─────────────────────\n\n"
    )

    for e_id in reversed(latest_ids):
        res, msg_data = mail.fetch(e_id, "(RFC822)")
        for response_part in msg_data:
            if isinstance(response_part, tuple):
                msg          = email.message_from_bytes(response_part[1])
                subj_decode  = email.header.decode_header(msg["Subject"])[0]
                subject      = subj_decode[0]
                if isinstance(subject, bytes):
                    subject = subject.decode(
                        subj_decode[1] if subj_decode[1] else "utf-8", errors="ignore"
                    )
                sender       = msg.get("From")
                result_text += f"👤 *Dari:* `{sender}`\n🏷️ *Subjek:* {subject}\n\n"

    mail.logout()
    return result_text.strip()


async def start_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    user_id = update.effective_user.id
    stored  = storage.get_email(user_id)

    if stored:
        keyboard = [
            [InlineKeyboardButton("✅ Gunakan Email Ini",  callback_data="use_stored_inbox")],
            [InlineKeyboardButton("🔄 Ganti Email",        callback_data="change_inbox_email")],
            [InlineKeyboardButton("🔙 Menu Utama",          callback_data="home")],
        ]
        await update.callback_query.edit_message_text(
            "╔══════════════════╗\n"
            "║     📥  CEK INBOX    ║\n"
            "╚══════════════════╝\n\n"
            f"📧 *Email tersimpan:*\n`{stored['email']}`\n\n"
            "Gunakan email ini atau ganti?",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )
        return USE_STORED
    else:
        await update.callback_query.edit_message_text(
            "╔══════════════════╗\n"
            "║     📥  CEK INBOX    ║\n"
            "╚══════════════════╝\n\n"
            "Masukkan *alamat email* yang ingin dicek:",
            reply_markup=InlineKeyboardMarkup(_BTN_HOME),
            parse_mode="Markdown",
        )
        return CHECK_EMAIL


async def handle_use_stored(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    user_id = update.effective_user.id

    if query.data == "use_stored_inbox":
        stored = storage.get_email(user_id)
        msg_ui = await query.edit_message_text("📡 Menghubungkan ke server IMAP...")
        try:
            result_text = await asyncio.to_thread(
                _check_inbox, stored["email"], stored["password"]
            )
            await msg_ui.edit_text(
                result_text,
                reply_markup=InlineKeyboardMarkup(_BTN_HOME),
                parse_mode="Markdown",
            )
        except Exception as e:
            await msg_ui.edit_text(
                f"❌ *Gagal Cek Inbox!*\n`{e}`",
                reply_markup=InlineKeyboardMarkup(_BTN_HOME),
                parse_mode="Markdown",
            )
        return ConversationHandler.END

    elif query.data == "change_inbox_email":
        await query.edit_message_text(
            "📥 *Masukkan alamat email:*",
            reply_markup=InlineKeyboardMarkup(_BTN_HOME),
            parse_mode="Markdown",
        )
        return CHECK_EMAIL

    elif query.data == "home":
        return await cancel_callback(update, context)


async def get_check_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["check_email"] = update.message.text.strip()
    await update.message.reply_text(
        "🔑 *Masukkan password email:*",
        reply_markup=InlineKeyboardMarkup(_BTN_HOME),
        parse_mode="Markdown",
    )
    return CHECK_PASS


async def process_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    check_pass  = update.message.text.strip()
    check_email = context.user_data["check_email"]
    user_id     = update.effective_user.id

    msg_ui = await update.message.reply_text("📡 Menghubungkan ke server IMAP...")

    try:
        result_text = await asyncio.to_thread(_check_inbox, check_email, check_pass)
        storage.set_email(user_id, check_email, check_pass)
        await msg_ui.edit_text(
            result_text,
            reply_markup=InlineKeyboardMarkup(_BTN_HOME),
            parse_mode="Markdown",
        )
    except imaplib.IMAP4.error:
        await msg_ui.edit_text(
            "❌ *Email atau password salah!*\nMasukkan password yang benar:",
            reply_markup=InlineKeyboardMarkup(_BTN_HOME),
            parse_mode="Markdown",
        )
        return CHECK_PASS
    except Exception as e:
        await msg_ui.edit_text(
            f"❌ *Gagal Cek Inbox!*\n`{e}`",
            reply_markup=InlineKeyboardMarkup(_BTN_HOME),
            parse_mode="Markdown",
        )

    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "❌ Proses dibatalkan.",
        reply_markup=InlineKeyboardMarkup(_BTN_HOME),
    )
    return ConversationHandler.END


async def cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    from main import render_home
    await render_home(update, context)
    return ConversationHandler.END
