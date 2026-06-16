import smtplib
import asyncio
import random
import uuid
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from config import SMTP_SERVER, SMTP_PORT
from modules import storage

USE_STORED, ASK_EMAIL, ASK_PASS, ASK_NOMOR = range(4)

DEFAULT_RECEIVER = "wanzlonely04@gmail.com"

DEFAULT_SUBJECT_LIST = [
    "Bantuan Registrasi WhatsApp - Nomor Diblokir",
    "Solicitação de Suporte - Número WhatsApp Bloqueado",
    "طلب دعم - مشكلة تسجيل واتساب",
    "Support Request - WhatsApp Number Registration Issue",
    "サポートリクエスト - WhatsApp番号登録の問題",
]

LANG_LABELS = ["🇮🇩 Indonesia", "🇧🇷 Português", "🇸🇦 العربية", "🇺🇸 English", "🇯🇵 日本語"]

_TEMPLATES = [
    (
        "Kepada Tim Dukungan,\n\n"
        "Saya mengalami masalah dalam mendaftarkan nomor {nomor}. "
        "Setiap kali mencoba, muncul pesan bahwa nomor tidak dapat digunakan. "
        "Nomor ini sangat penting bagi saya untuk keperluan komunikasi sehari-hari dan pendidikan. "
        "Saya sangat berharap tim dapat membantu menyelesaikan masalah ini "
        "sesegera mungkin agar saya dapat menggunakannya kembali.\n\n"
        "Terima kasih atas perhatian dan bantuannya."
    ),
    (
        "Prezada Equipe de Suporte,\n\n"
        "Estou com problemas para registrar meu número {nomor}. "
        "Sempre que tento, recebo a mensagem de que o número não está disponível. "
        "Este número é muito importante porque o utilizo para fins educacionais e de comunicação. "
        "Espero sinceramente que a equipe possa ajudar a resolver este problema "
        "o mais rápido possível para que eu possa usá-lo novamente.\n\n"
        "Agradeço a atenção e o apoio de todos."
    ),
    (
        "إلى فريق الدعم المحترم،\n\n"
        "أواجه مشكلة في تسجيل رقمي {nomor}. "
        "في كل مرة أحاول فيها، تظهر رسالة بأن الرقم غير متاح. "
        "هذا الرقم مهم جداً بالنسبة لي لأغراض التواصل اليومي والتعليم. "
        "آمل بصدق أن يتمكن الفريق من مساعدتي في حل هذه المشكلة في أقرب وقت ممكن.\n\n"
        "أشكركم على اهتمامكم ودعمكم."
    ),
    (
        "Dear Support Team,\n\n"
        "I am experiencing difficulties registering my number {nomor}. "
        "Every time I attempt to use it, I receive a message indicating the number is unavailable. "
        "This number is very important to me as I use it for daily communication "
        "and educational purposes. I sincerely hope the team can assist "
        "in resolving this issue as soon as possible so that I may use it again.\n\n"
        "Thank you for your attention and support."
    ),
    (
        "サポートチームへ、\n\n"
        "私の番号{nomor}の登録に問題が発生しています。"
        "使用しようとするたびに、番号が利用できないというメッセージが表示されます。"
        "この番号は日常の連絡や学習目的で非常に重要なものです。"
        "できるだけ早くこの問題を解決していただけるよう、ご支援をお願いいたします。\n\n"
        "ご対応いただけることに感謝申し上げます。"
    ),
]

_XMAILER_LIST = [
    "Microsoft Outlook 16.0.14931.20132",
    "Apple Mail 16.0 (3696.120.41.1.1)",
    "Mozilla Thunderbird 115.12.0",
    "The Bat! 10.5.1",
    "Postfix MTA 3.6.4",
]

_BTN_HOME = [[InlineKeyboardButton("🔙 Menu Utama", callback_data="home")]]


def _build_email(nomor: str, user_id: int) -> tuple[str, str]:
    subject  = random.choice(DEFAULT_SUBJECT_LIST)
    tmpl_idx = storage.get_template_index(user_id)
    body     = _TEMPLATES[tmpl_idx].format(nomor=nomor)
    return subject, body


def _verify_smtp(email: str, password: str) -> None:
    # Mengubah timeout dari 10 menjadi 30 detik
    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30)
    server.ehlo()
    server.starttls()
    server.login(email, password)
    server.quit()


def _send_email(sender_email: str, sender_pass: str, receiver: str, subject: str, body: str) -> None:
    msg        = MIMEMultipart("alternative")
    domain     = sender_email.split("@")[1]
    msg["From"]       = f"Customer Support <{sender_email}>"
    msg["To"]         = receiver
    msg["Subject"]    = subject
    msg["Date"]       = formatdate(localtime=True)
    msg["Message-ID"] = f"<{uuid.uuid4().hex}.{uuid.uuid4().hex[:10]}@{domain}>"
    msg["Reply-To"]   = sender_email
    msg["X-Mailer"]   = random.choice(_XMAILER_LIST)
    msg["MIME-Version"] = "1.0"
    msg["X-Priority"] = str(random.choice([1, 3]))
    msg.attach(MIMEText(body, "plain", "utf-8"))

    # Mengubah timeout dari 15 menjadi 30 detik
    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30)
    server.ehlo()
    server.starttls()
    server.login(sender_email, sender_pass)
    server.send_message(msg)
    server.quit()


async def start_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    user_id = update.effective_user.id

    ok, reason = storage.can_send(user_id)
    if not ok:
        await update.callback_query.edit_message_text(
            f"╔══════════════════╗\n"
            f"║  🚫  TIDAK BISA KIRIM  ║\n"
            f"╚══════════════════╝\n\n"
            f"{reason}",
            reply_markup=InlineKeyboardMarkup(_BTN_HOME),
            parse_mode="Markdown",
        )
        return ConversationHandler.END

    stored    = storage.get_email(user_id)
    count     = storage.get_send_count(user_id)
    max_daily = storage.get_max_daily(user_id)
    tier      = "💎 Premium" if storage.is_premium(user_id) else "🆓 Free"
    tmpl_idx  = storage.get_template_index(user_id)
    lang_label = LANG_LABELS[tmpl_idx]

    if stored:
        keyboard = [
            [InlineKeyboardButton("✅ Gunakan Email Ini", callback_data="use_stored_email")],
            [InlineKeyboardButton("🔄 Ganti Email",       callback_data="change_email")],
            [InlineKeyboardButton("🔙 Menu Utama",         callback_data="home")],
        ]
        await update.callback_query.edit_message_text(
            "╔══════════════════╗\n"
            "║   📤  KIRIM PESAN   ║\n"
            "╚══════════════════╝\n\n"
            f"📧 *Email:* `{stored['email']}`\n"
            f"🎨 *Template:* `#{tmpl_idx + 1}` — {lang_label}\n"
            f"📊 *Kuota:* `{count}/{max_daily}` | {tier}\n\n"
            "Gunakan email ini atau ganti?",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )
        return USE_STORED
    else:
        await update.callback_query.edit_message_text(
            "╔══════════════════╗\n"
            "║   📤  KIRIM PESAN   ║\n"
            "╚══════════════════╝\n\n"
            "Masukkan *email pengirim:*\n"
            "_(contoh: admin@walzhop.site)_",
            reply_markup=InlineKeyboardMarkup(_BTN_HOME),
            parse_mode="Markdown",
        )
        return ASK_EMAIL


async def handle_use_stored(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    user_id = update.effective_user.id

    if query.data == "use_stored_email":
        stored                         = storage.get_email(user_id)
        context.user_data["sender_email"] = stored["email"]
        context.user_data["sender_pass"]  = stored["password"]
        await query.edit_message_text(
            "📱 *Masukkan nomor yang bermasalah*\n\n"
            "Format: `+6281234567890`",
            reply_markup=InlineKeyboardMarkup(_BTN_HOME),
            parse_mode="Markdown",
        )
        return ASK_NOMOR

    elif query.data == "change_email":
        await query.edit_message_text(
            "📧 *Masukkan email pengirim baru:*",
            reply_markup=InlineKeyboardMarkup(_BTN_HOME),
            parse_mode="Markdown",
        )
        return ASK_EMAIL

    elif query.data == "home":
        return await cancel_callback(update, context)


async def get_sender_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["sender_email"] = update.message.text.strip()
    await update.message.reply_text(
        "🔑 *Masukkan password email:*",
        reply_markup=InlineKeyboardMarkup(_BTN_HOME),
        parse_mode="Markdown",
    )
    return ASK_PASS


async def get_sender_pass(update: Update, context: ContextTypes.DEFAULT_TYPE):
    password = update.message.text.strip()
    email    = context.user_data["sender_email"]
    user_id  = update.effective_user.id

    msg_ui = await update.message.reply_text("🔐 Memverifikasi kredensial...")

    try:
        await asyncio.to_thread(_verify_smtp, email, password)
    except smtplib.SMTPAuthenticationError:
        await msg_ui.edit_text(
            "❌ *Email atau password salah!*\n\nMasukkan password yang benar:",
            reply_markup=InlineKeyboardMarkup(_BTN_HOME),
            parse_mode="Markdown",
        )
        return ASK_PASS
    except Exception as e:
        await msg_ui.edit_text(
            f"❌ *Koneksi ke server mail gagal!*\n`{e}`\n\nMasukkan password lagi:",
            reply_markup=InlineKeyboardMarkup(_BTN_HOME),
            parse_mode="Markdown",
        )
        return ASK_PASS

    storage.set_email(user_id, email, password)
    context.user_data["sender_pass"] = password

    await msg_ui.edit_text(
        "✅ *Kredensial valid & tersimpan!*\n\n"
        "📱 Masukkan nomor yang bermasalah:\n"
        "Format: `+6281234567890`",
        reply_markup=InlineKeyboardMarkup(_BTN_HOME),
        parse_mode="Markdown",
    )
    return ASK_NOMOR


async def get_nomor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    nomor        = update.message.text.strip()
    sender_email = context.user_data["sender_email"]
    sender_pass  = context.user_data["sender_pass"]
    user_id      = update.effective_user.id

    ok, reason = storage.can_send(user_id)
    if not ok:
        await update.message.reply_text(
            f"╔══════════════════╗\n"
            f"║  🚫  TIDAK BISA KIRIM  ║\n"
            f"╚══════════════════╝\n\n"
            f"{reason}",
            reply_markup=InlineKeyboardMarkup(_BTN_HOME),
            parse_mode="Markdown",
        )
        return ConversationHandler.END

    subject, body = _build_email(nomor, user_id)
    tmpl_idx      = storage.get_template_index(user_id)
    lang_label    = LANG_LABELS[tmpl_idx]
    msg_ui        = await update.message.reply_text("🚀 Mengirim pesan ke Support...")

    try:
        await asyncio.to_thread(
            _send_email, sender_email, sender_pass, DEFAULT_RECEIVER, subject, body
        )
        storage.log_send(user_id)
        count     = storage.get_send_count(user_id)
        max_daily = storage.get_max_daily(user_id)
        tier      = "💎 Premium" if storage.is_premium(user_id) else "🆓 Free"

        keyboard = [
            [InlineKeyboardButton("📤 Kirim Lagi",   callback_data="send_email")],
            [InlineKeyboardButton("🔙 Menu Utama",   callback_data="home")],
        ]
        await msg_ui.edit_text(
            "╔══════════════════╗\n"
            "║  ✅  BERHASIL DIKIRIM  ║\n"
            "╚══════════════════╝\n\n"
            f"📧 *Ke:* `{DEFAULT_RECEIVER}`\n"
            f"📱 *Nomor:* `{nomor}`\n"
            f"🎨 *Template:* `#{tmpl_idx + 1}` — {lang_label}\n"
            f"📊 *Kuota:* `{count}/{max_daily}` | {tier}",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )
    except Exception as e:
        await msg_ui.edit_text(
            "╔══════════════════╗\n"
            "║  ❌  GAGAL MENGIRIM  ║\n"
            "╚══════════════════╝\n\n"
            f"`{e}`",
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
