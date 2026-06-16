import nest_asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ConversationHandler,
)
from config import TELEGRAM_TOKEN, ADMIN_ID, PREMIUM_PRICE, PAYMENT_INFO
from modules import creator, sender, receiver, storage

nest_asyncio.apply()

LANG_LABELS = ["🇮🇩 Indonesia", "🇧🇷 Português", "🇸🇦 العربية", "🇺🇸 English", "🇯🇵 日本語"]

_DIVIDER = "━━━━━━━━━━━━━━━━━━━━"


async def render_home(update: Update, context):
    user_id    = update.effective_user.id
    first_name = update.effective_user.first_name or "User"
    stored     = storage.get_email(user_id)
    count      = storage.get_send_count(user_id)
    premium    = storage.is_premium(user_id)
    max_daily  = storage.get_max_daily(user_id)
    tier_badge = "💎 PREMIUM" if premium else "🆓 FREE"

    if stored:
        tmpl_idx   = storage.get_template_index(user_id)
        lang_label = LANG_LABELS[tmpl_idx]

        bar_filled = round((count / max_daily) * 10) if max_daily > 0 else 0
        bar_empty  = 10 - bar_filled
        bar        = "█" * bar_filled + "░" * bar_empty

        status_line = (
            f"👤 *{first_name}* — {tier_badge}\n"
            f"{_DIVIDER}\n"
            f"📧 `{stored['email']}`\n"
            f"🎨 Template `#{tmpl_idx + 1}` — {lang_label}\n"
            f"📊 `[{bar}]` {count}/{max_daily}"
        )
        rows = [
            [InlineKeyboardButton("📤  Kirim Pesan Support",  callback_data="send_email")],
            [InlineKeyboardButton("📥  Cek Inbox",             callback_data="check_inbox")],
            [InlineKeyboardButton("🗑️  Hapus Akun Email",       callback_data="delete_email")],
        ]
        if not premium:
            rows.append([InlineKeyboardButton("💎  Upgrade Premium", callback_data="premium_info")])
        rows.append([InlineKeyboardButton("💻  Status Sistem",       callback_data="status")])
    else:
        status_line = (
            f"👤 *{first_name}* — {tier_badge}\n"
            f"{_DIVIDER}\n"
            "⚠️ Belum ada akun email terdaftar.\n\n"
            "Ketik perintah di bawah untuk mulai:\n"
            "`/create <username> <password>`"
        )
        rows = [
            [InlineKeyboardButton("📋  Cara Buat Email",    callback_data="cmd_create")],
        ]
        if not premium:
            rows.append([InlineKeyboardButton("💎  Upgrade Premium", callback_data="premium_info")])
        rows.append([InlineKeyboardButton("💻  Status Sistem",       callback_data="status")])

    text = (
        "╔══════════════════════╗\n"
        "║  ⚡  WALZHOP MAIL MGR  ⚡  ║\n"
        "╚══════════════════════╝\n\n"
        f"{status_line}"
    )

    reply_markup = InlineKeyboardMarkup(rows)

    if update.message:
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode="Markdown")
    elif update.callback_query:
        await update.callback_query.edit_message_text(
            text, reply_markup=reply_markup, parse_mode="Markdown"
        )


async def home(update: Update, context):
    await render_home(update, context)


async def button_router(update: Update, context):
    query   = update.callback_query
    await query.answer()
    user_id = update.effective_user.id

    if query.data == "home":
        await render_home(update, context)

    elif query.data == "cmd_create":
        await query.edit_message_text(
            "╔══════════════════╗\n"
            "║   🛠️  BUAT EMAIL   ║\n"
            "╚══════════════════╝\n\n"
            "Ketik perintah ini di chat:\n\n"
            "`/create <username> <password>`\n\n"
            "Contoh:\n"
            "`/create support admin123`\n\n"
            f"{_DIVIDER}\n"
            "📌 1 akun Telegram = 1 email\n"
            "📌 Template bahasa ditetapkan otomatis",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Menu Utama", callback_data="home")]]),
            parse_mode="Markdown",
        )

    elif query.data == "status":
        await query.edit_message_text(
            "╔══════════════════╗\n"
            "║   💻  STATUS SISTEM   ║\n"
            "╚══════════════════╝\n\n"
            "🟢 Bot aktif & berjalan\n"
            "🟢 SMTP (Port 587 STARTTLS)\n"
            "🟢 IMAP (Port 993 SSL)\n"
            f"{_DIVIDER}\n"
            f"🆓 Free  : *{storage.FREE_DAILY}x / hari*\n"
            f"💎 Premium: *{storage.PREMIUM_DAILY}x / hari*\n"
            f"⏱️ Cooldown: *{storage.COOLDOWN_SECONDS} detik*\n"
            f"{_DIVIDER}\n"
            "🌐 Template : *5 bahasa*\n"
            "🎲 Subject  : *Acak multilingual*\n"
            "🔀 Header   : *Random per kirim*\n"
            "💾 Storage  : *Persistent JSON*",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Menu Utama", callback_data="home")]]),
            parse_mode="Markdown",
        )

    elif query.data == "delete_email":
        stored = storage.get_email(user_id)
        if stored:
            keyboard = [
                [InlineKeyboardButton("✅ Ya, Hapus Akun", callback_data="confirm_delete")],
                [InlineKeyboardButton("❌ Batal",           callback_data="home")],
            ]
            await query.edit_message_text(
                "╔══════════════════╗\n"
                "║   🗑️  HAPUS AKUN   ║\n"
                "╚══════════════════╝\n\n"
                f"📧 `{stored['email']}`\n\n"
                "⚠️ Akun akan dihapus dari server\n"
                "CyberPanel dan bot secara permanen.\n\n"
                "Lanjutkan penghapusan?",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown",
            )
        else:
            await render_home(update, context)

    elif query.data == "confirm_delete":
        await creator.delete_email_account(update, context)

    elif query.data == "premium_info":
        keyboard = [
            [InlineKeyboardButton("👀 Preview Tampilan Premium", callback_data="premium_preview")],
            [InlineKeyboardButton("💳 Beli Premium Sekarang",    callback_data="premium_buy")],
            [InlineKeyboardButton("🔙 Menu Utama",               callback_data="home")],
        ]
        await query.edit_message_text(
            "╔══════════════════╗\n"
            "║   💎  UPGRADE PLAN   ║\n"
            "╚══════════════════╝\n\n"
            f"🆓 *FREE*\n"
            f"   ├ {storage.FREE_DAILY}x kirim per hari\n"
            f"   ├ 1 template otomatis\n"
            f"   └ Cooldown {storage.COOLDOWN_SECONDS} detik\n\n"
            f"💎 *PREMIUM*\n"
            f"   ├ {storage.PREMIUM_DAILY}x kirim per hari\n"
            f"   ├ Template sequential otomatis\n"
            f"   ├ Cooldown {storage.COOLDOWN_SECONDS} detik\n"
            f"   └ Prioritas support\n\n"
            f"{_DIVIDER}\n"
            f"💰 Harga: *{PREMIUM_PRICE}*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )

    elif query.data == "premium_preview":
        keyboard = [
            [InlineKeyboardButton("💳 Beli Premium Sekarang", callback_data="premium_buy")],
            [InlineKeyboardButton("🔙 Kembali",               callback_data="premium_info")],
        ]
        stored     = storage.get_email(user_id)
        email_line = f"`{stored['email']}`" if stored else "`support@walzhop.site`"
        bar        = "█" * 5 + "░" * 5
        await query.edit_message_text(
            "╔══════════════════════╗\n"
            "║  👀  PREVIEW — PREMIUM  ║\n"
            "╚══════════════════════╝\n\n"
            "─── *Simulasi Tampilan Premium* ───\n\n"
            "╔══════════════════════╗\n"
            "║  ⚡  WALZHOP MAIL MGR  ⚡  ║\n"
            "╚══════════════════════╝\n\n"
            f"👤 *Kamu* — 💎 PREMIUM\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📧 {email_line}\n"
            f"🎨 Template `#1` — 🇮🇩 Indonesia\n"
            f"📊 `[{bar}]` 0/{storage.PREMIUM_DAILY}\n\n"
            f"[ 📤  Kirim Pesan Support ]\n"
            f"[ 📥  Cek Inbox ]\n"
            f"[ 🗑️  Hapus Akun Email ]\n"
            f"[ 💻  Status Sistem ]\n\n"
            f"{_DIVIDER}\n"
            f"💰 Harga: *{PREMIUM_PRICE}*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )

    elif query.data == "premium_buy":
        keyboard = [
            [InlineKeyboardButton("✅ Saya Sudah Bayar", callback_data="premium_confirm_payment")],
            [InlineKeyboardButton("🔙 Kembali",          callback_data="premium_info")],
        ]
        await query.edit_message_text(
            "╔══════════════════╗\n"
            "║   💳  CARA BAYAR   ║\n"
            "╚══════════════════╝\n\n"
            f"💰 *Harga:* {PREMIUM_PRICE}\n\n"
            f"🏦 *Transfer ke:*\n`{PAYMENT_INFO}`\n\n"
            f"{_DIVIDER}\n"
            "1️⃣ Transfer sesuai nominal\n"
            "2️⃣ Screenshot bukti transfer\n"
            "3️⃣ Tekan *Saya Sudah Bayar*\n"
            "4️⃣ Kirim screenshot ke admin\n"
            "5️⃣ Tunggu konfirmasi (maks. 1x24 jam)\n\n"
            "⚠️ Pastikan transfer ke nomor yang benar.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )

    elif query.data == "premium_confirm_payment":
        username   = update.effective_user.username or "-"
        first_name = update.effective_user.first_name or "-"

        await query.edit_message_text(
            "╔══════════════════╗\n"
            "║  ⏳  MENUNGGU KONFIRMASI  ║\n"
            "╚══════════════════╝\n\n"
            "✅ Request premium sudah tercatat.\n"
            "⏱️ Aktivasi maks. *1x24 jam*.\n\n"
            "Kamu akan mendapat notifikasi\nsaat premium aktif.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Menu Utama", callback_data="home")]]),
            parse_mode="Markdown",
        )

        if ADMIN_ID:
            try:
                stored     = storage.get_email(user_id)
                email_info = stored["email"] if stored else "Belum punya email"
                await context.bot.send_message(
                    ADMIN_ID,
                    "╔══════════════════╗\n"
                    "║  🔔  REQUEST PREMIUM  ║\n"
                    "╚══════════════════╝\n\n"
                    f"👤 *Nama:* {first_name}\n"
                    f"🆔 *Username:* @{username}\n"
                    f"🔢 *User ID:* `{user_id}`\n"
                    f"📧 *Email:* `{email_info}`\n\n"
                    f"Untuk aktivasi: `/grant {user_id}`\n"
                    f"Untuk tolak: `/revoke {user_id}`",
                    parse_mode="Markdown",
                )
            except Exception:
                pass


async def grant_premium(update: Update, context):
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("Usage: `/grant <user_id>`", parse_mode="Markdown")
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ User ID tidak valid.")
        return

    storage.set_premium(target_id, True)
    await update.message.reply_text(
        f"✅ User `{target_id}` berhasil di-upgrade ke *Premium*.",
        parse_mode="Markdown",
    )

    try:
        await context.bot.send_message(
            target_id,
            "╔══════════════════╗\n"
            "║  🎉  PREMIUM AKTIF!   ║\n"
            "╚══════════════════╝\n\n"
            f"✅ Kuota kirim: *{storage.PREMIUM_DAILY}x per hari*\n"
            "✅ Semua fitur premium aktif\n\n"
            "Ketik /start untuk kembali ke menu.",
            parse_mode="Markdown",
        )
    except Exception:
        pass


async def revoke_premium(update: Update, context):
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("Usage: `/revoke <user_id>`", parse_mode="Markdown")
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ User ID tidak valid.")
        return

    storage.set_premium(target_id, False)
    await update.message.reply_text(
        f"✅ Premium User `{target_id}` telah dicabut.",
        parse_mode="Markdown",
    )


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    send_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(sender.start_send, pattern="^send_email$")],
        states={
            sender.USE_STORED: [
                CallbackQueryHandler(
                    sender.handle_use_stored,
                    pattern="^(use_stored_email|change_email|home)$",
                )
            ],
            sender.ASK_EMAIL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, sender.get_sender_email)
            ],
            sender.ASK_PASS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, sender.get_sender_pass)
            ],
            sender.ASK_NOMOR: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, sender.get_nomor)
            ],
        },
        fallbacks=[
            CommandHandler("cancel", sender.cancel),
            CallbackQueryHandler(sender.cancel_callback, pattern="^home$"),
        ],
    )

    recv_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(receiver.start_check, pattern="^check_inbox$")],
        states={
            receiver.USE_STORED: [
                CallbackQueryHandler(
                    receiver.handle_use_stored,
                    pattern="^(use_stored_inbox|change_inbox_email|home)$",
                )
            ],
            receiver.CHECK_EMAIL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receiver.get_check_email)
            ],
            receiver.CHECK_PASS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receiver.process_check)
            ],
        },
        fallbacks=[
            CommandHandler("cancel", receiver.cancel),
            CallbackQueryHandler(receiver.cancel_callback, pattern="^home$"),
        ],
    )

    app.add_handler(CommandHandler("start",  home))
    app.add_handler(CommandHandler("create", creator.create_email))
    app.add_handler(CommandHandler("grant",  grant_premium))
    app.add_handler(CommandHandler("revoke", revoke_premium))
    app.add_handler(send_conv)
    app.add_handler(recv_conv)
    app.add_handler(
        CallbackQueryHandler(
            button_router,
            pattern=(
                "^(home|cmd_create|status|delete_email|confirm_delete"
                "|premium_info|premium_preview|premium_buy|premium_confirm_payment)$"
            ),
        )
    )

    print("✅ GmailWalz v3 — Bot started.")
    app.run_polling()


if __name__ == "__main__":
    main()
