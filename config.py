import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
API_ENDPOINT   = os.getenv("API_ENDPOINT")
API_TOKEN      = os.getenv("API_TOKEN")
DOMAIN         = os.getenv("DOMAIN")
SMTP_SERVER    = os.getenv("SMTP_SERVER")
SMTP_PORT      = int(os.getenv("SMTP_PORT", 587))
IMAP_SERVER    = os.getenv("IMAP_SERVER")
IMAP_PORT      = int(os.getenv("IMAP_PORT", 993))
ADMIN_ID       = int(os.getenv("ADMIN_ID", 0))
PREMIUM_PRICE  = os.getenv("PREMIUM_PRICE", "Rp 25.000 / bulan")
PAYMENT_INFO   = os.getenv("PAYMENT_INFO", "Dana/GoPay: 0812-XXXX-XXXX (a/n Walzy)")
