import os, sys, platform, logging, shutil

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("nexus")

OWNER_ID        = "8062935882"
BOT_TOKEN       = "7673309476:AAEAg4kBjtBvCAKLAN3tBjNcuhJLYr7TdDg"
BOT_NAME        = "PREMIUM IVAS"
REQUIRED_GROUPS = []

URL_BASE    = "https://www.ivasms.com"
URL_LOGIN   = "https://www.ivasms.com/login"
URL_PORTAL  = "https://www.ivasms.com/portal"
URL_LIVE    = "https://www.ivasms.com/portal/live/test_sms"
URL_NUMBERS = "https://www.ivasms.com/portal/numbers"
URL_HUB     = "https://hub.orangecarrier.com"

BASE_DIR    = os.path.expanduser("~/nexus_data")
USERS_FILE  = os.path.join(BASE_DIR, "users.json")
TG_API      = f"https://api.telegram.org/bot{BOT_TOKEN}"

POLL_INTERVAL       = 0.05
RELOAD_INTERVAL     = 2
TOP_N               = 25
NOMOR_PER_REQUEST   = 50
INJECT_DELAY        = 0.3
MAX_FAIL            = 3
API_POLL_INTERVAL   = 4
AUTO_RANGE_INTERVAL = 7200
AUTO_RANGE_IDLE_TTL = 1800
AUTO_RANGE_QTY      = 100
CHROMIUM_PATH       = ""
CHROMEDRIVER_PATH   = ""

def detect_env():
    is_termux = "com.termux" in os.environ.get("PREFIX", "") or os.path.isdir("/data/data/com.termux")
    is_docker = os.path.isfile("/.dockerenv")
    is_linux  = sys.platform.startswith("linux")
    env = "termux" if is_termux else ("docker" if is_docker else ("vps" if is_linux else "other"))
    log.info(f"Env: {env} | Python {sys.version.split()[0]} | {platform.machine()}")
    return env

ENV = detect_env()
os.makedirs(BASE_DIR, exist_ok=True)

def find_chrome():
    if CHROMIUM_PATH and os.path.isfile(CHROMIUM_PATH) and os.access(CHROMIUM_PATH, os.X_OK):
        return CHROMIUM_PATH
    termux_paths = [
        "/data/data/com.termux/files/usr/bin/chromium-browser",
        "/data/data/com.termux/files/usr/bin/chromium",
    ]
    vps_paths = [
        "/usr/bin/google-chrome-stable", "/usr/bin/google-chrome",
        "/usr/bin/chromium-browser", "/usr/bin/chromium",
        "/usr/local/bin/chromium", "/usr/local/bin/chromium-browser",
        "/snap/bin/chromium", "/opt/google/chrome/google-chrome",
    ]
    paths = termux_paths + vps_paths if ENV == "termux" else vps_paths + termux_paths
    for p in paths:
        if p and os.path.isfile(p) and os.access(p, os.X_OK):
            return p
    for name in ["google-chrome-stable", "google-chrome", "chromium-browser", "chromium"]:
        p = shutil.which(name)
        if p:
            return p
    return None

def find_driver():
    if CHROMEDRIVER_PATH and os.path.isfile(CHROMEDRIVER_PATH) and os.access(CHROMEDRIVER_PATH, os.X_OK):
        return CHROMEDRIVER_PATH
    termux_paths = ["/data/data/com.termux/files/usr/bin/chromedriver"]
    vps_paths = [
        "/usr/bin/chromedriver", "/usr/local/bin/chromedriver",
        "/usr/lib/chromium-browser/chromedriver", "/usr/lib/chromium/chromedriver",
    ]
    paths = termux_paths + vps_paths if ENV == "termux" else vps_paths + termux_paths
    for p in paths:
        if p and os.path.isfile(p) and os.access(p, os.X_OK):
            return p
    return shutil.which("chromedriver")
