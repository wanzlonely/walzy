import os, re, time, glob, pickle, threading, json, requests, math, shutil
from collections import Counter
from datetime import datetime, date, timezone, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import WebDriverException, InvalidSessionIdException
import logging

BOT_TOKEN     = "7673309476:AAEAg4kBjtBvCAKLAN3tBjNcuhJLYr7TdDg"
OWNER_ID      = "8062935882"
BOT_NAME      = "◈ SCRIPT PREMIUM IVASMS"
TG_API        = f"https://api.telegram.org/bot{BOT_TOKEN}"

GROUP_LINK_1  = "https://t.me/+LINK_GRUP_1_KAMU"
GROUP_LINK_2  = "https://t.me/+LINK_GRUP_2_KAMU"
GROUP_TITLE_1 = "Channel"
GROUP_TITLE_2 = "Number"

BASE_DIR      = os.path.expanduser("~/ivas_data")
DATA_FILE     = os.path.join(BASE_DIR, "data.json")
DL_DIR        = os.path.join(BASE_DIR, "downloads")

URL_LOGIN     = "https://www.ivasms.com/login"
URL_PORTAL    = "https://www.ivasms.com/portal"
URL_LIVE      = "https://www.ivasms.com/portal/live/test_sms"
URL_NUMBERS   = "https://www.ivasms.com/portal/numbers"
URL_SMS_RX    = "https://www.ivasms.com/portal/sms/received"
URL_HUB       = "https://hub.orangecarrier.com"

LIVE_POLL         = 0.3
INJECT_WAIT       = 12
MAX_FAIL          = 3
NOMOR_PER_REQUEST = 50
INJECT_DELAY      = 0.1
TOP_N             = 15
AUTO_RANGE_QTY    = 100
SMS_POLL_INTERVAL = 45
MAX_GROUPS        = 5

os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(DL_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("ivas")

_tg       = requests.Session()
_tg.headers.update({"Content-Type": "application/json"})
_sess_lock = threading.Lock()
S          = {}

def server_today():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def server_yesterday():
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

def get_account(cid=None):
    try:
        with open(DATA_FILE) as f:
            return json.load(f)
    except Exception:
        return {}

def save_account(email, password, cid=None):
    with _sess_lock:
        d = get_account()
        d["email"]    = email
        d["password"] = password
        with open(DATA_FILE, "w") as f:
            json.dump(d, f, indent=2)

def get_groups():
    try:
        with open(DATA_FILE) as f:
            groups = json.load(f).get("forward_groups", [])
    except Exception:
        groups = []
    if not groups:
        defaults = []
        if GROUP_LINK_1 and not GROUP_LINK_1.endswith("_KAMU"):
            defaults.append({"id": "0", "title": GROUP_TITLE_1, "invite_link": GROUP_LINK_1})
        if GROUP_LINK_2 and not GROUP_LINK_2.endswith("_KAMU"):
            defaults.append({"id": "1", "title": GROUP_TITLE_2, "invite_link": GROUP_LINK_2})
        return defaults
    return groups

def add_group(chat_id, title="", invite_link=""):
    with _sess_lock:
        try:
            with open(DATA_FILE) as f:
                d = json.load(f)
        except Exception:
            d = {}
        groups  = d.get("forward_groups", [])
        chat_id = str(chat_id)
        for g in groups:
            if str(g["id"]) == chat_id:
                if title:       g["title"]       = title
                if invite_link: g["invite_link"] = invite_link
                d["forward_groups"] = groups
                with open(DATA_FILE, "w") as f:
                    json.dump(d, f, indent=2)
                return "exists"
        if len(groups) >= MAX_GROUPS:
            return "full"
        groups.append({"id": chat_id, "title": title or chat_id, "invite_link": invite_link})
        d["forward_groups"] = groups
        with open(DATA_FILE, "w") as f:
            json.dump(d, f, indent=2)
        return "ok"

def remove_group(chat_id):
    with _sess_lock:
        try:
            with open(DATA_FILE) as f:
                d = json.load(f)
        except Exception:
            return False
        groups  = d.get("forward_groups", [])
        chat_id = str(chat_id)
        before  = len(groups)
        d["forward_groups"] = [g for g in groups if str(g["id"]) != chat_id]
        if len(d["forward_groups"]) == before:
            return False
        with open(DATA_FILE, "w") as f:
            json.dump(d, f, indent=2)
        return True

def tg(ep, data, timeout=10):
    for _ in range(3):
        try:
            r = _tg.post(f"{TG_API}/{ep}", json=data, timeout=timeout)
            if r.ok:
                return r.json()
        except Exception:
            time.sleep(0.3)
    return None

def send(cid, text, markup=None):
    p = {"chat_id": str(cid), "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if markup: p["reply_markup"] = markup
    r = tg("sendMessage", p)
    return r["result"]["message_id"] if r and r.get("ok") else None

def edit(cid, mid, text, markup=None):
    p = {"chat_id": str(cid), "message_id": mid, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if markup is not None: p["reply_markup"] = markup
    r = tg("editMessageText", p)
    return bool(r and (r.get("ok") or "not modified" in str(r).lower()))

def delete_msg(cid, mid):
    threading.Thread(target=tg, args=("deleteMessage", {"chat_id": str(cid), "message_id": mid}), daemon=True).start()

def answer(cb_id, text="", alert=False):
    threading.Thread(target=tg, args=("answerCallbackQuery", {"callback_query_id": cb_id, "text": text, "show_alert": alert}), daemon=True).start()

def send_doc(cid, path, caption=""):
    for i in range(3):
        try:
            with open(path, "rb") as f:
                data = f.read()
            r = requests.post(
                f"{TG_API}/sendDocument",
                data={"chat_id": str(cid), "caption": caption[:1024], "parse_mode": "HTML"},
                files={"document": (os.path.basename(path), data, "text/plain")},
                timeout=120
            )
            if r.ok: return True
        except Exception:
            if i < 2: time.sleep(3)
    return False

def kb(rows):
    return {"inline_keyboard": [[{"text": l, "callback_data": d} for l, d in row] for row in rows]}

def div(label="", w=28):
    if not label: return "━" * w
    pad = w - len(label) - 2
    l   = pad // 2
    return "━" * l + f" {label} " + "━" * (pad - l)

def esc(t):
    return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def fmt(n):
    try:    return f"{int(n):,}".replace(",", ".")
    except: return str(n)

FLAG = {
    "AFGHANISTAN":"🇦🇫","ALBANIA":"🇦🇱","ALGERIA":"🇩🇿","ANDORRA":"🇦🇩","ANGOLA":"🇦🇴",
    "ANTIGUA AND BARBUDA":"🇦🇬","ARGENTINA":"🇦🇷","ARMENIA":"🇦🇲","AUSTRALIA":"🇦🇺",
    "AUSTRIA":"🇦🇹","AZERBAIJAN":"🇦🇿","BAHAMAS":"🇧🇸","BAHRAIN":"🇧🇭","BANGLADESH":"🇧🇩",
    "BARBADOS":"🇧🇧","BELARUS":"🇧🇾","BELGIUM":"🇧🇪","BELIZE":"🇧🇿","BENIN":"🇧🇯",
    "BHUTAN":"🇧🇹","BOLIVIA":"🇧🇴","BOSNIA AND HERZEGOVINA":"🇧🇦","BOTSWANA":"🇧🇼",
    "BRAZIL":"🇧🇷","BRUNEI":"🇧🇳","BULGARIA":"🇧🇬","BURKINA FASO":"🇧🇫","BURUNDI":"🇧🇮",
    "CABO VERDE":"🇨🇻","CAMBODIA":"🇰🇭","CAMEROON":"🇨🇲","CANADA":"🇨🇦","CENTRAL AFRICAN REPUBLIC":"🇨🇫",
    "CHAD":"🇹🇩","CHILE":"🇨🇱","CHINA":"🇨🇳","COLOMBIA":"🇨🇴","COMOROS":"🇰🇲",
    "CONGO (BRAZZAVILLE)":"🇨🇬","CONGO (KINSHASA)":"🇨🇩","COSTA RICA":"🇨🇷","CROATIA":"🇭🇷",
    "CUBA":"🇨🇺","CYPRUS":"🇨🇾","CZECHIA":"🇨🇿","DENMARK":"🇩🇰","DJIBOUTI":"🇩🇯",
    "DOMINICA":"🇩🇲","DOMINICAN REPUBLIC":"🇩🇴","ECUADOR":"🇪🇨","EGYPT":"🇪🇬",
    "EL SALVADOR":"🇸🇻","EQUATORIAL GUINEA":"🇬🇶","ERITREA":"🇪🇷","ESTONIA":"🇪🇪",
    "ESWATINI":"🇸🇿","ETHIOPIA":"🇪🇹","FIJI":"🇫🇯","FINLAND":"🇫🇮","FRANCE":"🇫🇷",
    "GABON":"🇬🇦","GAMBIA":"🇬🇲","GEORGIA":"🇬🇪","GERMANY":"🇩🇪","GHANA":"🇬🇭",
    "GREECE":"🇬🇷","GRENADA":"🇬🇩","GUATEMALA":"🇬🇹","GUINEA":"🇬🇬","GUINEA-BISSAU":"🇬🇼",
    "GUYANA":"🇬🇾","HAITI":"🇭🇹","HONDURAS":"🇭🇳","HONG KONG":"🇭🇰","HUNGARY":"🇭🇺",
    "ICELAND":"🇮🇸","INDIA":"🇮🇳","INDONESIA":"🇮🇩","IRAN":"🇮🇷","IRAQ":"🇮🇶",
    "IRELAND":"🇮🇪","ISRAEL":"🇮🇱","ITALY":"🇮🇹","IVORY COAST":"🇨🇮","JAMAICA":"🇯🇲",
    "JAPAN":"🇯🇵","JORDAN":"🇯🇴","KAZAKHSTAN":"🇰🇿","KENYA":"🇰🇪","KIRIBATI":"🇰🇮",
    "KUWAIT":"🇰🇼","KYRGYZSTAN":"🇰🇬","LAOS":"🇱🇦","LATVIA":"🇱🇻","LEBANON":"🇱🇧",
    "LESOTHO":"🇱🇸","LIBERIA":"🇱🇷","LIBYA":"🇱🇾","LIECHTENSTEIN":"🇱🇮","LITHUANIA":"🇱🇹",
    "LUXEMBOURG":"🇱🇺","MACEDONIA":"🇲🇰","MADAGASCAR":"🇲🇬","MALAWI":"🇲🇼","MALAYSIA":"🇲🇾",
    "MALDIVES":"🇲🇻","MALI":"🇲🇱","MALTA":"🇲🇹","MARSHALL ISLANDS":"🇲🇭","MAURITANIA":"🇲🇷",
    "MAURITIUS":"🇲🇺","MEXICO":"🇲🇽","MICRONESIA":"🇫🇲","MOLDOVA":"🇲🇩","MONACO":"🇲🇨",
    "MONGOLIA":"🇲🇳","MONTENEGRO":"🇲🇪","MOROCCO":"🇲🇦","MOZAMBIQUE":"🇲🇿","MYANMAR":"🇲🇲",
    "NAMIBIA":"🇳🇦","NAURU":"🇳🇷","NEPAL":"🇳🇵","NETHERLANDS":"🇳🇱","NEW ZEALAND":"🇳🇿",
    "NICARAGUA":"🇳🇮","NIGER":"🇳🇪","NIGERIA":"🇳🇬","NORTH KOREA":"🇰🇵","NORWAY":"🇳🇴",
    "OMAN":"🇴🇲","PAKISTAN":"🇵🇰","PALAU":"🇵🇼","PALESTINE":"🇵🇸","PANAMA":"🇵🇦",
    "PAPUA NEW GUINEA":"🇵🇬","PARAGUAY":"🇵🇾","PERU":"🇵🇪","PHILIPPINES":"🇵🇭","POLAND":"🇵🇱",
    "PORTUGAL":"🇵🇹","QATAR":"🇶🇦","ROMANIA":"🇷🇴","RUSSIA":"🇷🇺","RWANDA":"🇷🇼",
    "SAINT KITTS AND NEVIS":"🇰🇳","SAINT LUCIA":"🇱🇨","SAINT VINCENT":"🇻🇨","SAMOA":"🇼🇸",
    "SAN MARINO":"🇸🇲","SAO TOME AND PRINCIPE":"🇸🇹","SAUDI ARABIA":"🇸🇦","SENEGAL":"🇸🇳",
    "SERBIA":"🇷🇸","SEYCHELLES":"🇸🇨","SIERRA LEONE":"🇸🇱","SINGAPORE":"🇸🇬","SLOVAKIA":"🇸🇰",
    "SLOVENIA":"🇸🇮","SOLOMON ISLANDS":"🇸🇧","SOMALIA":"🇸🇴","SOUTH AFRICA":"🇿🇦",
    "SOUTH KOREA":"🇰🇷","SOUTH SUDAN":"🇸🇸","SPAIN":"🇪🇸","SRI LANKA":"🇱🇰","SUDAN":"🇸🇩",
    "SURINAME":"🇸🇷","SWEDEN":"🇸🇪","SWITZERLAND":"🇨🇭","SYRIA":"🇸🇾","TAIWAN":"🇹🇼",
    "TAJIKISTAN":"🇹🇯","TANZANIA":"🇹🇿","THAILAND":"🇹🇭","TIMOR-LESTE":"🇹🇱","TOGO":"🇹🇬",
    "TONGA":"🇹🇴","TRINIDAD AND TOBAGO":"🇹🇹","TUNISIA":"🇹🇳","TURKEY":"🇹🇷",
    "TURKMENISTAN":"🇹🇲","TUVALU":"🇹🇻","UGANDA":"🇺🇬","UKRAINE":"🇺🇦","UAE":"🇦🇪",
    "UK":"🇬🇧","USA":"🇺🇸","URUGUAY":"🇺🇾","UZBEKISTAN":"🇺🇿","VANUATU":"🇻🇺",
    "VATICAN CITY":"🇻🇦","VENEZUELA":"🇻🇪","VIETNAM":"🇻🇳","YEMEN":"🇾🇪","ZAMBIA":"🇿🇲","ZIMBABWE":"🇿🇼"
}

def flag(name):
    n = name.upper().strip()
    for k, v in FLAG.items():
        if k in n: return v
    return "🌍"

def find_chrome():
    for p in [
        "/data/data/com.termux/files/usr/bin/chromium-browser",
        "/usr/bin/chromium-browser", "/usr/bin/google-chrome-stable", "/usr/local/bin/chromium"
    ]:
        if os.path.isfile(p) and os.access(p, os.X_OK): return p
    for n in ["chromium-browser", "chromium", "google-chrome-stable", "google-chrome"]:
        p = shutil.which(n)
        if p: return p
    return None

def find_driver():
    for p in [
        "/data/data/com.termux/files/usr/bin/chromedriver",
        "/usr/bin/chromedriver", "/usr/local/bin/chromedriver", "/usr/lib/chromium/chromedriver"
    ]:
        if os.path.isfile(p) and os.access(p, os.X_OK): return p
    return shutil.which("chromedriver")

def make_driver():
    chrome = find_chrome()
    if not chrome: raise RuntimeError("Chromium tidak ditemukan.")
    prof = os.path.join(BASE_DIR, "chrome_profile")
    os.makedirs(prof, exist_ok=True)
    for lf in ["SingletonLock", "SingletonSocket", "SingletonCookie"]:
        try: os.remove(os.path.join(prof, lf))
        except: pass

    opt = Options()
    opt.binary_location = chrome
    for arg in [
        "--headless", "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
        "--no-zygote", "--disable-setuid-sandbox", "--disable-seccomp-filter-sandbox",
        "--window-size=1280,800", f"--user-data-dir={prof}",
        "--disable-extensions", "--disable-notifications", "--mute-audio",
        "--ignore-certificate-errors", "--disable-blink-features=AutomationControlled",
        "--disable-background-networking", "--disable-default-apps", "--disable-sync",
        "--disable-software-rasterizer", "--js-flags=--max-old-space-size=256",
        "--disable-features=VizDisplayCompositor,Translate,AudioServiceOutOfProcess",
        "--blink-settings=imagesEnabled=false",
    ]:
        opt.add_argument(arg)

    opt.add_experimental_option("prefs", {
        "download.default_directory":           DL_DIR,
        "download.prompt_for_download":         False,
        "download.directory_upgrade":           True,
        "safebrowsing.enabled":                 False,
        "profile.managed_default_content_settings.images": 2,
    })
    opt.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    opt.add_experimental_option("useAutomationExtension", False)

    drv_path = find_driver()
    kwargs   = {"service": Service(drv_path)} if drv_path else {}
    for attempt in range(3):
        try:
            drv = webdriver.Chrome(**kwargs, options=opt)
            try: drv.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": DL_DIR})
            except: pass
            drv.set_page_load_timeout(25)
            drv.set_script_timeout(15)
            return drv
        except Exception:
            if attempt < 2: time.sleep(2)
    raise RuntimeError("Browser gagal start.")

def do_login(drv, email, password):
    drv.get(URL_LOGIN)
    time.sleep(1.5)
    for _ in range(5):
        body = drv.page_source.lower()
        if "checking your browser" in body or "just a moment" in body or "cloudflare" in body:
            time.sleep(2)
        else:
            break

    e_field = None
    for sel in ["input[type='email']", "input[name='email']", "#email"]:
        try:
            f = drv.find_element(By.CSS_SELECTOR, sel)
            if f.is_displayed():
                e_field = f
                break
        except: pass
    if not e_field: raise RuntimeError("Form email tidak ditemukan")

    p_field = drv.find_element(By.CSS_SELECTOR, "input[type='password']")
    e_field.clear(); e_field.send_keys(email); time.sleep(0.1)
    p_field.clear(); p_field.send_keys(password); time.sleep(0.1)
    p_field.send_keys(Keys.RETURN)
    time.sleep(3)

    if "login" not in drv.current_url: return True
    body = drv.execute_script("return document.body.innerText;").lower()
    if "captcha" in body or "robot" in body:
        raise RuntimeError("Terblokir captcha — tunggu beberapa menit")
    raise RuntimeError("Email/Password salah")

def try_cookie_login(drv):
    cf = os.path.join(BASE_DIR, "cookies.pkl")
    if not os.path.exists(cf): return False
    try:
        drv.get("https://www.ivasms.com")
        time.sleep(0.8)
        with open(cf, "rb") as f: cookies = pickle.load(f)
        for c in cookies:
            try: drv.add_cookie(c)
            except: pass
        drv.get(URL_PORTAL)
        time.sleep(1.5)
        if "login" not in drv.current_url: return True
        os.remove(cf)
        return False
    except: return False

def save_cookies(drv):
    cf = os.path.join(BASE_DIR, "cookies.pkl")
    try:
        with open(cf, "wb") as f: pickle.dump(drv.get_cookies(), f)
    except: pass

def init_hub(drv, email):
    drv.get(URL_PORTAL)
    time.sleep(1)
    hub_url = None
    for iframe in drv.find_elements(By.TAG_NAME, "iframe"):
        src = iframe.get_attribute("src") or ""
        if "hub.orangecarrier" in src:
            hub_url = src
            break
    drv.get(hub_url or f"{URL_HUB}?system=ivas")
    for _ in range(30):
        time.sleep(0.2)
        try:
            if drv.execute_script("return typeof socket!=='undefined'&&socket.connected;"): break
        except: pass

def hub_info(drv):
    try:
        return drv.execute_script(
            "return{email:(typeof currentUserInfo!=='undefined'&&currentUserInfo.email)||'',"
            "system:'ivas',type:'internal'};"
        )
    except:
        return {"email": "", "system": "ivas", "type": "internal"}

def _parse_country_and_id(col0):
    lines = [l.strip() for l in col0.split("\n") if l.strip()]
    if not lines: return "", "", ""
    full_range = re.sub(r'^[>\v+\-]\s*', '', lines[0].strip().upper()).strip()
    country    = re.sub(r'[^A-Z\s]', '', full_range).strip()
    nomor      = ""
    for ln in lines[1:]:
        sv = re.sub(r'[^0-9]', '', ln)
        if len(sv) >= 8:
            nomor = sv
            break
    return country, full_range, nomor

def scrape_live(drv, seen_uids, us):
    now = time.time()
    try:
        if URL_LIVE not in drv.current_url:
            drv.get(URL_LIVE)
            time.sleep(1.5)
            us["last_reload"] = now
        elif now - us.get("last_reload", 0) >= 300.0:
            drv.refresh()
            us["last_reload"] = now
            time.sleep(1.5)
    except: pass

    today_str = server_today()
    today     = datetime.strptime(today_str, "%Y-%m-%d").date()
    if us.get("tanggal") != today:
        with us.get("data_lock", threading.Lock()):
            us["traffic_counter"] = Counter()
            us["best_ranges"]     = {}
            us["seen"]            = set()
            us["tanggal"]         = today
        seen_uids.clear()

    js_script = """
    if(typeof window.ivas_seen === 'undefined') {
        window.ivas_seen = new Set();
        window.ivas_buf = [];
        setInterval(function(){
            var trs = document.querySelectorAll('table tbody tr');
            for(var i=0; i<trs.length; i++){
                var c0 = trs[i].cells[0] ? trs[i].cells[0].innerText.trim() : '';
                var c1 = trs[i].cells[1] ? trs[i].cells[1].innerText.trim() : '';
                var c2 = trs[i].cells[2] ? trs[i].cells[2].innerText.trim() : '';
                if(!c0) continue;
                var hash = c0 + '|' + c1 + '|' + c2;
                if(!window.ivas_seen.has(hash)){
                    window.ivas_seen.add(hash);
                    window.ivas_buf.push([c0, c1, c2]);
                }
            }
        }, 100);
    }
    var res = window.ivas_buf;
    window.ivas_buf = [];
    return res;
    """
    rows = drv.execute_script(js_script) or []

    results      = []
    dl           = us.get("data_lock", threading.Lock())
    new_wa_found = False

    for cols in rows:
        col0, app, msg = cols[0], cols[1], cols[2]
        country, full_range, nomor = _parse_country_and_id(col0)
        if not country: continue

        uid = f"{full_range}|{nomor[-6:] if nomor else '?'}|{msg[:15]}"
        if uid in seen_uids: continue
        seen_uids.add(uid)

        is_wa = bool(re.search(r'(whatsapp|wa\.me|<#>|\bwa\b)', app + " " + msg, re.IGNORECASE))
        if not is_wa: continue

        with dl:
            us["traffic_counter"][country] += 1
            if "best_ranges" not in us: us["best_ranges"] = {}
            us["best_ranges"][country] = full_range

        results.append(uid)
        new_wa_found = True

    if new_wa_found: us["live_otp_active"] = True
    return results

def do_inject_hub(drv, range_name, qty, callback, email=""):
    try: drv.execute_script("if(typeof socket==='undefined' || !socket.connected) location.reload();")
    except: pass
    info    = hub_info(drv)
    em, sy, ct = info["email"] or email, info["system"], info["type"]
    if not drv.execute_script("return typeof socket!=='undefined'&&socket.connected;"):
        raise RuntimeError("Hub socket tidak terhubung")
    total_r     = max(1, math.ceil(qty / NOMOR_PER_REQUEST))
    ok = fail = done = fail_streak = 0

    for i in range(total_r):
        if S.get("stop") and S["stop"].is_set(): break
        mb = drv.execute_script("return document.querySelectorAll('#messages .message').length;") or 0
        r1 = drv.execute_script(
            f"try{{socket.emit('menu_selection',{{selection:'add_numbers',email:'{em}',system:'{sy}',type:'{ct}'}});return 'ok';}}catch(e){{return 'err:'+e.message;}}"
        )
        if r1 != "ok":
            fail += 1; fail_streak += 1
            if fail_streak >= MAX_FAIL: break
            time.sleep(0.8); continue

        time.sleep(0.3)
        r2 = drv.execute_script(
            f"try{{socket.emit('form_submission',{{formType:'add_numbers',formData:{{termination_string:'{range_name}'}},email:'{em}',system:'{sy}',type:'{ct}'}});return 'ok';}}catch(e){{return 'err:'+e.message;}}"
        )
        if r2 != "ok":
            fail += 1; fail_streak += 1
            if fail_streak >= MAX_FAIL: break
            time.sleep(0.8); continue

        deadline = time.time() + INJECT_WAIT
        ma       = mb
        while time.time() < deadline:
            time.sleep(0.2)
            try:
                ma = drv.execute_script("return document.querySelectorAll('#messages .message').length;") or mb
                if ma > mb: break
            except: pass

        if ma > mb:
            ok += 1; done += NOMOR_PER_REQUEST; fail_streak = 0
        else:
            fail += 1; fail_streak += 1
            if fail_streak >= MAX_FAIL: break

        callback(int((i + 1) / total_r * 100), ok, fail, done)
        time.sleep(INJECT_DELAY)
    return ok, fail, done

def check_auto_range(drv):
    us = S
    with us.get("data_lock", threading.Lock()):
        counter     = Counter(us.get("traffic_counter", {}))
        best_ranges = dict(us.get("best_ranges", {}))
    if not counter: return

    now        = time.time()
    last_notif = us.setdefault("last_notif_top", {})
    top4       = counter.most_common(4)
    new_highs  = [(c, cnt) for c, cnt in top4 if now - last_notif.get(c, 0) >= 1800]
    if not new_highs: return

    total_wa = sum(counter.values())
    lines    = [f"<b>🔥 HIGH TRAFFIC ALERT</b>\n{div()}\n\n  📱 Total WA OTP: <b>{fmt(total_wa)}</b>\n\n{div('TOP TRAFFIC SEKARANG')}"]
    btn_rows = []

    for i, (country, cnt) in enumerate(top4, 1):
        medal      = "🥇" if i == 1 else ("🥈" if i == 2 else ("🥉" if i == 3 else "🏅"))
        full_range = best_ranges.get(country, country)
        lines.append(f"  {medal} {flag(country)} <b>{esc(country)}</b> — <b>{fmt(cnt)}</b> SMS")
        btn_rows.append([(f"📤 {full_range}", f"inj_custom:{full_range}")])
        last_notif[country] = now

    lines += ["\n  <i>Pilih range di bawah untuk inject:</i>"]
    btn_rows.append([("◀ Kembali", "m:home")])
    send(OWNER_ID, "\n".join(lines), kb(btn_rows))

def scrape_numbers_page(drv):
    drv.get(URL_NUMBERS)
    time.sleep(1.5)
    try:
        sel_el = drv.find_element(By.CSS_SELECTOR, "select[name*='DataTables_Table'],select[name*='length']")
        for opt in sel_el.find_elements(By.TAG_NAME, "option"):
            if (opt.get_attribute("value") or "") == "-1":
                drv.execute_script("arguments[0].value='-1'; arguments[0].dispatchEvent(new Event('change',{bubbles:true}));", sel_el)
                time.sleep(1.5)
                break
    except: pass
    numbers = []
    while True:
        rows = drv.execute_script("""
            var o=[];
            document.querySelectorAll('table tbody tr').forEach(function(r){
              var td=r.querySelectorAll('td');
              if(!td.length)return;
              for(var i=0;i<td.length;i++){
                var v=td[i].innerText.trim().split('.')[0].replace(/[^0-9]/g,'');
                if(v.length>=8){o.push(v);break;}
              }
            });
            return o;
        """) or []
        if not rows: break
        numbers.extend(rows)
        try:
            nxt = drv.find_element(By.CSS_SELECTOR, "a.paginate_button.next:not(.disabled),li.next:not(.disabled) a")
            if nxt.is_displayed():
                drv.execute_script("arguments[0].click();", nxt)
                time.sleep(0.8)
            else: break
        except: break
    return list(dict.fromkeys(numbers))

def do_export_excel(drv):
    drv.get(URL_NUMBERS)
    time.sleep(1.5)
    try: drv.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": DL_DIR})
    except: pass
    for f in glob.glob(os.path.join(DL_DIR, "*.xls*")):
        try: os.remove(f)
        except: pass
    for by, sel in [
        (By.XPATH, "//a[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export number excel')]"),
        (By.XPATH, "//button[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export number excel')]"),
        (By.CSS_SELECTOR, "a.btn-primary[href*='export']")
    ]:
        try:
            btn = WebDriverWait(drv, 5).until(EC.element_to_be_clickable((by, sel)))
            drv.execute_script("arguments[0].scrollIntoView(true);arguments[0].click();", btn)
            break
        except: pass

    deadline = time.time() + 40
    while time.time() < deadline:
        time.sleep(0.5)
        files = [f for f in glob.glob(os.path.join(DL_DIR, "*.xls*")) if not f.endswith((".crdownload", ".part", ".tmp"))]
        if files:
            xl = max(files, key=os.path.getmtime)
            if os.path.getsize(xl) > 0: return xl
    return None

def parse_xlsx(xl_path):
    try:
        import openpyxl
        nums = []
        wb   = openpyxl.load_workbook(xl_path, read_only=True, data_only=True)
        for ws in wb.worksheets:
            for row in ws.iter_rows(values_only=True):
                for cell in row:
                    if cell is None: continue
                    sv = str(cell).strip().split(".")[0].replace("+", "").replace(" ", "")
                    if sv.isdigit() and len(sv) >= 8: nums.append(sv)
        return list(dict.fromkeys(nums))
    except:
        return []

def do_bulk_return(drv):
    drv.get(URL_NUMBERS)
    time.sleep(2)
    try: WebDriverWait(drv, 8).until(lambda d: d.execute_script("return document.readyState") == "complete")
    except: pass
    clicked = False
    for sel in [
        "//button[contains(normalize-space(.),'Bulk return all numbers')]",
        "//a[contains(normalize-space(.),'Bulk return all numbers')]",
        "//button[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'bulk return all')]",
        "//a[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'bulk return all')]"
    ]:
        try:
            btn = WebDriverWait(drv, 5).until(EC.element_to_be_clickable((By.XPATH, sel)))
            drv.execute_script("arguments[0].scrollIntoView(true);", btn)
            time.sleep(0.2)
            drv.execute_script("arguments[0].click();", btn)
            clicked = True
            time.sleep(1.2)
            break
        except: pass

    if not clicked:
        result = drv.execute_script(
            "var btns=Array.from(document.querySelectorAll('button,a.btn'));"
            "for(var i=0;i<btns.length;i++){"
            "  if(btns[i].innerText.trim().toLowerCase().includes('bulk return all')){"
            "    btns[i].click();return btns[i].innerText.trim();"
            "  }}"
            "return null;"
        )
        if result:
            clicked = True
            time.sleep(1.2)

    if not clicked: return False

    try:
        WebDriverWait(drv, 3).until(EC.alert_is_present())
        drv.switch_to.alert.accept()
        time.sleep(1)
    except: pass

    for sel in [
        "button.swal-button--confirm", "button.swal2-confirm",
        ".swal2-popup button.swal2-confirm", "button.confirm",
        ".modal-footer button.btn-danger", ".modal-footer button.btn-primary",
        "//button[contains(normalize-space(.),'OK')]",
        "//button[contains(normalize-space(.),'Yes')]",
        "//button[contains(normalize-space(.),'Confirm')]"
    ]:
        try:
            el = WebDriverWait(drv, 3).until(EC.element_to_be_clickable(
                (By.XPATH if sel.startswith("//") else By.CSS_SELECTOR, sel)
            ))
            if el.is_displayed():
                drv.execute_script("arguments[0].click();", el)
                time.sleep(1.5)
                break
        except: pass

    for _ in range(15):
        time.sleep(0.8)
        try:
            pt = drv.execute_script("return document.body.innerText.toLowerCase();")
            if any(x in pt for x in ["no data", "no entries", "showing 0", "0 entries", "success", "returned"]):
                return True
        except: pass

    try:
        if drv.execute_script("return document.querySelectorAll('table tbody tr').length;") == 0: return True
    except: pass
    return False

def _strip_tags(html_fragment):
    return re.sub(r'<[^>]+>', '', html_fragment).strip()

def _decode_response(r):
    enc       = r.headers.get("Content-Encoding", "").lower()
    raw_bytes = r.content
    if "br" in enc:
        try:
            import brotli
            raw_bytes = brotli.decompress(raw_bytes)
        except Exception:
            raw_bytes = r.content
    elif "gzip" in enc:
        import gzip as _gzip
        try:
            raw_bytes = _gzip.decompress(raw_bytes)
        except Exception:
            raw_bytes = r.content
    try:    return raw_bytes.decode("utf-8"), enc
    except: return r.text, enc

def _save_debug_html(html_text, label="getsms"):
    try:
        with open(os.path.join(BASE_DIR, f"debug_{label}.html"), "w", encoding="utf-8") as f:
            f.write(html_text)
    except: pass

def _make_csrf_session(drv):
    import urllib.parse as _up
    try:
        if "ivasms.com" not in drv.current_url:
            drv.get(URL_SMS_RX)
            time.sleep(1.5)
    except: pass

    raw_cookies = drv.get_cookies()
    cookies     = {c["name"]: c["value"] for c in raw_cookies}
    ua          = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"

    sess = requests.Session()
    sess.headers.update({
        "User-Agent":               ua,
        "Accept":                   "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":          "en-US,en;q=0.9",
        "Accept-Encoding":          "gzip, deflate",
        "Connection":               "keep-alive",
        "Upgrade-Insecure-Requests":"1",
    })
    for name, value in cookies.items():
        sess.cookies.set(name, value, domain="www.ivasms.com")

    csrf_token = ""
    try:
        r = sess.get(URL_SMS_RX, timeout=12)
        if r.status_code == 200:
            html = r.text
            m    = re.search(r'<input[^>]+name=["\']_token["\'][^>]+value=["\']([^"\']+)["\']', html)
            if not m:
                m = re.search(r'<input[^>]+value=["\']([^"\']+)["\'][^>]+name=["\']_token["\']', html)
            if m:
                csrf_token = m.group(1).strip()
            else:
                m2 = re.search(r'<meta[^>]+name=["\']csrf-token["\'][^>]+content=["\']([^"\']+)["\']', html)
                if m2: csrf_token = m2.group(1).strip()
    except: pass

    if not csrf_token:
        try:
            csrf_token = drv.execute_script(
                "var el=document.querySelector('input[name=\"_token\"]');return el?el.value:'';"
            ) or ""
        except: pass

    sess.headers.update({
        "Accept":           "text/html, */*; q=0.01",
        "Origin":           "https://www.ivasms.com",
        "Referer":          URL_SMS_RX,
        "X-Requested-With": "XMLHttpRequest",
    })
    return sess, csrf_token, _up.quote(csrf_token, safe=""), "; ".join(f"{k}={v}" for k, v in cookies.items())

def _parse_getsms_html(html_text):
    summary = []
    stats   = {"total": "0", "paid": "0", "unpaid": "0", "revenue": "─"}

    for m in re.finditer(r'\$\(["\'](#\w+)["\']\)\.html\(["\'](.*?)["\'\)]\)', html_text):
        k, v = m.group(1).lstrip("#"), m.group(2)
        if   k == "CountSMS":   stats["total"]   = v
        elif k == "PaidSMS":    stats["paid"]    = v
        elif k == "UnpaidSMS":  stats["unpaid"]  = v
        elif k == "RevenueSMS": stats["revenue"] = v

    if stats["total"] == "0":
        for tag_id, key in [("CountSMS","total"),("PaidSMS","paid"),("UnpaidSMS","unpaid"),("RevenueSMS","revenue")]:
            m = re.search(r'id=["\']' + tag_id + r'["\'][^>]*>(.*?)</\w+>', html_text, re.S)
            if m:
                v = _strip_tags(m.group(1)).strip()
                if v: stats[key] = v

    item_blocks = re.findall(
        r'<div[^>]+class=["\'][^"\']*\bitem\b[^"\']*["\'][^>]*>(.*?)</div>\s*(?:</div>|<div)',
        html_text, re.S
    )
    if not item_blocks:
        item_blocks = re.findall(
            r'<div[^>]+class=["\'][^"\']*\bitem\b[^"\']*["\'][^>]*>(.*?)(?=<div[^>]+class=["\'][^"\']*\bitem\b|$)',
            html_text, re.S
        )

    for block in item_blocks:
        m_rname = re.search(r'class=["\'][^"\']*col-sm-4[^"\']*["\'][^>]*>(.*?)</div>', block, re.S)
        if not m_rname: continue
        rname = _strip_tags(m_rname.group(1)).strip()
        if not rname or not re.search(r'[A-Za-z]', rname): continue

        rng_id = ""
        m_oc   = re.search(r"toggleRange\s*\(\s*'([^']*)'\s*,\s*'([^']*)'", block)
        if m_oc:  rng_id = m_oc.group(1)
        else:
            m_oc2 = re.search(r"toggleRange\s*\(\s*'([^']*)'", block)
            if m_oc2: rng_id = m_oc2.group(1)
        if not rng_id:
            rng_id = re.sub(r"[^A-Z0-9_]", "_", rname.upper().strip())

        all_p  = re.findall(r'<p[^>]*>\s*([^<]{1,30}?)\s*</p>', block, re.S)
        all_p  = [_strip_tags(v).strip() for v in all_p if _strip_tags(v).strip()]
        m_rev  = re.search(r'currency_cdr[^>]*>(.*?)</span>', block, re.S)
        rev_val = _strip_tags(m_rev.group(1)).strip() if m_rev else (all_p[3] if len(all_p) > 3 else "─")

        summary.append({
            "range":    rname,
            "range_id": rng_id,
            "count":    all_p[0] if len(all_p) > 0 else "0",
            "paid":     all_p[1] if len(all_p) > 1 else "0",
            "unpaid":   all_p[2] if len(all_p) > 2 else "0",
            "revenue":  rev_val,
        })

    if summary: return {"summary": summary, "stats": stats}

    for block in re.split(r"(?=toggleRange\s*\()", html_text):
        _m2 = re.search(r"""toggleRange\s*\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)""", block)
        _m1 = re.search(r"""toggleRange\s*\(\s*['"]([^'"]+)['"]\s*\)""", block)
        if _m2:   rname, rng_id = _m2.group(1).strip(), _m2.group(2).strip()
        elif _m1: rname, rng_id = _m1.group(1).strip(), re.sub(r"[^A-Z0-9_]", "_", _m1.group(1).upper().strip())
        else: continue
        if not rname: continue

        def _gcv(cls, blk=block):
            for cpat in [
                r'class=["\'\']*[^"\'\' ]*\b' + cls + r'\b[^"\'\' ]*["\'\']*[^>]*>(.*?)</div>',
                r'\b' + cls + r'["\'\']*[^>]*>(.*?)</div>',
            ]:
                _mc = re.search(cpat, blk, re.S)
                if _mc: return _strip_tags(_mc.group(1)).strip()
            return "0"

        summary.append({
            "range":    rname,
            "range_id": rng_id,
            "count":    _gcv("v-count"),
            "paid":     _gcv("v-paid"),
            "unpaid":   _gcv("v-unpaid"),
            "revenue":  _gcv("v-rev") or "─",
        })

    if not summary:
        for block in re.findall(r'<div[^>]+class=["\'][^"\']*\binner\b[^"\']*["\'][^>]*>(.*?)</div>\s*</div>', html_text, re.S):
            m_rn = re.search(r'class=["\'][^"\']*rname[^"\']*["\'][^>]*>(.*?)</(?:span|div)>', block, re.S)
            if not m_rn:
                m_rn = re.search(r'class=["\'][^"\']*c-name[^"\']*["\'][^>]*>(.*?)</div>', block, re.S)
            if not m_rn: continue
            rname = _strip_tags(m_rn.group(1)).strip()
            if not rname: continue
            vals  = [_strip_tags(v).strip() for v in re.findall(r'class=["\'][^"\']*c-val[^"\']*["\'][^>]*>(.*?)</div>', block, re.S)]
            summary.append({
                "range":    rname,
                "range_id": re.sub(r"[^A-Z0-9_]", "_", rname.upper().strip()),
                "count":   vals[0] if len(vals) > 0 else "0",
                "paid":    vals[1] if len(vals) > 1 else "0",
                "unpaid":  vals[2] if len(vals) > 2 else "0",
                "revenue": vals[3] if len(vals) > 3 else "─",
            })

    if not summary:
        for m_row in re.finditer(r'<tr[^>]*>(.*?)</tr>', html_text, re.S):
            tds = re.findall(r'<td[^>]*>(.*?)</td>', m_row.group(1), re.S)
            if len(tds) < 4: continue
            rname = re.sub(r'^[>+\-\s]+', '', _strip_tags(tds[0])).strip()
            if rname and re.search(r'[A-Za-z]', rname) and "range" not in rname.lower():
                summary.append({
                    "range":    rname,
                    "range_id": re.sub(r"[^A-Z0-9_]", "_", rname.upper().strip()),
                    "count":   _strip_tags(tds[1]),
                    "paid":    _strip_tags(tds[2]),
                    "unpaid":  _strip_tags(tds[3]),
                    "revenue": _strip_tags(tds[4]) if len(tds) > 4 else "─",
                })

    return {"summary": summary, "stats": stats}

def _parse_via_dom_injection(drv, html_text, start_date, end_date):
    try:
        escaped = html_text.replace("\\", "\\\\").replace("`", "\\`").replace("$", "\\$")
        drv.execute_script(f"""
            var container = document.getElementById('__ivas_debug_container__');
            if (!container) {{
                container = document.createElement('div');
                container.id = '__ivas_debug_container__';
                container.style.display = 'none';
                document.body.appendChild(container);
            }}
            container.innerHTML = `{escaped}`;
        """)
        time.sleep(0.2)
        summary = drv.execute_script("""
            var container = document.getElementById('__ivas_debug_container__');
            if (!container) return [];
            var rows = [];
            container.querySelectorAll('div.rng').forEach(function(d) {
                var oc = d.getAttribute('onclick') || '';
                var m  = oc.match(/toggleRange\\('([^']+)'/);
                var rname = m ? m[1] : '';
                var rspan = d.querySelector('.rname, .c-name span');
                if (rspan) rname = rspan.innerText.trim();
                if (!rname) return;
                var count  = d.querySelector('.v-count');
                var paid   = d.querySelector('.v-paid');
                var unpaid = d.querySelector('.v-unpaid');
                var rev    = d.querySelector('.v-rev');
                rows.push({ range: rname, count: count ? count.innerText.trim() : '0',
                    paid: paid ? paid.innerText.trim() : '0',
                    unpaid: unpaid ? unpaid.innerText.trim() : '0',
                    revenue: rev ? rev.innerText.trim() : '─' });
            });
            if (!rows.length) {
                container.querySelectorAll('.inner').forEach(function(d) {
                    var rspan = d.querySelector('.rname, .c-name span, .c-name');
                    var rname = rspan ? rspan.innerText.trim() : '';
                    if (!rname) return;
                    var vals = Array.from(d.querySelectorAll('.c-val')).map(function(v){ return v.innerText.trim(); });
                    rows.push({ range: rname, count: vals[0]||'0', paid: vals[1]||'0', unpaid: vals[2]||'0', revenue: vals[3]||'─' });
                });
            }
            if (!rows.length) {
                container.querySelectorAll('table tbody tr').forEach(function(tr) {
                    var tds = tr.querySelectorAll('td');
                    if (tds.length < 4) return;
                    var rname = tds[0].innerText.trim().replace(/^[>+\\-\\s]+/, '').trim();
                    if (rname && /[A-Za-z]/.test(rname) && !rname.toLowerCase().includes('range'))
                        rows.push({ range: rname, count: tds[1].innerText.trim(), paid: tds[2].innerText.trim(),
                            unpaid: tds[3].innerText.trim(), revenue: tds[4] ? tds[4].innerText.trim() : '─' });
                });
            }
            return rows;
        """) or []
        stats = drv.execute_script("""
            var container = document.getElementById('__ivas_debug_container__');
            var s = {total:'0', paid:'0', unpaid:'0', revenue:'─'};
            if (!container) return s;
            [['CountSMS','total'],['PaidSMS','paid'],['UnpaidSMS','unpaid'],['RevenueSMS','revenue']].forEach(function(pair) {
                var el = container.querySelector('#' + pair[0]);
                if (el) s[pair[1]] = el.innerText.trim();
            });
            return s;
        """) or {}
        drv.execute_script("var c=document.getElementById('__ivas_debug_container__'); if(c) c.remove();")
        return {"summary": summary, "stats": stats}
    except Exception as e:
        log.error(f"[dom_inject] Error: {e}")
        return {"summary": [], "stats": {}}

def do_get_sms(drv, start_date, end_date):
    log.info(f"[getsms] {start_date} -> {end_date}")
    if S.get("is_logged_in"):
        try:
            result = _do_get_sms_selenium(drv, start_date, end_date)
            if result.get("summary"):
                return result
        except Exception as e:
            log.warning(f"[getsms] Selenium error: {e}")

    result = {"summary": [], "stats": {}}
    try:
        sess, csrf_decoded, csrf_encoded, _ = _make_csrf_session(drv)
        r = sess.post(
            "https://www.ivasms.com/portal/sms/received/getsms",
            data={"from": start_date, "to": end_date, "_token": csrf_decoded},
            timeout=25,
        )
        r.raise_for_status()
        raw, enc = _decode_response(r)
        _save_debug_html(raw, "getsms")
        if raw.strip():
            result = _parse_getsms_html(raw)
            if not result.get("summary") and len(raw) > 100:
                result = _parse_via_dom_injection(drv, raw, start_date, end_date)
    except requests.RequestException as e:
        log.error(f"[getsms] HTTP error: {e}")
    return result

def _do_get_sms_selenium(drv, start_date, end_date):
    if not S.get("is_logged_in"):
        return {"summary": [], "stats": {}}
    drv.get(URL_SMS_RX)
    time.sleep(1.5)
    for _ in range(5):
        try:
            body = drv.execute_script("return document.body.innerText;").lower()
            if "checking your browser" in body or "just a moment" in body: time.sleep(2)
            else: break
        except: break

    drv.execute_script(f"""
        var s = document.querySelector("input[id*='start'], input[name*='start'], input[name='from']");
        var e = document.querySelector("input[id*='end'], input[name*='end'], input[name='to']");
        var setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
        if(s) {{ setter.call(s, '{start_date}'); s.dispatchEvent(new Event('input', {{bubbles:true}})); s.dispatchEvent(new Event('change', {{bubbles:true}})); }}
        if(e) {{ setter.call(e, '{end_date}'); e.dispatchEvent(new Event('input', {{bubbles:true}})); e.dispatchEvent(new Event('change', {{bubbles:true}})); }}
    """)
    time.sleep(0.3)

    try:
        btn = drv.find_element(By.XPATH, "//button[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'get sms')]")
        drv.execute_script("arguments[0].scrollIntoView(); arguments[0].click();", btn)
    except:
        drv.execute_script("""
            var btn = document.querySelector('button[type=submit], input[type=submit], .btn-get-sms');
            if (btn) { btn.click(); return; }
            var f = document.querySelector('form');
            if (f) f.dispatchEvent(new Event('submit', {bubbles:true, cancelable:true}));
        """)

    time.sleep(1.5)
    for _ in range(15):
        try:
            body = drv.execute_script("return document.body.innerText;").lower()
            if "loading" not in body and "processing" not in body: break
        except: pass
        time.sleep(0.8)

    summary_dom = drv.execute_script("""
        var rows = [];
        document.querySelectorAll('div.rng').forEach(function(d) {
            var rname  = d.querySelector('.rname, .c-name span, .c-name, [class*="rname"], [class*="range-name"]');
            var count  = d.querySelector('.v-count');
            var paid   = d.querySelector('.v-paid');
            var unpaid = d.querySelector('.v-unpaid');
            var rev    = d.querySelector('.v-rev');
            var onclick    = d.getAttribute('onclick') || '';
            var rawRangeId = '';
            var rnameText  = rname ? rname.innerText.trim() : '';
            var mId = onclick.match(/toggleRange\\s*\\(\\s*['"]([^'"]*)['"]/);
            if (mId) rawRangeId = mId[1];
            if (!rnameText) {
                if (rawRangeId) {
                    var m2 = onclick.match(/toggleRange\\s*\\(\\s*['"][^'"]*['"]\\s*,\\s*['"]([^'"]+)['"]/);
                    rnameText = m2 ? m2[1].replace(/_/g, ' ') : rawRangeId.replace(/_/g, ' ');
                }
            }
            if (rnameText) rows.push({ range: rnameText, range_id: rawRangeId,
                count:   count  ? count.innerText.trim()  : '0',
                paid:    paid   ? paid.innerText.trim()   : '0',
                unpaid:  unpaid ? unpaid.innerText.trim() : '0',
                revenue: rev    ? rev.innerText.trim()    : '─' });
        });
        if (!rows.length) {
            document.querySelectorAll('table tbody tr').forEach(function(tr) {
                var tds = tr.querySelectorAll('td');
                if(tds.length >= 5) {
                    var r = tds[0].innerText.trim().replace(/^[>+\\-\\s]+/, '').trim();
                    if (r && /[A-Za-z]/.test(r) && !r.toLowerCase().includes('range'))
                        rows.push({range: r, range_id: '', count: tds[1].innerText.trim(),
                                   paid: tds[2].innerText.trim(), unpaid: tds[3].innerText.trim(),
                                   revenue: tds[4].innerText.trim()});
                }
            });
        }
        return rows;
    """) or []

    stats_dom = drv.execute_script("""
        var s = {total: '0', paid: '0', unpaid: '0', revenue: '─'};
        ['CountSMS','PaidSMS','UnpaidSMS','RevenueSMS'].forEach(function(id) {
            var el = document.getElementById(id);
            if(el) {
                if(id==='CountSMS')   s.total   = el.innerText.trim();
                if(id==='PaidSMS')    s.paid    = el.innerText.trim();
                if(id==='UnpaidSMS')  s.unpaid  = el.innerText.trim();
                if(id==='RevenueSMS') s.revenue = el.innerText.trim();
            }
        });
        return s;
    """) or {}

    return {"summary": summary_dom, "stats": stats_dom}

def sess(cid=None):    return dict(S)
def is_online(cid=None): return bool(S.get("is_logged_in"))
def get_drv(cid=None): return S.get("driver")
def set_s(key, val):   S[key] = val
def _get_us(cid=None): return S

def start_engine(cid, msg_id=None):
    with _sess_lock:
        us = S
        if us.get("thread") and us["thread"].is_alive(): return
        us.clear()
        us.update({
            "stop":            threading.Event(),
            "busy":            threading.Event(),
            "seen":            set(),
            "is_logged_in":    False,
            "driver":          None,
            "data_lock":       threading.Lock(),
            "traffic_counter": Counter(),
            "best_ranges":     {},
            "last_reload":     0.0,
            "tanggal":         datetime.strptime(server_today(), "%Y-%m-%d").date(),
            "live_otp_active": False,
            "sms_seen":        set(),
            "last_sms_poll":   0.0,
        })
    t = threading.Thread(target=_engine_loop, args=(cid, msg_id), daemon=True)
    t.start()
    with _sess_lock:
        S["thread"] = t

def stop_engine(cid=None):
    us = S
    if us.get("stop"): us["stop"].set()
    if us.get("driver"):
        try: us["driver"].quit()
        except: pass
    us["is_logged_in"] = False
    us["driver"]       = None

def _engine_loop(cid, msg_id):
    acc    = get_account(cid)
    em, pw = acc.get("email", ""), acc.get("password", "")
    us     = S
    drv    = None
    try:
        drv          = make_driver()
        us["driver"] = drv
        if not try_cookie_login(drv):
            do_login(drv, em, pw)
        save_cookies(drv)
        us["is_logged_in"] = True
        init_hub(drv, em)
        from bot import page_home
        txt, markup = page_home(cid)
        if msg_id: edit(cid, msg_id, txt, markup)
        _monitor(drv)
    except Exception as e:
        log.error(f"Engine crash: {e}")
        if msg_id:
            edit(cid, msg_id, f"❌ <b>Error:</b>\n<code>{esc(str(e)[:150])}</code>",
                 kb([[("🔄 Coba Lagi", "engine:start")]]))
    finally:
        us["is_logged_in"] = False
        if drv:
            try: drv.quit()
            except: pass

def _monitor(drv):
    us        = S
    stop_ev   = us.get("stop")
    seen      = us.get("seen", set())
    last_chk  = 0.0

    drv.get(URL_LIVE)
    us["last_reload"]     = time.time()
    us["live_otp_active"] = True

    threading.Thread(target=_auto_sms_loop, args=(drv,), daemon=True).start()

    while stop_ev and not stop_ev.is_set():
        try:
            if us.get("busy") and us["busy"].is_set():
                time.sleep(0.2); continue
            scrape_live(drv, seen, us)
            now = time.time()
            if now - last_chk >= 300:
                last_chk = now
                threading.Thread(target=check_auto_range, args=(drv,), daemon=True).start()
        except Exception as e:
            log.warning(f"Monitor: {e}")
            break
        time.sleep(LIVE_POLL)

    us["live_otp_active"] = False

def _auto_sms_loop(drv):
    us      = S
    stop_ev = us.get("stop")
    time.sleep(8)
    while stop_ev and not stop_ev.is_set():
        try:
            _auto_sms_check(drv)
        except Exception as e:
            log.warning(f"[sms_poll] Error: {e}")
        for _ in range(SMS_POLL_INTERVAL * 2):
            if stop_ev.is_set(): break
            time.sleep(0.5)

def _auto_sms_check(drv):
    us    = S
    today = server_today()
    if us.get("sms_date") != today:
        us["sms_seen"]      = set()
        us["sms_date"]      = today
        us["sms_first_run"] = True

    sms_seen     = us.setdefault("sms_seen", set())
    is_first_run = us.get("sms_first_run", True)

    try:
        result = do_get_sms(drv, today, today)
    except Exception as e:
        log.warning(f"[sms_poll] do_get_sms error: {e}")
        return

    summary = result.get("summary", [])
    if not summary:
        if is_first_run: us["sms_first_run"] = False
        return

    new_count = 0
    for rng in summary:
        rng_name = rng.get("range", "").strip()
        rng_id   = (rng.get("range_id") or "").strip()
        if not rng_id and rng_name:
            rng_id = re.sub(r"[^A-Z0-9_]", "_", rng_name.upper().strip())
        if not rng_id and not rng_name: continue
        if rng.get("count", "0") == "0": continue

        sms_list = _fetch_sms_detail_selenium(drv, rng_id, rng_name)
        if not sms_list:
            sms_list = _fetch_sms_detail(drv, today, rng_id, rng_name)

        for sms in sms_list:
            uid = sms.get("uid", "")
            if not uid: continue
            if is_first_run:
                sms_seen.add(uid)
            else:
                if uid in sms_seen: continue
                sms_seen.add(uid)
                new_count += 1
                _forward_sms_to_telegram(sms)
                time.sleep(0.3)

    if is_first_run: us["sms_first_run"] = False
    if new_count: log.info(f"[sms_poll] ✅ {new_count} SMS baru di-forward")

def _fetch_sms_detail_selenium(drv, rng_id, rng_name):
    try:
        rng_name_safe = repr(rng_name)
        rng_id_safe   = repr(rng_id)
        clicked       = drv.execute_script(f"""
            var rngId   = {rng_id_safe};
            var rngName = {rng_name_safe};
            var rngIdUp = rngId.toUpperCase().replace(/[^A-Z0-9]/g,'');
            var rngNmUp = rngName.toUpperCase().replace(/[^A-Z0-9]/g,'');
            function score(oc) {{
                var ocUp = oc.toUpperCase().replace(/[^A-Z0-9]/g,'');
                if (ocUp.indexOf(rngIdUp) !== -1 || rngIdUp.indexOf(ocUp.substring(3,20)) !== -1) return 3;
                if (ocUp.indexOf(rngNmUp) !== -1 || rngNmUp.indexOf(ocUp.substring(3,20)) !== -1) return 2;
                var nums = rngNmUp.replace(/[^0-9]/g,'');
                if (nums.length >= 4 && ocUp.indexOf(nums) !== -1) return 1;
                return 0;
            }}
            var best = null, bestScore = 0;
            document.querySelectorAll('div.rng').forEach(function(d) {{
                var oc = d.getAttribute('onclick') || '';
                var s  = score(oc);
                if (s > bestScore) {{ best = d; bestScore = s; }}
            }});
            if (!best || bestScore === 0) return false;
            best.click();
            return true;
        """)
        if not clicked: return []
        time.sleep(2)

        sms_rows = drv.execute_script(f"""
            var rngName = {rng_name_safe};
            var rngNmUp = rngName.toUpperCase().replace(/[^A-Z0-9]/g,'');
            var results = [];
            var candidates = [];
            document.querySelectorAll('[id^="sp_"]').forEach(function(el) {{
                if (el.offsetParent !== null || el.style.display !== 'none') candidates.push(el);
            }});
            var sub = null;
            if (candidates.length === 1) {{ sub = candidates[0]; }}
            else if (candidates.length > 1) {{
                var bestScore = -1;
                candidates.forEach(function(el) {{
                    var idUp = el.id.toUpperCase().replace(/[^A-Z0-9]/g,'');
                    var common = 0;
                    for (var i = 0; i < Math.min(idUp.length, rngNmUp.length); i++) {{
                        if (idUp[i] === rngNmUp[i]) common++;
                    }}
                    if (common > bestScore) {{ bestScore = common; sub = el; }}
                }});
            }}
            if (!sub) return results;
            var currentPhone = '';
            sub.querySelectorAll('tr').forEach(function(row) {{
                var tds = row.querySelectorAll('td');
                if (!tds.length) return;
                var firstText = (tds[0].innerText || '').trim();
                var numOnly   = firstText.replace(/[^0-9]/g, '');
                if (tds.length === 1 && numOnly.length >= 8 && numOnly.length <= 15) {{
                    currentPhone = numOnly; return;
                }}
                if (numOnly.length >= 8 && numOnly.length <= 15 &&
                    /^[+]?[0-9][0-9 ().\\u002d]*$/.test(firstText)) {{
                    currentPhone = numOnly; return;
                }}
                if (tds.length >= 3) {{
                    var sender  = (tds[0].innerText || '').trim();
                    var message = (tds[1].innerText || '').trim();
                    var ts      = (tds[2].innerText || '').trim();
                    var rev     = tds.length > 3 ? (tds[3].innerText || '').trim() : '';
                    var lo = sender.toLowerCase();
                    if (lo === 'sender' || lo === 'from' || lo === 'pengirim') return;
                    if (!message || !ts) return;
                    if (!/[0-9]{{1,2}}:[0-9]{{2}}/.test(ts)) return;
                    if (!currentPhone) {{
                        var prev = row.parentElement && row.parentElement.previousElementSibling;
                        if (prev) {{
                            var p = prev.innerText.replace(/[^0-9]/g,'');
                            if (p.length >= 8 && p.length <= 15) currentPhone = p;
                        }}
                    }}
                    results.push({{ phone: currentPhone, sender: sender, message: message,
                        time: ts, revenue: rev, range: rngName,
                        uid: currentPhone + '|' + sender + '|' + ts }});
                }}
            }});
            return results;
        """) or []
        return sms_rows
    except Exception as e:
        log.warning(f"[sms_detail_sel] {rng_name}: {e}")
        return []

def _fetch_sms_detail(drv, today, rng_id, rng_name):
    def _dec(r):
        enc = r.headers.get("Content-Encoding", "").lower()
        raw = r.content
        if "br" in enc:
            try:
                import brotli as _br
                return _br.decompress(raw).decode("utf-8", errors="replace")
            except: pass
        if "gzip" in enc:
            import gzip as _gz
            try: return _gz.decompress(raw).decode("utf-8", errors="replace")
            except: pass
        try:    return r.text
        except: return raw.decode("utf-8", errors="replace")

    _HEADERS = {
        "Accept":           "text/html, */*; q=0.01",
        "Accept-Encoding":  "gzip, deflate",
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin":           "https://www.ivasms.com",
        "Referer":          "https://www.ivasms.com/portal/sms/received",
    }
    sms_list = []
    try:
        sess, csrf, _enc, _ck = _make_csrf_session(drv)
        r1 = sess.post(
            "https://www.ivasms.com/portal/sms/received/getsms/number",
            data={"_token": csrf, "start": today, "end": today, "range": rng_name},
            headers=_HEADERS, timeout=20,
        )
        if r1.status_code in (419, ):
            return []
        if r1.status_code != 200:
            return []

        html1 = _dec(r1)
        _save_debug_html(html1, "step1_" + rng_name[:20])
        if "<!DOCTYPE" in html1[:200] or "<html" in html1[:200].lower():
            return []

        phones = []
        for m in re.finditer(r"toggleNum\w*\s*\(\s*'([^']*)'\s*,\s*'([^']*)'", html1):
            raw_ph = m.group(1).strip()
            clean  = re.sub(r'[^0-9]', '', raw_ph)
            if 8 <= len(clean) <= 15 and not any(p['clean'] == clean for p in phones):
                phones.append({'raw': raw_ph, 'clean': clean})

        if not phones:
            for m in re.finditer(r'class=["\'][^"\']*c-name[^"\']*["\'][^>]*>\s*<[^>]+>\s*([+0-9][\d\s().+\-]{6,18})\s*<', html1):
                raw_ph = m.group(1).strip()
                clean  = re.sub(r'[^0-9]', '', raw_ph)
                if 8 <= len(clean) <= 15 and not any(p['clean'] == clean for p in phones):
                    phones.append({'raw': raw_ph, 'clean': clean})

        if not phones:
            for m in re.finditer(r'<(?:span|td|div|p)[^>]*>\s*(\+?[0-9]{9,15})\s*</(?:span|td|div|p)>', html1):
                raw_ph = m.group(1).strip()
                clean  = re.sub(r'[^0-9]', '', raw_ph)
                if 8 <= len(clean) <= 15 and not any(p['clean'] == clean for p in phones):
                    phones.append({'raw': raw_ph, 'clean': clean})

        if not phones: return []

        SKIP = {"sender","message","time","revenue","pesan","waktu","from","pengirim"}
        for ph in phones:
            try:
                r2 = sess.post(
                    "https://www.ivasms.com/portal/sms/received/getsms/number/sms",
                    data={"_token": csrf, "start": today, "end": today, "Number": ph['raw'], "Range": rng_name},
                    headers=_HEADERS, timeout=15,
                )
                if r2.status_code != 200: continue
                html2  = _dec(r2)
                _save_debug_html(html2, "step2_" + ph['clean'])
                tbody_m = re.search(r'<tbody[^>]*>(.*?)</tbody>', html2, re.S)
                tr_html = tbody_m.group(1) if tbody_m else html2
                for tr_m in re.finditer(r'<tr[^>]*>(.*?)</tr>', tr_html, re.S):
                    tds = [_strip_tags(td).strip() for td in re.findall(r'<td[^>]*>(.*?)</td>', tr_m.group(1), re.S)]
                    if len(tds) < 3: continue
                    sender, message, ts = tds[0], tds[1], tds[2]
                    rev = tds[3] if len(tds) > 3 else ""
                    if sender.lower() in SKIP: continue
                    if not message or not ts: continue
                    if not re.search(r'\d{1,2}:\d{2}', ts): continue
                    uid = f"{rng_name}|{ph['clean']}|{today}|{ts}|{message[:30]}"
                    sms_list.append({"uid": uid, "range": rng_name, "phone": ph['clean'],
                                     "sender": sender, "message": message, "time": ts,
                                     "revenue": rev, "date": today})
                time.sleep(0.2)
            except Exception as ex:
                log.warning(f"[sms_detail] {rng_name}/{ph['clean']}: {ex}")
    except Exception as e:
        log.warning(f"[sms_detail] {rng_name}: {e}")
    return sms_list

def _mask_phone(phone, with_cc=False):
    p = re.sub(r'[^0-9]', '', str(phone))
    if len(p) <= 6:
        return ("+" + p) if with_cc else p
    if with_cc:
        cc   = p[:2]
        rest = p[2:]
        if len(rest) >= 6:
            masked_rest = rest[:2] + "★★★★" + rest[-4:]
        else:
            masked_rest = rest[:2] + "★★" + rest[-2:]
        return f"+{cc}{masked_rest}"
    return p[:4] + "★★★★" + p[-4:]

def _extract_otp(message):
    if not message: return None
    m = re.search(r'([0-9]{3,4}[-][0-9]{3,4})', message)
    if m: return m.group(1)
    for pat in [
        r'[Cc]odigo[^0-9]*([0-9]{4,8})',
        r'[Cc]ode[\s:]+([0-9]{4,8})',
        r'OTP[^0-9]*([0-9]{4,8})',
        r'kode[^0-9]*([0-9]{4,8})',
        r'verif[a-z]*[^0-9]*([0-9]{4,8})',
    ]:
        m = re.search(pat, message, re.I)
        if m:
            val = m.group(1)
            if not re.match(r'^20[12][0-9]', val): return val
    for m in re.finditer(r'(?<![0-9])([0-9]{6})(?![0-9])', message):
        val = m.group(1)
        if not re.match(r'^20[12][0-9]', val): return val
    for m in re.finditer(r'(?<![0-9])([0-9]{4,8})(?![0-9])', message):
        val = m.group(1)
        if not re.match(r'^20[12][0-9]$', val): return val
    return None

def _build_otp_keyboard(otp, phone, rng):
    btn_rows = []
    if otp:
        btn_rows.append([{"text": f"🔑  {otp}", "copy_text": {"text": otp}}])

    groups = get_groups()
    if not groups:
        groups = [
            {"id": "0", "title": GROUP_TITLE_1, "invite_link": GROUP_LINK_1},
            {"id": "1", "title": GROUP_TITLE_2, "invite_link": GROUP_LINK_2},
        ]

    ch_btns = []
    for g in groups:
        title = g.get("title", "Channel")[:16]
        link  = g.get("invite_link", "").strip()
        if link and not link.endswith("_KAMU"):
            ch_btns.append({"text": f"📢 {title}", "url": link})
        else:
            gid = str(g["id"])
            cb  = f"ch:{gid}|{phone}|{otp or ''}|{rng}"
            ch_btns.append({"text": f"📢 {title}", "callback_data": cb[:64]})
    if ch_btns:
        btn_rows.append(ch_btns)

    return {"inline_keyboard": btn_rows} if btn_rows else None

def _send_to_targets(text, markup):
    groups  = get_groups()
    targets = [str(OWNER_ID)]
    for g in groups:
        gid = str(g["id"])
        if gid not in targets:
            targets.append(gid)
    for target in targets:
        payload = {"chat_id": target, "text": text, "parse_mode": "HTML"}
        if markup: payload["reply_markup"] = markup
        try: _tg.post(f"{TG_API}/sendMessage", json=payload, timeout=10)
        except Exception as ef: log.warning(f"[forward] kirim ke {target}: {ef}")
        time.sleep(0.15)

def _forward_sms_to_telegram(sms):
    import html as _html
    import re as _re2
    phone   = sms.get("phone", "")
    sender  = sms.get("sender", "")
    message = sms.get("message", "")
    rng     = sms.get("range", "")
    fl      = flag(rng)
    masked  = _mask_phone(phone, with_cc=True)
    badge   = "WS" if "whatsapp" in sender.lower() else (sender[:2].upper() if sender else "WS")
    msg_clean = _re2.sub(r'<[^>]+>', '', _html.unescape(message)).strip()
    otp       = _extract_otp(msg_clean)
    text      = f"<b>{badge}</b> | <code>{masked}</code> | {fl}"
    markup    = _build_otp_keyboard(otp, phone, rng)
    _send_to_targets(text, markup)
    log.info(f"[forward] {rng} | {masked} | otp={otp}")

def _forward_batch_to_telegram(rng_name, sms_list):
    import html as _html
    import re as _re2
    if not sms_list: return
    fl           = flag(rng_name)
    phone_order  = []
    phone_groups = {}
    for sms in sms_list:
        ph = sms.get("phone", "?")
        if ph not in phone_groups:
            phone_groups[ph] = []
            phone_order.append(ph)
        phone_groups[ph].append(sms)

    lines    = []
    all_otps = []
    for phone in phone_order:
        msgs   = phone_groups[phone]
        masked = _mask_phone(phone, with_cc=True)
        sender = msgs[0].get("sender", "")
        badge  = "WS" if "whatsapp" in sender.lower() else (sender[:2].upper() if sender else "WS")
        lines.append(f"<b>{badge}</b> | <code>{masked}</code> | {fl}")
        otps = []
        for sms in msgs:
            msg_clean = _re2.sub(r'<[^>]+>', '', _html.unescape(sms.get("message", ""))).strip()
            otp       = _extract_otp(msg_clean)
            if otp and otp not in otps:
                otps.append(otp)
                all_otps.append(otp)
        if not otps:
            msg0 = _re2.sub(r'<[^>]+>', '', _html.unescape(msgs[0].get("message", ""))).strip()
            lines.append(f"<i>{esc(msg0[:80])}</i>")
        elif len(otps) == 1:
            lines.append(f"<b><code>{otps[0]}</code></b>")
        else:
            for i, otp in enumerate(otps, 1):
                lines.append(f"OTP {i}: <b><code>{otp}</code></b>")
        lines.append("")

    text     = "\n".join(lines).rstrip()
    btn_rows = []
    for otp in all_otps:
        btn_rows.append([{"text": f"🔑  {otp}", "copy_text": {"text": otp}}])

    groups = get_groups()
    if not groups:
        groups = [
            {"id": "0", "title": GROUP_TITLE_1, "invite_link": GROUP_LINK_1},
            {"id": "1", "title": GROUP_TITLE_2, "invite_link": GROUP_LINK_2},
        ]

    ch_btns = []
    for g in groups:
        title       = g.get("title", "Channel")[:14]
        link        = g.get("invite_link", "").strip()
        first_phone = phone_order[0] if phone_order else ""
        first_otp   = all_otps[0] if all_otps else ""
        if link and not link.endswith("_KAMU"):
            ch_btns.append({"text": f"📢 {title}", "url": link})
        else:
            gid = str(g["id"])
            cb  = f"ch:{gid}|{first_phone}|{first_otp}|{rng_name}"
            ch_btns.append({"text": f"📢 {title}", "callback_data": cb[:64]})
    if ch_btns:
        btn_rows.append(ch_btns)
    markup = {"inline_keyboard": btn_rows} if btn_rows else None
    _send_to_targets(text, markup)
    log.info(f"[forward_batch] {rng_name} | {len(phone_order)} nomor | {len(all_otps)} OTP")

def inject_top_range_with_id(drv, country, qty, callback, email=""):
    us = S
    with us.get("data_lock", threading.Lock()):
        full_range = us.get("best_ranges", {}).get(country, country)
    return do_inject_hub(drv, full_range, qty, callback, email)

def do_get_sms_today(*args, **kwargs):
    pass

def _fetch_sms_detail_http(*args, **kwargs):
    return []

def _parse_sms_detail_html(*args, **kwargs):
    return []
