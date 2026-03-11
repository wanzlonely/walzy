import sys, os, time, traceback, subprocess, requests

def _auto_install_chromium():
    import shutil
    if shutil.which("chromium-browser") or shutil.which("chromium"):
        return
    is_termux = os.path.isdir("/data/data/com.termux")
    if is_termux:
        try:
            subprocess.run(["pkg", "install", "-y", "chromium"], check=False)
        except Exception:
            pass
    else:
        try:
            subprocess.run(["apt-get", "update", "-qq"], check=False)
            subprocess.run([
                "apt-get", "install", "-y", "-qq",
                "chromium", "chromium-driver",
                "libglib2.0-0", "libnss3", "libnspr4",
                "libatk1.0-0", "libatk-bridge2.0-0", "libcups2",
                "libdbus-1-3", "libdrm2", "libxcb1", "libxkbcommon0",
                "libx11-6", "libxcomposite1", "libxdamage1", "libxext6",
                "libxfixes3", "libxrandr2", "libgbm1", "libasound2",
            ], check=False)
        except Exception:
            pass

_auto_install_chromium()

from config import BOT_NAME, BOT_TOKEN, OWNER_ID, ENV, TG_API, log, find_chrome
from database import db_all, init_db
from core import start_engine
from bot import handle_message, handle_callback

_tg_sess = requests.Session()
_tg_sess.headers.update({"Content-Type": "application/json"})

def listener():
    offset = None
    log.info("Listener started")
    while True:
        try:
            resp = _tg_sess.get(
                f"{TG_API}/getUpdates",
                params={
                    "timeout":         30,
                    "offset":          offset,
                    "allowed_updates": ["message", "callback_query"],
                },
                timeout=35)
            data = resp.json()
            for upd in data.get("result", []):
                offset = upd["update_id"] + 1
                try:
                    if "callback_query" in upd:
                        handle_callback(upd["callback_query"])
                    elif "message" in upd:
                        handle_message(upd["message"])
                except Exception as e:
                    log.error(f"handler error: {e}\n{traceback.format_exc()}")
        except KeyboardInterrupt:
            break
        except requests.RequestException as e:
            log.warning(f"listener network: {e}")
            time.sleep(5)
        except Exception as e:
            log.error(f"listener crash: {e}\n{traceback.format_exc()}")
            time.sleep(2)

def main():
    log.info(f"{BOT_NAME} booting — ENV={ENV}")
    init_db()

    chrome = find_chrome()
    if not chrome:
        log.warning("Chromium NOT found!")
    else:
        log.info(f"Chrome: {chrome}")

    users  = db_all()
    booted = 0
    for cid, u in users.items():
        if u.get("banned"):
            continue
        if u.get("status") == "active":
            log.info(f"Booting node: {cid}")
            start_engine(cid)
            time.sleep(0.8)
            booted += 1

    log.info(f"{BOT_NAME} ready — {booted}/{len(users)} nodes booted")

    try:
        listener()
    except KeyboardInterrupt:
        log.info("Shutdown.")

if __name__ == "__main__":
    main()
