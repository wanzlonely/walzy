import sys, time, traceback, requests

from engine import BOT_TOKEN, TG_API, log, get_account
from bot import handle_message, handle_callback

_sess = requests.Session()
_sess.headers.update({"Content-Type": "application/json"})

def listener():
    offset = None
    log.info("━" * 36)
    log.info("  PREMIUM iVAS Bot — Started")
    log.info("━" * 36)
    while True:
        try:
            r = _sess.get(
                f"{TG_API}/getUpdates",
                params={"timeout": 20, "offset": offset, "allowed_updates": ["message", "callback_query"]},
                timeout=25
            )
            updates = r.json().get("result", [])
            for upd in updates:
                offset = upd["update_id"] + 1
                try:
                    if "callback_query" in upd: handle_callback(upd["callback_query"])
                    elif "message"       in upd: handle_message(upd["message"])
                except Exception as e:
                    log.error(f"Handler error: {e}\n{traceback.format_exc()}")
        except KeyboardInterrupt:
            log.info("Shutdown.")
            sys.exit(0)
        except requests.RequestException as e:
            log.warning(f"Network: {e}")
            time.sleep(2)
        except Exception as e:
            log.error(f"Listener crash: {e}")
            time.sleep(1)

if __name__ == "__main__":
    listener()
