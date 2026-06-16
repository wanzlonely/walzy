import json
import os
from datetime import date, datetime

_DATA_FILE = "data.json"

FREE_DAILY       = 2
PREMIUM_DAILY    = 50
COOLDOWN_SECONDS = 60
TEMPLATE_COUNT   = 5

_data: dict = {"users": {}, "template_counter": 0}


def _load() -> None:
    global _data
    if os.path.exists(_DATA_FILE):
        try:
            with open(_DATA_FILE, "r", encoding="utf-8") as f:
                _data = json.load(f)
        except Exception:
            _data = {"users": {}, "template_counter": 0}
    _data.setdefault("template_counter", 0)
    _data.setdefault("users", {})


def _save() -> None:
    with open(_DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(_data, f, ensure_ascii=False, indent=2)


_load()


def _user(user_id: int) -> dict:
    key = str(user_id)
    if key not in _data["users"]:
        _data["users"][key] = {}
    return _data["users"][key]


def get_email(user_id: int) -> dict | None:
    u = _user(user_id)
    if "email" not in u:
        return None
    return {"email": u["email"], "password": u["password"]}


def set_email(user_id: int, email: str, password: str) -> None:
    u = _user(user_id)
    u["email"]    = email
    u["password"] = password
    _save()


def delete_email(user_id: int) -> None:
    u = _user(user_id)
    for key in ("email", "password", "template_index"):
        u.pop(key, None)
    _save()


def has_email(user_id: int) -> bool:
    return "email" in _user(user_id)


def is_premium(user_id: int) -> bool:
    return bool(_user(user_id).get("premium", False))


def set_premium(user_id: int, value: bool) -> None:
    _user(user_id)["premium"] = value
    _save()


def get_max_daily(user_id: int) -> int:
    return PREMIUM_DAILY if is_premium(user_id) else FREE_DAILY


def assign_template(user_id: int) -> int:
    idx = _data["template_counter"] % TEMPLATE_COUNT
    _data["template_counter"] += 1
    _user(user_id)["template_index"] = idx
    _save()
    return idx


def get_template_index(user_id: int) -> int:
    return _user(user_id).get("template_index", 0)


def can_send(user_id: int) -> tuple[bool, str]:
    today = str(date.today())
    now   = datetime.now().timestamp()
    u     = _user(user_id)
    rl    = u.get("rate_limit", {"date": "", "count": 0, "last_send": 0})

    if rl.get("date") != today:
        rl = {"date": today, "count": 0, "last_send": 0}

    elapsed = now - rl.get("last_send", 0)
    if elapsed < COOLDOWN_SECONDS:
        remaining = int(COOLDOWN_SECONDS - elapsed)
        return False, f"⏳ Cooldown aktif — tunggu *{remaining} detik* lagi."

    max_daily = get_max_daily(user_id)
    tier      = "💎 Premium" if is_premium(user_id) else "🆓 Free"
    if rl.get("count", 0) >= max_daily:
        return False, f"🚫 Kuota harian *{max_daily}x* ({tier}) habis.\nSilakan coba lagi besok."

    return True, ""


def log_send(user_id: int) -> None:
    today = str(date.today())
    now   = datetime.now().timestamp()
    u     = _user(user_id)
    rl    = u.get("rate_limit", {"date": "", "count": 0, "last_send": 0})

    if rl.get("date") != today:
        rl = {"date": today, "count": 0, "last_send": 0}

    rl["count"]     = rl.get("count", 0) + 1
    rl["last_send"] = now
    u["rate_limit"] = rl
    _save()


def get_send_count(user_id: int) -> int:
    today = str(date.today())
    u     = _user(user_id)
    rl    = u.get("rate_limit", {})
    if rl.get("date") != today:
        return 0
    return rl.get("count", 0)
