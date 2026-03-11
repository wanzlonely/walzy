import json, threading, time
from config import USERS_FILE, log

_cache:          dict           = {}
_dirty:          bool           = False
_lock:           threading.Lock = threading.Lock()
_flush_interval: float          = 3.0

def _read_disk() -> dict:
    try:
        with open(USERS_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def _write_disk(data: dict):
    try:
        with open(USERS_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        log.error(f"DB write error: {e}")

def _flush_worker():
    global _dirty
    while True:
        time.sleep(_flush_interval)
        with _lock:
            if _dirty:
                _write_disk(_cache)
                _dirty = False

def init_db():
    global _cache
    with _lock:
        _cache = _read_disk()
    threading.Thread(target=_flush_worker, daemon=True).start()
    log.info(f"DB loaded: {len(_cache)} users")

def db_get(cid) -> dict | None:
    with _lock:
        return _cache.get(str(cid))

def db_set(cid, field: str, value):
    global _dirty
    cid = str(cid)
    with _lock:
        if cid not in _cache:
            _cache[cid] = {}
        _cache[cid][field] = value
        _dirty = True

def db_update(cid, fields: dict):
    global _dirty
    cid = str(cid)
    with _lock:
        if cid not in _cache:
            _cache[cid] = {}
        _cache[cid].update(fields)
        _dirty = True

def db_delete(cid):
    global _dirty
    with _lock:
        _cache.pop(str(cid), None)
        _dirty = True

def db_all() -> dict:
    with _lock:
        return dict(_cache)

def db_flush_now():
    global _dirty
    with _lock:
        _write_disk(_cache)
        _dirty = False
