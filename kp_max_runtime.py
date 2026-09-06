import os
import sqlite3
import time
from pathlib import Path
from threading import Lock

DB_PATH = Path(os.getenv("KP_MAX_RUNTIME_DB", "/opt/kp-api/data/kp_max_runtime.sqlite"))
LOCK_TTL_SECONDS = int(os.getenv("KP_MAX_KP_LOCK_TTL_SECONDS", "600"))
_INIT_LOCK = Lock()
_INITIALIZED_PATH = ""


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=5.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db() -> None:
    global _INITIALIZED_PATH
    target = str(DB_PATH)
    if _INITIALIZED_PATH == target and DB_PATH.exists():
        return
    with _INIT_LOCK:
        if _INITIALIZED_PATH == target and DB_PATH.exists():
            return
        conn = _connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS max_users (
                user_id TEXT PRIMARY KEY,
                first_name TEXT NOT NULL DEFAULT '',
                last_name TEXT NOT NULL DEFAULT '',
                display_name TEXT NOT NULL DEFAULT '',
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS kp_locks (
                kp_ref TEXT PRIMARY KEY,
                kp_number TEXT NOT NULL,
                user_id TEXT NOT NULL,
                acquired_at INTEGER NOT NULL,
                touched_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_kp_locks_user ON kp_locks(user_id);
            CREATE INDEX IF NOT EXISTS idx_kp_locks_expires ON kp_locks(expires_at);
            """)
        finally:
            conn.close()
        try:
            os.chmod(DB_PATH, 0o600)
        except OSError:
            pass
        _INITIALIZED_PATH = target


def _now() -> int:
    return int(time.time())
def remember_user(user_id: str, first_name: str = "", last_name: str = "", display_name: str = "") -> None:
    uid = str(user_id or "").strip()
    if not uid:
        return
    first = str(first_name or "").strip()
    last = str(last_name or "").strip()
    display = str(display_name or "").strip()
    if not display:
        display = " ".join(x for x in (first, last) if x).strip()
    init_db()
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO max_users(user_id,first_name,last_name,display_name,updated_at) VALUES(?,?,?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET first_name=excluded.first_name,last_name=excluded.last_name,"
            "display_name=excluded.display_name,updated_at=excluded.updated_at",
            (uid, first, last, display, _now()),
        )
    finally:
        conn.close()


def remember_from_payload(payload: dict) -> str:
    if not isinstance(payload, dict):
        return ""
    callback = payload.get("callback") if isinstance(payload.get("callback"), dict) else {}
    message = payload.get("message") if isinstance(payload.get("message"), dict) else {}
    user = callback.get("user") if isinstance(callback.get("user"), dict) else {}
    if not user:
        user = message.get("sender") if isinstance(message.get("sender"), dict) else {}
    if not user and isinstance(payload.get("user"), dict):
        user = payload.get("user")
    uid = str(user.get("user_id") or user.get("id") or "").strip()
    if not uid:
        return ""
    first = str(user.get("first_name") or "").strip()
    last = str(user.get("last_name") or "").strip()
    display = str(user.get("name") or user.get("display_name") or "").strip()
    if not display:
        display = " ".join(x for x in (first, last) if x).strip()
    if not display:
        display = str(user.get("username") or "").strip()
    remember_user(uid, first, last, display)
    return uid


def user_label(user_id: str, conn: sqlite3.Connection | None = None) -> str:
    uid = str(user_id or "").strip()
    if not uid:
        return "неизвестный пользователь"
    own = conn is None
    if own:
        init_db(); conn = _connect()
    try:
        row = conn.execute("SELECT display_name,first_name,last_name FROM max_users WHERE user_id=?", (uid,)).fetchone()
        if row:
            label = str(row["display_name"] or "").strip()
            if not label:
                label = " ".join(x for x in (row["first_name"], row["last_name"]) if x).strip()
            if label:
                return label
        return f"пользователь {uid}"
    finally:
        if own and conn is not None:
            conn.close()


def acquire(kp_ref: str, kp_number: str, user_id: str, ttl_seconds: int | None = None) -> dict:
    ref = str(kp_ref or "").strip()
    number = str(kp_number or "").strip()
    uid = str(user_id or "").strip()
    if not ref or not uid:
        raise ValueError("kp_ref and user_id are required")
    ttl = max(60, int(ttl_seconds or LOCK_TTL_SECONDS))
    now = _now(); expires = now + ttl
    init_db(); conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM kp_locks WHERE expires_at<=?", (now,))
        row = conn.execute("SELECT * FROM kp_locks WHERE kp_ref=?", (ref,)).fetchone()
        if row and str(row["user_id"]) != uid:
            owner_id = str(row["user_id"])
            result = {"ok": False, "kpRef": ref, "kpNumber": str(row["kp_number"]),
                      "ownerId": owner_id, "ownerLabel": user_label(owner_id, conn),
                      "expiresAt": int(row["expires_at"])}
            conn.execute("COMMIT")
            return result
        if row:
            conn.execute("UPDATE kp_locks SET kp_number=?,touched_at=?,expires_at=? WHERE kp_ref=? AND user_id=?",
                         (number, now, expires, ref, uid))
        else:
            conn.execute("INSERT INTO kp_locks(kp_ref,kp_number,user_id,acquired_at,touched_at,expires_at) VALUES(?,?,?,?,?,?)",
                         (ref, number, uid, now, now, expires))
        conn.execute("COMMIT")
        return {"ok": True, "kpRef": ref, "kpNumber": number, "ownerId": uid,
                "ownerLabel": user_label(uid, conn), "expiresAt": expires}
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        conn.close()


def touch_user(user_id: str, ttl_seconds: int | None = None) -> int:
    uid = str(user_id or "").strip()
    if not uid:
        return 0
    ttl = max(60, int(ttl_seconds or LOCK_TTL_SECONDS))
    now = _now(); expires = now + ttl
    init_db(); conn = _connect()
    try:
        conn.execute("DELETE FROM kp_locks WHERE expires_at<=?", (now,))
        cur = conn.execute("UPDATE kp_locks SET touched_at=?,expires_at=? WHERE user_id=?", (now, expires, uid))
        return int(cur.rowcount or 0)
    finally:
        conn.close()


def release_user(user_id: str) -> int:
    uid = str(user_id or "").strip()
    if not uid:
        return 0
    init_db(); conn = _connect()
    try:
        cur = conn.execute("DELETE FROM kp_locks WHERE user_id=?", (uid,))
        return int(cur.rowcount or 0)
    finally:
        conn.close()
def current_lock(kp_ref: str) -> dict | None:
    ref = str(kp_ref or "").strip()
    if not ref:
        return None
    now = _now(); init_db(); conn = _connect()
    try:
        conn.execute("DELETE FROM kp_locks WHERE expires_at<=?", (now,))
        row = conn.execute("SELECT * FROM kp_locks WHERE kp_ref=?", (ref,)).fetchone()
        if not row:
            return None
        owner_id = str(row["user_id"])
        return {"kpRef": ref, "kpNumber": str(row["kp_number"]), "ownerId": owner_id,
                "ownerLabel": user_label(owner_id, conn), "acquiredAt": int(row["acquired_at"]),
                "touchedAt": int(row["touched_at"]), "expiresAt": int(row["expires_at"])}
    finally:
        conn.close()


def stats() -> dict:
    now = _now(); init_db(); conn = _connect()
    try:
        conn.execute("DELETE FROM kp_locks WHERE expires_at<=?", (now,))
        users = int(conn.execute("SELECT COUNT(*) FROM max_users").fetchone()[0])
        locks = int(conn.execute("SELECT COUNT(*) FROM kp_locks").fetchone()[0])
        return {"users": users, "locks": locks, "ttlSeconds": LOCK_TTL_SECONDS}
    finally:
        conn.close()
