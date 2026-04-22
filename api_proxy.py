#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import base64
import collections
import hashlib
import hmac
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Optional

import requests
import urllib3
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

urllib3.disable_warnings()

app = FastAPI(title="1C KP Realtime API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE = os.getenv(
    "ODATA_BASE_URL",
    "https://aclient.1c-hosting.com/1R88669/1R88669_UT11_bfimz0bdj3/odata/standard.odata",
)
USERNAME = os.getenv("ODATA_USERNAME", "павел")
PASSWORD = os.getenv("ODATA_PASSWORD", "1")
ENTITY = os.getenv("ODATA_ENTITY", "Document_КоммерческоеПредложениеКлиенту")
SEED_DATA_FILE = os.getenv(
    "SEED_DATA_FILE",
    os.getenv("DATA_FILE", "data/kp_2026_march_april.json"),
)
RUNTIME_DATA_FILE = os.getenv("RUNTIME_DATA_FILE", "data/kp_runtime_cache.json")
RUNTIME_META_FILE = os.getenv("RUNTIME_META_FILE", "data/kp_runtime_meta.json")
RUNTIME_CURRENT_FILE = os.getenv("RUNTIME_CURRENT_FILE", "data/kp_runtime_current.json")
STATUS_RULES_FILE = os.getenv("STATUS_RULES_FILE", "data/status_rules.json")
SEED_MAX_AGE_SECONDS = int(os.getenv("SEED_MAX_AGE_SECONDS", "600"))
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "300"))
FAST_PARTIAL_REFRESH_SECONDS = int(os.getenv("FAST_PARTIAL_REFRESH_SECONDS", "120"))
ENABLE_BACKGROUND_REFRESH = os.getenv("ENABLE_BACKGROUND_REFRESH", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
FAST_PARTIAL_CHUNK_SIZE = int(os.getenv("FAST_PARTIAL_CHUNK_SIZE", "150"))
FAST_PARTIAL_DOC_TIMEOUT = float(os.getenv("FAST_PARTIAL_DOC_TIMEOUT", "3.0"))
FAST_PARTIAL_WORKERS = int(os.getenv("FAST_PARTIAL_WORKERS", "20"))
STALE_REFRESH_AFTER_SECONDS = int(os.getenv("STALE_REFRESH_AFTER_SECONDS", "20"))
ENRICH_PER_REFRESH = int(os.getenv("ENRICH_PER_REFRESH", "60"))
FORCE_INFO_REFRESH_TOP_ROWS = int(os.getenv("FORCE_INFO_REFRESH_TOP_ROWS", "20"))
GROUP_ENRICH_INTERVAL_SECONDS = int(os.getenv("GROUP_ENRICH_INTERVAL_SECONDS", "300"))
DOC_TIMEOUT_SECONDS = float(os.getenv("DOC_TIMEOUT_SECONDS", "1.5"))
NAV_TIMEOUT_SECONDS = float(os.getenv("NAV_TIMEOUT_SECONDS", "0.8"))
BASE_BATCH_TIMEOUT_SECONDS = float(os.getenv("BASE_BATCH_TIMEOUT_SECONDS", "120"))
MANUAL_REFRESH_TIMEOUT_SECONDS = int(os.getenv("MANUAL_REFRESH_TIMEOUT_SECONDS", "900"))
MANUAL_REFRESH_PAGE_SIZE = int(os.getenv("MANUAL_REFRESH_PAGE_SIZE", "300"))
COLD_START_DOC_ENRICH_LIMIT = int(os.getenv("COLD_START_DOC_ENRICH_LIMIT", "40"))
GROUP_CHECK_TIMEOUT_SECONDS = float(os.getenv("GROUP_CHECK_TIMEOUT_SECONDS", "8"))
NAV_LINK_LIMIT = int(os.getenv("NAV_LINK_LIMIT", "4"))
STATUS_KP_PROPERTY_KEY = os.getenv(
    "STATUS_KP_PROPERTY_KEY",
    "e1c7a0e4-4f8d-11f0-8d50-bc97e15eb091",
)
RENDER_API_KEY = os.getenv("RENDER_API_KEY", "")
RENDER_SERVICE_NAME = os.getenv("RENDER_SERVICE_NAME", "onec-kp-realtime")
RENDER_STATUS_TTL = int(os.getenv("RENDER_STATUS_TTL", "30"))
STATUS_RULES_TEXT_ENV = os.getenv("STATUS_RULES_TEXT", "").strip()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
GITHUB_REPO = os.getenv("GITHUB_REPO", "pavel9619229-cmyk/KP").strip()
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main").strip()
GITHUB_RUNTIME_BRANCH = os.getenv("GITHUB_RUNTIME_BRANCH", GITHUB_BRANCH).strip() or GITHUB_BRANCH
GITHUB_RUNTIME_CURRENT_PATH = os.getenv("GITHUB_RUNTIME_CURRENT_PATH", "data/kp_runtime_current.json").strip()
GITHUB_RUNTIME_VERSIONS_DIR = os.getenv("GITHUB_RUNTIME_VERSIONS_DIR", "data/runtime_versions").strip().strip("/")
GITHUB_RULES_PATH = os.getenv("GITHUB_RULES_PATH", "data/status_rules.json").strip()
ACCESS_RIGHTS_FILE = os.getenv("ACCESS_RIGHTS_FILE", "data/access_rights.json").strip()
ADMIN_USER = os.getenv("ADMIN_USER", "admin").strip()
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "").strip()
ADMIN_PASSWORD_HASH = os.getenv("ADMIN_PASSWORD_HASH", "").strip().lower()
_DEFAULT_ADMIN_SESSION_SECRET = "change-me-admin-secret"
_raw_admin_session_secret = os.getenv("ADMIN_SESSION_SECRET", _DEFAULT_ADMIN_SESSION_SECRET).strip()
_raw_user_session_secret = os.getenv("USER_SESSION_SECRET", "").strip()

ADMIN_SESSION_SECRET = _raw_admin_session_secret or _DEFAULT_ADMIN_SESSION_SECRET
# User tokens derive from admin secret for backwards-compatibility.
# Set USER_SESSION_SECRET env var to use an independent secret.
USER_SESSION_SECRET = _raw_user_session_secret or (ADMIN_SESSION_SECRET + ":user")

ADMIN_SESSION_SECRET_IS_EPHEMERAL = ADMIN_SESSION_SECRET == _DEFAULT_ADMIN_SESSION_SECRET
USER_SESSION_SECRET_IS_EPHEMERAL = False

ADMIN_SESSION_TTL_SECONDS = int(os.getenv("ADMIN_SESSION_TTL_SECONDS", "43200"))
ADMIN_SESSION_COOKIE = "kp_admin_session"
USER_SESSION_TTL_SECONDS = int(os.getenv("USER_SESSION_TTL_SECONDS", "43200"))
USER_SESSION_COOKIE = "kp_user_session"
APP_COMMIT_SHA = (
    os.getenv("RENDER_GIT_COMMIT", "").strip()
    or os.getenv("GIT_COMMIT", "").strip()
    or os.getenv("COMMIT_SHA", "").strip()
)
APP_BRANCH = (
    os.getenv("RENDER_GIT_BRANCH", "").strip()
    or os.getenv("GIT_BRANCH", "").strip()
)

TARGET_START = datetime(2026, 3, 1, 0, 0, 0)
TARGET_END = datetime(2026, 4, 30, 23, 59, 59)

LIGHT_SELECT_FIELDS = [
    "Number",
    "Date",
    "Статус",
    "ДополнительныеРеквизиты",
    "Комментарий",
]

_cached_rows = []
_cached_fp = ""
_last_refresh = None
_last_refresh_error = None
_last_comment_refresh = None
_last_comment_refresh_error = None
_last_group_enrich = None

_TZ_MSK = timezone(timedelta(hours=3))
_app_started_at = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")
_customer_name_cache = {}
_additional_info_cache = {}
_status_kp_value_cache = {}
_status_kp_catalog_value_key_cache = {}
_manager_filled_cache = {}
_manager_name_cache = {}
_product_specified_cache = {}
_price_filled_cache = {}
_kp_sent_cache = {}
_receipt_confirmed_cache = {}
_edo_sent_cache = {}
_rejected_cache = {}
_problem_cache = {}
_shipment_pending_cache = {}
_refresh_run_lock = threading.Lock()
_refresh_lock = threading.Lock()
_partial_refresh_lock = threading.Lock()
_render_status_cache: dict = {"status": None, "updatedAt": None}
_last_cache_push: Optional[datetime] = None
_last_confirmed_runtime_sync_check = 0.0

# Persistent order→KP mapping cache.  Survives incomplete orders scans so
# payment matching still works even when Document_ЗаказКлиента times out.
ORDER_CACHE_FILE = os.getenv("ORDER_CACHE_FILE", "data/kp_order_cache.json")
_order_to_kp_cache: dict[str, dict] = {}   # order_ref -> {"kp": kp_ref, "num": order_number}
_order_cache_loaded: bool = False
_order_cache_lock = threading.Lock()

# Persistent payment seed cache. Manually seeded payments that scans may miss
# (e.g. early-numbered docs like ДС-76 that live near the start of the DB).
PAYMENT_SEED_FILE = os.getenv("PAYMENT_SEED_FILE", "data/kp_payment_seed.json")
_payment_seed: list[dict] = []   # [{"payShort": "76", "purposeNums": ["218"], "purpose": "..."}]
_payment_seed_loaded: bool = False
_payment_seed_lock = threading.Lock()
CACHE_PUSH_MIN_INTERVAL = 3600  # push runtime cache to GitHub at most once per hour
CONFIRMED_RUNTIME_SYNC_TTL_SECONDS = int(os.getenv("CONFIRMED_RUNTIME_SYNC_TTL_SECONDS", "300"))
_render_status_lock = threading.Lock()
_status_rules_lock = threading.Lock()
_runtime_write_guard_lock = threading.Lock()
_confirmed_runtime_sync_lock = threading.Lock()
_enrich_cursor = 0
_partial_refresh_cursor = 0
_manual_refresh_state_lock = threading.Lock()
_manual_refresh_state: dict = {
    "running": False,
    "requestedAt": None,
    "requestedBy": None,
    "requestedFrom": None,
    "startedAt": None,
    "finishedAt": None,
    "lastOk": None,
    "lastError": None,
}


def _manual_refresh_snapshot() -> dict:
    with _manual_refresh_state_lock:
        state = dict(_manual_refresh_state)
    state["rows"] = len(_cached_rows)
    state["lastRefresh"] = _last_refresh
    state["lastRefreshError"] = _last_refresh_error
    return state


def _set_manual_refresh_state(**updates: object) -> None:
    with _manual_refresh_state_lock:
        _manual_refresh_state.update(updates)

DEFAULT_STATUS_RULES_TEXT = """# Формат 1 (простой):
# статус СТАТУС устанавливается, если Поле - ДА, Поле - НЕТ
#
# Поля:
# Проблема, Отказ, Накладная создана, Оплата получена,
# В ЭДО отправлено, Отгрузить, Клиент КП увидел, КП отправлено,
# Клиент заполнен, Менеджер заполнен, Товар указан, Цена указана
#
# Формат 2 (технический, тоже поддерживается на фронтенде):
# condition AND condition -> STATUS

статус ПРОБЛЕМА устанавливается, если Проблема - ДА
статус ОТКАЗ устанавливается, если Отказ - ДА
статус ОТГРУЖЕНО, ОФОРМЛЕНО И ОПЛАЧЕНО устанавливается, если Накладная создана - ДА, Оплата получена - ДА, В ЭДО отправлено - ДА
статус ЖДЕМ ОПЛАТУ устанавливается, если Накладная создана - ДА, В ЭДО отправлено - ДА, Оплата получена - НЕТ
статус ОТПРАВИТЬ В ЭДО устанавливается, если Накладная создана - ДА, В ЭДО отправлено - НЕТ
статус ОТГРУЗИТЬ устанавливается, если Отгрузить - ДА
статус КЛИЕНТ ДУМАЕТ устанавливается, если Клиент КП увидел - ДА
статус ПРОВЕРИТЬ ПОЛУЧЕНИЕ КП устанавливается, если КП отправлено - ДА
статус ОБРАБОТАТЬ устанавливается, если выполнено хотя бы одно из условий: Клиент заполнен - НЕТ, Менеджер заполнен - НЕТ, Цена в первой строке товара указана - НЕТ
статус ОТПРАВИТЬ КЛИЕНТУ устанавливается, если Клиент заполнен - ДА, Менеджер заполнен - ДА, Товар указан - ДА
"""

ZERO_GUID = "00000000-0000-0000-0000-000000000000"
UNKNOWN_MANAGER_NAME = os.getenv("UNKNOWN_MANAGER_NAME", "НЕ ОПРЕДЕЛЕН")
UNKNOWN_CUSTOMER_NAME = os.getenv("UNKNOWN_CUSTOMER_NAME", "НЕ ОПРЕДЕЛЕН")
NEW_REQUEST_STATUS_TEXT = os.getenv("NEW_REQUEST_STATUS_TEXT", "1. НОВЫЙ ЗАПРОС")

STORAGE_DEFAULTS = {
    "statusKp": "",
    "managerName": UNKNOWN_MANAGER_NAME,
    "managerFilled": None,
    "productSpecified": None,
    "priceFilled": None,
    "kpSent": None,
    "receiptConfirmed": None,
    "edoSent": None,
    "rejected": None,
    "problem": None,
    "shipmentPending": None,
    "invoiceCreated": None,
    "paymentReceived": None,
    "statusHash": "",
}


_log_buffer: collections.deque = collections.deque(maxlen=200)


def log(message: str) -> None:
    line = f"[{datetime.now(_TZ_MSK).strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    _log_buffer.append(line)
    print(line, flush=True)


def _build_headers() -> dict:
    creds = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode("utf-8")).decode("ascii")
    return {
        "Authorization": f"Basic {creds}",
        "Accept": "application/json",
    }


class NewRequestPayload(BaseModel):
    requestText: str = Field(min_length=3, max_length=8000)


class StatusRulesPayload(BaseModel):
    rulesText: str = Field(min_length=1, max_length=40000)


class AdminLoginPayload(BaseModel):
    username: str = Field(min_length=1, max_length=120)
    password: str = Field(min_length=1, max_length=200)


class UserLoginPayload(BaseModel):
    username: str = Field(min_length=1, max_length=200)
    password: str = Field(min_length=1, max_length=200)


class AccessRightsPayload(BaseModel):
    users: list[dict] = Field(default_factory=list)


def _status_rules_path() -> Path:
    return Path(STATUS_RULES_FILE)


def _access_rights_path() -> Path:
    return Path(ACCESS_RIGHTS_FILE)


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _clear_session_cookies(response: JSONResponse | RedirectResponse) -> None:
    # Match cookie attributes to maximize compatibility on mobile browsers.
    response.delete_cookie(
        key=ADMIN_SESSION_COOKIE,
        path="/",
        secure=True,
        httponly=True,
        samesite="lax",
    )
    response.delete_cookie(
        key=USER_SESSION_COOKIE,
        path="/",
        secure=True,
        httponly=True,
        samesite="lax",
    )


def _admin_password_ok(password: str) -> bool:
    candidate_hash = _sha256_hex(password)
    if ADMIN_PASSWORD_HASH:
        return hmac.compare_digest(candidate_hash, ADMIN_PASSWORD_HASH)
    if ADMIN_PASSWORD:
        return hmac.compare_digest(str(password or ""), ADMIN_PASSWORD)
    return False


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64url_decode(data: str) -> bytes:
    raw = str(data or "")
    padding = "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode((raw + padding).encode("ascii"))


def _sign_admin_payload(payload_b64: str) -> str:
    signature = hmac.new(
        ADMIN_SESSION_SECRET.encode("utf-8"),
        payload_b64.encode("ascii"),
        hashlib.sha256,
    ).digest()
    return _b64url_encode(signature)


def _issue_admin_token(username: str) -> str:
    payload = {
        "u": username,
        "exp": int(time.time()) + max(300, ADMIN_SESSION_TTL_SECONDS),
    }
    payload_b64 = _b64url_encode(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    signature = _sign_admin_payload(payload_b64)
    return f"{payload_b64}.{signature}"


def _sign_user_payload(payload_b64: str) -> str:
    signature = hmac.new(
        USER_SESSION_SECRET.encode("utf-8"),
        payload_b64.encode("ascii"),
        hashlib.sha256,
    ).digest()
    return _b64url_encode(signature)


def _issue_user_token(username: str) -> str:
    payload = {
        "u": username,
        "exp": int(time.time()) + max(300, USER_SESSION_TTL_SECONDS),
    }
    payload_b64 = _b64url_encode(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    signature = _sign_user_payload(payload_b64)
    return f"{payload_b64}.{signature}"


def _read_admin_token(token: str) -> dict | None:
    value = str(token or "").strip()
    if "." not in value:
        return None
    payload_b64, signature = value.split(".", 1)
    expected = _sign_admin_payload(payload_b64)
    if not hmac.compare_digest(signature, expected):
        return None
    try:
        payload_raw = _b64url_decode(payload_b64).decode("utf-8")
        payload = json.loads(payload_raw)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if int(payload.get("exp") or 0) < int(time.time()):
        return None
    username = str(payload.get("u") or "").strip()
    if not username:
        return None
    return payload


def _read_user_token(token: str) -> dict | None:
    value = str(token or "").strip()
    if "." not in value:
        return None
    payload_b64, signature = value.split(".", 1)
    expected = _sign_user_payload(payload_b64)
    if not hmac.compare_digest(signature, expected):
        return None
    try:
        payload_raw = _b64url_decode(payload_b64).decode("utf-8")
        payload = json.loads(payload_raw)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if int(payload.get("exp") or 0) < int(time.time()):
        return None
    username = str(payload.get("u") or "").strip()
    if not username:
        return None
    return payload


def _get_admin_username(request: Request) -> str | None:
    token = request.cookies.get(ADMIN_SESSION_COOKIE)
    payload = _read_admin_token(token or "")
    if not payload:
        return None
    return str(payload.get("u") or "").strip() or None


def _require_admin(request: Request) -> str:
    username = _get_admin_username(request)
    if not username:
        raise HTTPException(status_code=401, detail="Admin auth required")
    return username


def _normalize_username(value: str) -> str:
    return str(value or "").strip().casefold()


def _normalize_manager_name_for_acl(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).casefold().replace("ё", "е")


def _row_manager_name(row: dict) -> str:
    manager = str(row.get("managerName") or row.get("manager") or row.get("Менеджер") or "").strip()
    return manager or UNKNOWN_MANAGER_NAME


def _find_access_user(username: str) -> dict | None:
    wanted = _normalize_username(username)
    if not wanted:
        return None
    rights = load_access_rights()
    for item in rights.get("users", []):
        if not isinstance(item, dict):
            continue
        current = _normalize_username(item.get("username") or "")
        if current == wanted:
            return item
    return None


def _resolve_effective_user(username: str) -> dict | None:
    uname = str(username or "").strip()
    if not uname:
        return None
    if _normalize_username(uname) == _normalize_username(ADMIN_USER):
        return {
            "username": uname,
            "role": "admin",
            "allowedManagers": "*",
        }

    user = _find_access_user(uname)
    if not user:
        return None

    role = str(user.get("role") or "manager").strip().lower()
    if role not in {"admin", "manager"}:
        role = "manager"
    allowed = user.get("allowedManagers")
    if allowed == "*":
        allowed_managers = "*"
    elif isinstance(allowed, list):
        allowed_managers = [str(v).strip() for v in allowed if str(v).strip()]
    else:
        allowed_managers = []
    return {
        "username": str(user.get("username") or uname).strip(),
        "role": role,
        "allowedManagers": allowed_managers,
    }


def _is_valid_sha256_hex(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-f]{64}", str(value or "").strip().lower()))


def _user_password_ok(username: str, password: str) -> bool:
    uname = str(username or "").strip()
    pwd = str(password or "")

    if _normalize_username(uname) == _normalize_username(ADMIN_USER):
        return _admin_password_ok(pwd)

    user = _find_access_user(uname)
    if not user:
        return False

    password_hash = str(user.get("passwordHash") or "").strip().lower()
    if not _is_valid_sha256_hex(password_hash):
        return False
    return hmac.compare_digest(_sha256_hex(pwd), password_hash)


def _get_user_from_request(request: Request) -> dict:
    # Prefer explicit admin session cookie when present.
    # This prevents role downgrade if browser still has an old manager user-session.
    admin_token = request.cookies.get(ADMIN_SESSION_COOKIE)
    admin_payload = _read_admin_token(admin_token or "")
    if admin_payload:
        username = str(admin_payload.get("u") or "").strip()
        if username:
            return {
                "username": username,
                "role": "admin",
                "allowedManagers": "*",
            }

    # Then try regular user session cookie.
    token = request.cookies.get(USER_SESSION_COOKIE)
    payload = _read_user_token(token or "")
    if payload:
        username = str(payload.get("u") or "").strip()
        user = _resolve_effective_user(username)
        if user:
            return user

    raise HTTPException(status_code=401, detail="Login required")


def _get_user_from_websocket(websocket: WebSocket) -> dict | None:
    token = websocket.cookies.get(USER_SESSION_COOKIE)
    payload = _read_user_token(token or "")
    if not payload:
        return None
    username = str(payload.get("u") or "").strip()
    return _resolve_effective_user(username)


def _filter_rows_for_user(rows: list[dict], user: dict) -> list[dict]:
    role = str(user.get("role") or "manager").lower()
    allowed = user.get("allowedManagers")
    if role == "admin" or allowed == "*":
        return list(rows)
    if not isinstance(allowed, list) or not allowed:
        return []

    allowed_norm = {_normalize_manager_name_for_acl(v) for v in allowed}
    filtered: list[dict] = []
    for row in rows:
        manager_name = _row_manager_name(row)
        if _normalize_manager_name_for_acl(manager_name) in allowed_norm:
            filtered.append(row)
    return filtered


def load_access_rights() -> dict:
    path = _access_rights_path()
    if not path.exists():
        return {"users": [], "updatedAt": None}
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        users = payload.get("users")
        if not isinstance(users, list):
            users = []
        updated_at = str(payload.get("updatedAt") or "") or None
        return {"users": users, "updatedAt": updated_at}
    except Exception as exc:
        log(f"access rights read failed: {exc}")
        return {"users": [], "updatedAt": None}


def save_access_rights(users: list[dict]) -> dict:
    cleaned_users: list[dict] = []
    for item in list(users or []):
        if not isinstance(item, dict):
            continue
        username = str(item.get("username") or "").strip()
        role = str(item.get("role") or "manager").strip().lower()
        if not username:
            continue
        if role not in {"admin", "manager"}:
            role = "manager"
        allowed = item.get("allowedManagers")
        if allowed == "*":
            allowed_managers = "*"
        elif isinstance(allowed, list):
            values = sorted({str(v).strip() for v in allowed if str(v).strip()})
            allowed_managers = values
        else:
            allowed_managers = []
        password_hash = str(item.get("passwordHash") or "").strip().lower()
        if password_hash and not _is_valid_sha256_hex(password_hash):
            password_hash = ""
        cleaned_users.append(
            {
                "username": username,
                "role": role,
                "allowedManagers": allowed_managers,
                "passwordHash": password_hash,
            }
        )

    payload = {
        "users": cleaned_users,
        "updatedAt": datetime.now().isoformat(),
    }
    path = _access_rights_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return payload


def _effective_default_rules() -> str:
    return STATUS_RULES_TEXT_ENV or DEFAULT_STATUS_RULES_TEXT


def load_status_rules_text() -> str:
    path = _status_rules_path()
    if not path.exists():
        return _effective_default_rules()

    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        rules_text = str(payload.get("rulesText") or "").strip()
        return rules_text or _effective_default_rules()
    except Exception as exc:
        log(f"status rules read failed, using default: {exc}")
        return _effective_default_rules()


def _push_rules_to_github(rules_text: str, updated_at: str) -> None:
    """Push data/status_rules.json to GitHub via Contents API so the file
    survives the next Render deploy. Runs in a background thread."""
    if not GITHUB_TOKEN or not GITHUB_REPO:
        log("GitHub push skipped: GITHUB_TOKEN or GITHUB_REPO not set")
        return

    payload = {
        "rulesText": rules_text,
        "updatedAt": updated_at,
    }
    content_bytes = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    content_b64 = base64.b64encode(content_bytes).decode("ascii")

    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_RULES_PATH}"
    gh_headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    # Get current file SHA (required for update)
    current_sha = ""
    try:
        resp = requests.get(
            api_url,
            headers=gh_headers,
            params={"ref": GITHUB_BRANCH},
            timeout=10,
        )
        if resp.status_code == 200:
            current_sha = str(resp.json().get("sha") or "")
    except Exception as exc:
        log(f"GitHub SHA fetch failed: {exc}")

    body: dict = {
        "message": f"Auto-sync status_rules.json ({updated_at}) [skip ci]",
        "content": content_b64,
        "branch": GITHUB_BRANCH,
    }
    if current_sha:
        body["sha"] = current_sha

    try:
        resp = requests.put(api_url, headers=gh_headers, json=body, timeout=15)
        if resp.status_code in (200, 201):
            log(f"GitHub push OK: {GITHUB_REPO}/{GITHUB_RULES_PATH}")
        else:
            log(f"GitHub push failed: HTTP {resp.status_code}: {resp.text[:300]}")
    except Exception as exc:
        log(f"GitHub push error: {exc}")


def _push_access_rights_to_github(payload: dict) -> None:
    """Push data/access_rights.json to GitHub via Contents API so rights
    survive the next Render deploy. Runs in a background thread."""
    if not GITHUB_TOKEN or not GITHUB_REPO:
        log("GitHub push skipped for access rights: GITHUB_TOKEN or GITHUB_REPO not set")
        return

    content_bytes = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    content_b64 = base64.b64encode(content_bytes).decode("ascii")

    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{ACCESS_RIGHTS_FILE}"
    gh_headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    current_sha = ""
    try:
        resp = requests.get(
            api_url,
            headers=gh_headers,
            params={"ref": GITHUB_BRANCH},
            timeout=10,
        )
        if resp.status_code == 200:
            current_sha = str(resp.json().get("sha") or "")
    except Exception as exc:
        log(f"GitHub SHA fetch failed for access rights: {exc}")

    updated_at = str(payload.get("updatedAt") or datetime.now().isoformat())
    body: dict = {
        "message": f"Auto-sync access_rights.json ({updated_at}) [skip ci]",
        "content": content_b64,
        "branch": GITHUB_BRANCH,
    }
    if current_sha:
        body["sha"] = current_sha

    try:
        resp = requests.put(api_url, headers=gh_headers, json=body, timeout=15)
        if resp.status_code in (200, 201):
            log(f"GitHub push OK: {GITHUB_REPO}/{ACCESS_RIGHTS_FILE}")
        else:
            log(f"GitHub push failed for access rights: HTTP {resp.status_code}: {resp.text[:300]}")
    except Exception as exc:
        log(f"GitHub push error for access rights: {exc}")


def _push_runtime_cache_to_github(rows: list, meta_payload: dict | None = None) -> None:
    """Push kp_runtime_cache.json + kp_runtime_meta.json to GitHub so enriched
    data survives the next Render deploy. Throttled to once per hour. [skip ci]."""
    _push_runtime_cache_to_github_sync(rows, meta_payload)


def _push_runtime_cache_to_github_sync(
    rows: list,
    meta_payload: dict | None = None,
    *,
    force: bool = False,
) -> bool:
    """Synchronously push kp_runtime_cache.json + kp_runtime_meta.json to GitHub.

    Returns True only when both files are updated successfully.
    """
    global _last_cache_push

    if not GITHUB_TOKEN or not GITHUB_REPO:
        log("GitHub cache push skipped: GITHUB_TOKEN or GITHUB_REPO not set")
        return False

    now = datetime.now()
    if not force and _last_cache_push and (now - _last_cache_push).total_seconds() < CACHE_PUSH_MIN_INTERVAL:
        return False

    gh_headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    def _push_one(file_path: str, content_bytes: bytes, label: str) -> bool:
        api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_path}"
        content_b64 = base64.b64encode(content_bytes).decode("ascii")
        current_sha = ""
        try:
            r = requests.get(api_url, headers=gh_headers, params={"ref": GITHUB_BRANCH}, timeout=10)
            if r.status_code == 200:
                current_sha = str(r.json().get("sha") or "")
        except Exception as exc:
            log(f"GitHub SHA fetch failed ({label}): {exc}")
        ts = now.strftime("%Y-%m-%dT%H:%M:%S")
        body: dict = {
            "message": f"Auto-sync {label} ({ts}) [skip ci]",
            "content": content_b64,
            "branch": GITHUB_BRANCH,
        }
        if current_sha:
            body["sha"] = current_sha
        try:
            r = requests.put(api_url, headers=gh_headers, json=body, timeout=20)
            if r.status_code in (200, 201):
                log(f"GitHub cache push OK: {label}")
                return True
            log(f"GitHub cache push failed ({label}): HTTP {r.status_code}: {r.text[:200]}")
            return False
        except Exception as exc:
            log(f"GitHub cache push error ({label}): {exc}")
            return False

    try:
        cache_bytes = json.dumps(rows, ensure_ascii=False, indent=2).encode("utf-8")
        cache_ok = _push_one("data/kp_runtime_cache.json", cache_bytes, "kp_runtime_cache.json")
        meta = meta_payload or {"generatedAt": now.isoformat(), "rowCount": len(rows)}
        meta_bytes = json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8")
        meta_ok = _push_one("data/kp_runtime_meta.json", meta_bytes, "kp_runtime_meta.json")
        if cache_ok and meta_ok:
            _last_cache_push = now
            return True
        return False
    except Exception as exc:
        log(f"GitHub cache push unexpected error: {exc}")
        return False


def _write_runtime_snapshot_files(rows: list, meta_payload: dict) -> None:
    runtime_path = Path(RUNTIME_DATA_FILE)
    runtime_meta_path = Path(RUNTIME_META_FILE)
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_meta_path.parent.mkdir(parents=True, exist_ok=True)

    normalized_rows = []
    for row in list(rows or []):
        copied = dict(row)
        apply_storage_defaults(copied)
        normalized_rows.append(copied)

    with runtime_path.open("w", encoding="utf-8") as f:
        json.dump(normalized_rows, f, ensure_ascii=False, indent=2)
    with runtime_meta_path.open("w", encoding="utf-8") as f:
        json.dump(meta_payload or {}, f, ensure_ascii=False, indent=2)


def _sync_runtime_cache_via_github_or_raise() -> tuple[list, dict]:
    rows = load_rows_from_path(Path(RUNTIME_DATA_FILE))
    if not rows:
        raise RuntimeError("runtime snapshot is empty after 1C refresh")

    meta = _read_runtime_meta()
    if not meta:
        raise RuntimeError("runtime metadata is missing after 1C refresh")

    pushed = _push_runtime_cache_to_github_sync(rows, meta, force=True)
    if not pushed:
        raise RuntimeError("GitHub runtime sync failed")

    github_rows = _load_runtime_rows_from_github()
    if not github_rows:
        raise RuntimeError("GitHub runtime readback returned no rows")

    github_meta = _load_runtime_meta_from_github()
    if not github_meta:
        raise RuntimeError("GitHub runtime meta readback failed")

    _write_runtime_snapshot_files(github_rows, github_meta)
    return github_rows, github_meta


def _load_runtime_rows_from_github() -> list:
    if not GITHUB_REPO:
        return []

    gh_headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if GITHUB_TOKEN:
        gh_headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/data/kp_runtime_cache.json"
    try:
        resp = requests.get(api_url, headers=gh_headers, params={"ref": GITHUB_BRANCH}, timeout=20)
        if resp.status_code == 200:
            payload = resp.json()
            content_b64 = str(payload.get("content") or "").replace("\n", "")
            if content_b64:
                decoded = base64.b64decode(content_b64.encode("ascii")).decode("utf-8")
                rows = json.loads(decoded)
                if isinstance(rows, list):
                    for row in rows:
                        apply_storage_defaults(row)
                    rows.sort(key=lambda x: x.get("createdAt", ""), reverse=True)
                    return rows
    except Exception as exc:
        log(f"github runtime cache API fetch failed: {exc}")

    raw_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/data/kp_runtime_cache.json"
    try:
        resp = requests.get(raw_url, timeout=20)
        if resp.status_code != 200:
            return []
        rows = resp.json()
        if not isinstance(rows, list):
            return []
        for row in rows:
            apply_storage_defaults(row)
        rows.sort(key=lambda x: x.get("createdAt", ""), reverse=True)
        return rows
    except Exception as exc:
        log(f"github runtime cache RAW fetch failed: {exc}")
        return []


def _recover_runtime_cache_from_github_if_needed(reason: str) -> bool:
    return _sync_confirmed_runtime_cache_from_github_if_needed(reason, force=not bool(_cached_rows))


def save_status_rules_text(rules_text: str) -> None:
    clean_text = str(rules_text or "").strip()
    if not clean_text:
        raise ValueError("rulesText must not be empty")

    path = _status_rules_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "rulesText": clean_text,
        "updatedAt": datetime.now().isoformat(),
    }
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


DEFAULT_FALLBACK_STATUS = "ОБРАБОТАТЬ"
RULE_FIELD_ALIASES = {
    "проблема": "problem",
    "отказ": "rejected",
    "накладнаясоздана": "invoiceCreated",
    "оплатаполучена": "paymentReceived",
    "вэдоотправлено": "edoSent",
    "отгрузить": "shipmentPending",
    "клиенткпувидел": "receiptConfirmed",
    "кпотправлено": "kpSent",
    "клиентзаполнен": "clientFilled",
    "менеджерзаполнен": "managerFilled",
    "товаруказан": "productSpecified",
    "ценауказана": "priceFilled",
    "ценавпервойстрокетоварауказана": "priceFilled",
}


def _normalize_rule_field_name(value: str) -> str:
    normalized = str(value or "").strip().lower().replace("ё", "е")
    return re.sub(r"[^a-zа-я0-9]", "", normalized)


def _parse_bool_token(value: str) -> bool | None:
    v = str(value or "").strip().lower()
    if v in {"true", "1", "yes", "y", "да"}:
        return True
    if v in {"false", "0", "no", "n", "нет"}:
        return False
    return None


def _parse_condition_token(token: str) -> dict | None:
    raw = str(token or "").strip()
    human_match = re.match(r"^(.+?)\s*[-:=]\s*(.+)$", raw, flags=re.IGNORECASE)
    if human_match:
        field_name = _normalize_rule_field_name(human_match.group(1))
        field = RULE_FIELD_ALIASES.get(field_name)
        if not field:
            return None
        bool_value = _parse_bool_token(human_match.group(2))
        if bool_value is None:
            return None
        return {"field": field, "operator": "is_true" if bool_value else "is_false"}

    tech_match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s*(=|!=)\s*(.+)$", raw)
    if not tech_match:
        return None
    field = RULE_FIELD_ALIASES.get(_normalize_rule_field_name(tech_match.group(1)))
    if not field:
        return None
    bool_value = _parse_bool_token(tech_match.group(3))
    if bool_value is None:
        return None
    if tech_match.group(2) == "=" and bool_value:
        operator = "is_true"
    elif tech_match.group(2) == "=" and not bool_value:
        operator = "is_false"
    elif tech_match.group(2) == "!=" and bool_value:
        operator = "is_not_true"
    else:
        operator = "is_not_false"
    return {"field": field, "operator": operator}


def _parse_status_rules_text(rules_text: str) -> list[dict]:
    rules: list[dict] = []
    for raw_line in str(rules_text or "").replace("\r\n", "\n").split("\n"):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        match = re.match(r"^статус\s+(.+?)\s+устанавливается,\s*если\s+(.+)$", line, flags=re.IGNORECASE)
        if match:
            label = str(match.group(1) or "").strip()
            left = str(match.group(2) or "").strip()
            if not label or not left:
                continue

            match_mode = "all"
            any_of = re.match(r"^(?:(?:выполнено|выполняется)\s+)?хотя\s*бы\s*одно\s+из\s+условий\s*(?::|-)?\s*(.+)$", left, flags=re.IGNORECASE)
            if any_of:
                match_mode = "any"
                left = str(any_of.group(1) or "").strip()

            splitter = r"\s*,\s*|\s+(?:OR|ИЛИ|AND|И)\s+" if match_mode == "any" else r"\s*,\s*|\s+(?:AND|И)\s+"
            tokens = [x.strip() for x in re.split(splitter, left, flags=re.IGNORECASE) if x.strip()]
            conditions = []
            for token in tokens:
                cond = _parse_condition_token(token)
                if cond is not None:
                    conditions.append(cond)
            if conditions:
                rules.append({"label": label, "conditions": conditions, "matchMode": match_mode})
            continue

        if "->" in line:
            parts = line.split("->")
            left = parts[0].strip()
            label = "->".join(parts[1:]).strip()
            tokens = [x.strip() for x in re.split(r"\s+(?:AND|И)\s+", left, flags=re.IGNORECASE) if x.strip()]
            conditions = []
            for token in tokens:
                cond = _parse_condition_token(token)
                if cond is not None:
                    conditions.append(cond)
            if label and conditions:
                rules.append({"label": label, "conditions": conditions, "matchMode": "all"})

    return rules


def _matches_condition(facts: dict, condition: dict) -> bool:
    value = facts.get(condition.get("field"))
    operator = condition.get("operator")
    if operator == "is_true":
        return value is True
    if operator == "is_false":
        return value is False
    if operator == "is_not_true":
        return value is not True
    if operator == "is_not_false":
        return value is not False
    return False


def _compute_status_for_row(row: dict, rules: list[dict]) -> str:
    facts = {
        "problem": row.get("problem"),
        "rejected": row.get("rejected"),
        "invoiceCreated": row.get("invoiceCreated"),
        "paymentReceived": row.get("paymentReceived"),
        "edoSent": row.get("edoSent"),
        "shipmentPending": row.get("shipmentPending"),
        "receiptConfirmed": row.get("receiptConfirmed"),
        "kpSent": row.get("kpSent"),
        "clientFilled": row.get("clientFilled"),
        "managerFilled": row.get("managerFilled"),
        "productSpecified": row.get("productSpecified"),
        "priceFilled": row.get("priceFilled"),
    }
    for rule in rules:
        conditions = rule.get("conditions") or []
        if not conditions:
            continue
        match_mode = "any" if str(rule.get("matchMode") or "").lower() == "any" else "all"
        is_matched = any(_matches_condition(facts, c) for c in conditions) if match_mode == "any" else all(_matches_condition(facts, c) for c in conditions)
        if is_matched:
            return str(rule.get("label") or "").strip() or DEFAULT_FALLBACK_STATUS
    return DEFAULT_FALLBACK_STATUS


def _escape_odata_literal(value: str) -> str:
    return str(value).replace("'", "''")


def _normalize_human_name(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().lower().replace("ё", "е")


def _extract_customer_name_from_text(request_text: str) -> str:
    text = str(request_text or "").strip()
    if not text:
        return ""

    for line in text.splitlines():
        line_l = line.lower()
        if line_l.startswith("клиент:") or line_l.startswith("компания:"):
            candidate = line.split(":", 1)[1].strip()
            if candidate:
                return candidate

    legal_re = re.compile(
        r"\b(?:ООО|ИП|АО|ПАО|ЗАО)\s+[\"«]?[A-Za-zА-Яа-я0-9 .,_\-]{2,}[\"»]?",
        re.IGNORECASE,
    )
    match = legal_re.search(text)
    if match:
        return match.group(0).strip()

    return ""


def _find_catalog_item_key_by_description(
    entity_name: str,
    description: str,
    headers: dict,
) -> tuple[str, str]:
    desired = str(description or "").strip()
    if not desired:
        return "", ""

    params = {
        "$select": "Ref_Key,Description",
        "$filter": f"Description eq '{_escape_odata_literal(desired)}'",
        "$top": "1",
    }
    payload, error = _get_json_with_retry(
        f"{BASE}/{entity_name}",
        headers,
        params=params,
        timeout=6,
        retries=2,
    )
    if not error and isinstance(payload, dict):
        rows = payload.get("value", [])
        if rows:
            first = rows[0] if isinstance(rows[0], dict) else {}
            return str(first.get("Ref_Key") or "").strip(), str(first.get("Description") or "").strip()

    wanted_norm = _normalize_human_name(desired)
    best_ref = ""
    best_name = ""

    for skip in range(0, 2000, 200):
        payload, error = _get_json_with_retry(
            f"{BASE}/{entity_name}",
            headers,
            params={"$select": "Ref_Key,Description", "$top": "200", "$skip": str(skip)},
            timeout=8,
            retries=2,
        )
        if error or not isinstance(payload, dict):
            break

        rows = payload.get("value", [])
        if not rows:
            break

        for item in rows:
            if not isinstance(item, dict):
                continue
            name = str(item.get("Description") or "").strip()
            if not name:
                continue
            name_norm = _normalize_human_name(name)
            if name_norm == wanted_norm:
                return str(item.get("Ref_Key") or "").strip(), name
            if wanted_norm and (wanted_norm in name_norm or name_norm in wanted_norm):
                if not best_ref:
                    best_ref = str(item.get("Ref_Key") or "").strip()
                    best_name = name

    return best_ref, best_name


def _ensure_catalog_item_key_by_description(
    entity_name: str,
    description: str,
    headers: dict,
) -> tuple[str, str]:
    ref_key, name = _find_catalog_item_key_by_description(entity_name, description, headers)
    if ref_key:
        return ref_key, (name or description)

    response = requests.post(
        f"{BASE}/{entity_name}",
        headers={**headers, "Content-Type": "application/json; charset=utf-8"},
        json={"Description": description},
        timeout=20,
        verify=False,
    )
    if response.status_code not in (200, 201):
        return "", description

    try:
        payload = response.json() if isinstance(response.json(), dict) else {}
    except Exception:
        payload = {}

    return str(payload.get("Ref_Key") or "").strip(), str(payload.get("Description") or description).strip()


def _find_catalog_value_key_for_property(
    property_key: str,
    value_description: str,
    headers: dict,
) -> str:
    cache_key = f"{property_key.lower()}::{_normalize_human_name(value_description)}"
    if cache_key in _status_kp_catalog_value_key_cache:
        return _status_kp_catalog_value_key_cache[cache_key]

    desired = str(value_description or "").strip()
    if not desired:
        return ""

    payload, error = _get_json_with_retry(
        (
            f"{BASE}/Catalog_ЗначенияСвойствОбъектов"
            f"?$select=Ref_Key,Description,Owner_Key"
            f"&$filter=Owner_Key eq guid'{property_key}'"
            f" and Description eq '{_escape_odata_literal(desired)}'"
            f"&$top=1"
        ),
        headers,
        timeout=6,
        retries=2,
    )
    if not error and isinstance(payload, dict):
        rows = payload.get("value", [])
        if rows:
            ref_key = str((rows[0] or {}).get("Ref_Key") or "").strip()
            if ref_key:
                _status_kp_catalog_value_key_cache[cache_key] = ref_key
                return ref_key

    desired_norm = _normalize_human_name(desired)
    property_key_norm = str(property_key or "").strip().lower()
    for skip in range(0, 500, 100):
        payload, error = _get_json_with_retry(
            (
                f"{BASE}/Catalog_ЗначенияСвойствОбъектов"
                f"?$select=Ref_Key,Description,Owner_Key"
                f"&$filter=Owner_Key eq guid'{property_key}'"
                f"&$top=100&$skip={skip}"
            ),
            headers,
            timeout=8,
            retries=2,
        )
        if error or not isinstance(payload, dict):
            break

        rows = payload.get("value", [])
        if not rows:
            break

        for item in rows:
            if not isinstance(item, dict):
                continue
            if str(item.get("Owner_Key") or "").strip().lower() != property_key_norm:
                continue
            description = str(item.get("Description") or "").strip()
            if _normalize_human_name(description) != desired_norm:
                continue
            ref_key = str(item.get("Ref_Key") or "").strip()
            if ref_key:
                _status_kp_catalog_value_key_cache[cache_key] = ref_key
                return ref_key

    return ""


def _try_apply_status_kp_after_create(ref_key: str, headers: dict) -> bool:
    ref_key = str(ref_key or "").strip()
    if not ref_key:
        return False

    status_value_key = _find_catalog_value_key_for_property(
        STATUS_KP_PROPERTY_KEY,
        NEW_REQUEST_STATUS_TEXT,
        headers,
    )
    if not status_value_key:
        log(f"Status value '{NEW_REQUEST_STATUS_TEXT}' was not found for property {STATUS_KP_PROPERTY_KEY}")
        return False

    response = requests.patch(
        f"{BASE}/{ENTITY}(guid'{ref_key}')",
        headers={**headers, "Content-Type": "application/json; charset=utf-8"},
        json={
            "ДополнительныеРеквизиты": [
                {
                    "Ref_Key": ref_key,
                    "LineNumber": 1,
                    "Свойство_Key": STATUS_KP_PROPERTY_KEY,
                    "Значение": status_value_key,
                    "Значение_Type": "StandardODATA.Catalog_ЗначенияСвойствОбъектов",
                    "ТекстоваяСтрока": "",
                }
            ]
        },
        timeout=20,
        verify=False,
    )
    if response.status_code in (200, 204):
        return True

    log(
        "Status KP patch failed for "
        f"{ref_key}: HTTP {response.status_code}: {response.text[:300]}"
    )
    return False


def _try_prefix_status_in_comment(ref_key: str, request_text: str, headers: dict) -> bool:
    ref_key = str(ref_key or "").strip()
    if not ref_key:
        return False

    comment = f"{NEW_REQUEST_STATUS_TEXT}\n{request_text}" if request_text else NEW_REQUEST_STATUS_TEXT
    response = requests.patch(
        f"{BASE}/{ENTITY}(guid'{ref_key}')",
        headers={**headers, "Content-Type": "application/json; charset=utf-8"},
        json={"Комментарий": comment},
        timeout=20,
        verify=False,
    )
    if response.status_code in (200, 204):
        return True

    log(
        "Comment fallback patch failed for "
        f"{ref_key}: HTTP {response.status_code}: {response.text[:300]}"
    )
    return False


def _resolve_manager_key(headers: dict) -> str:
    manager_catalogs = [
        "Catalog_Пользователи",
        "Catalog_Сотрудники",
        "Catalog_СотрудникиОрганизаций",
    ]
    for entity_name in manager_catalogs:
        ref_key, _ = _find_catalog_item_key_by_description(entity_name, UNKNOWN_MANAGER_NAME, headers)
        if ref_key:
            return ref_key
    return ZERO_GUID


def _resolve_customer_for_new_request(request_text: str, headers: dict) -> tuple[str, str, str, str, bool]:
    partner_catalog = "Catalog_Партнеры"
    customer_catalog = "Catalog_Контрагенты"

    unknown_partner_key, unknown_partner_name = _ensure_catalog_item_key_by_description(
        partner_catalog,
        UNKNOWN_CUSTOMER_NAME,
        headers,
    )
    unknown_customer_key, unknown_customer_name = _ensure_catalog_item_key_by_description(
        customer_catalog,
        UNKNOWN_CUSTOMER_NAME,
        headers,
    )
    if not unknown_partner_name:
        unknown_partner_name = UNKNOWN_CUSTOMER_NAME
    if not unknown_partner_key:
        unknown_partner_key = ZERO_GUID
    if not unknown_customer_name:
        unknown_customer_name = UNKNOWN_CUSTOMER_NAME
    if not unknown_customer_key:
        unknown_customer_key = ZERO_GUID

    candidate_name = _extract_customer_name_from_text(request_text)
    if candidate_name:
        partner_key, partner_name = _find_catalog_item_key_by_description(partner_catalog, candidate_name, headers)
        customer_key, customer_name = _find_catalog_item_key_by_description(customer_catalog, candidate_name, headers)
        if partner_key or customer_key:
            resolved_name = partner_name or customer_name or candidate_name
            return (
                partner_key or unknown_partner_key,
                customer_key or unknown_customer_key,
                resolved_name,
                candidate_name,
                True,
            )

    return unknown_partner_key, unknown_customer_key, unknown_customer_name, UNKNOWN_CUSTOMER_NAME, False


def _create_kp_in_1c_from_request(request_text: str) -> dict:
    headers = _build_headers()
    normalized_request_text = str(request_text).replace("\x00", "").strip()
    client_key, customer_key, resolved_customer_name, requested_customer_name, recognized = _resolve_customer_for_new_request(normalized_request_text, headers)
    manager_key = _resolve_manager_key(headers)
    now = datetime.now()
    create_dt = now + timedelta(hours=2)
    now_iso = create_dt.replace(microsecond=0).isoformat()

    base_payload = {
        "Date": now_iso,
        "ДействуетДо": now_iso,
        "ЦенаВключаетНДС": True,
        "Комментарий": normalized_request_text,
        "Клиент_Key": client_key,
        "Контрагент_Key": customer_key,
        "Менеджер_Key": manager_key,
        "Товары": [],
    }

    post_headers = {
        **headers,
        "Content-Type": "application/json; charset=utf-8",
    }

    resp = requests.post(
        f"{BASE}/{ENTITY}",
        headers=post_headers,
        json=base_payload,
        timeout=20,
        verify=False,
    )

    if resp.status_code not in (200, 201):
        raise HTTPException(
            status_code=502,
            detail=f"1C create failed: HTTP {resp.status_code}: {resp.text[:500]}",
        )

    created = {}
    try:
        created = resp.json() if isinstance(resp.json(), dict) else {}
    except Exception:
        created = {}

    ref_key = str(created.get("Ref_Key") or "").strip()
    status_kp_applied = _try_apply_status_kp_after_create(ref_key, headers)
    status_marked_in_comment = False
    if not status_kp_applied:
        status_marked_in_comment = _try_prefix_status_in_comment(
            ref_key,
            normalized_request_text,
            headers,
        )

    return {
        "ok": True,
        "number": str(created.get("Number") or "").strip(),
        "refKey": ref_key,
        "resolvedCustomerName": resolved_customer_name,
        "requestedCustomerName": requested_customer_name,
        "recognizedCustomer": recognized,
        "manager": UNKNOWN_MANAGER_NAME,
        "statusKp": NEW_REQUEST_STATUS_TEXT,
        "statusKpApplied": status_kp_applied,
        "statusMarkedInComment": status_marked_in_comment,
    }


def strip_html(text: str) -> str:
    if not text:
        return ""
    text = unescape(text)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    return text


def first_line(*values: str) -> str:
    for raw in values:
        if not raw:
            continue
        cleaned = strip_html(str(raw)).replace("\r\n", "\n").replace("\r", "\n")
        for line in cleaned.split("\n"):
            line = line.strip()
            if line:
                return line
    return ""


def looks_like_product_hint(value: str) -> bool:
    line = first_line(value)
    if not line:
        return False

    upper = line.upper()
    non_product_markers = (
        "НОВЫЙ ЗАПРОС",
        "КЛИЕНТ ПОЛУЧИЛ КП",
        "ОБРАБОТАТЬ И ОТПРАВИТЬ КП",
        "КП ОТПРАВЛЕНО",
        "ПРОБЛЕМА",
        "ОТКАЗ",
    )
    if any(marker in upper for marker in non_product_markers):
        return False

    if "\t" in line:
        return True

    return bool(re.search(r"\d+[\.,]\d{2,3}", line))


def has_reject_marker(*values: str) -> bool:
    for value in values:
        text = str(value or "").upper()
        if "ОТКАЗ" in text:
            return True
    return False


def is_client_filled(customer_name: str | None) -> bool:
    name = str(customer_name or "").strip()
    if not name:
        return False

    normalized = name.casefold().replace("ё", "е")
    return normalized not in {"не определен", "неопределен"}


def is_manager_filled(manager_name: str | None) -> bool:
    name = str(manager_name or "").strip()
    if not name:
        return True

    normalized = name.casefold().replace("ё", "е")
    return normalized not in {"не определен", "неопределен"}


def apply_storage_defaults(row: dict) -> dict:
    if "customerName" not in row:
        row["customerName"] = ""
    if "managerName" not in row:
        row["managerName"] = UNKNOWN_MANAGER_NAME

    row["clientFilled"] = is_client_filled(row.get("customerName"))
    for key, default_value in STORAGE_DEFAULTS.items():
        if key not in row:
            row[key] = default_value
    return row


def apply_runtime_defaults(row: dict) -> dict:
    row["clientFilled"] = is_client_filled(row.get("customerName"))
    if not str(row.get("managerName") or "").strip():
        row["managerName"] = UNKNOWN_MANAGER_NAME
    return row


def resolve_manager_name_for_ref(
    ref_key: str,
    headers: dict,
    doc: dict | None = None,
    use_cache: bool = True,
) -> str | None:
    if not ref_key:
        return None
    if use_cache and ref_key in _manager_name_cache:
        return _manager_name_cache[ref_key]

    row = doc or _fetch_doc_by_ref(ref_key, headers, timeout=DOC_TIMEOUT_SECONDS)
    if not row:
        return None

    manager_key = str(row.get("Менеджер_Key") or "").strip()
    if not manager_key or manager_key == ZERO_GUID:
        _manager_name_cache[ref_key] = UNKNOWN_MANAGER_NAME
        return UNKNOWN_MANAGER_NAME

    nav_link = str(row.get("Менеджер@navigationLinkUrl") or "").strip()
    if not nav_link:
        _manager_name_cache[ref_key] = UNKNOWN_MANAGER_NAME
        return UNKNOWN_MANAGER_NAME

    try:
        nav_resp = requests.get(
            f"{BASE}/{nav_link}",
            headers=headers,
            timeout=NAV_TIMEOUT_SECONDS,
            verify=False,
        )
        if nav_resp.status_code == 200:
            nav_obj = nav_resp.json() if isinstance(nav_resp.json(), dict) else {}
            manager_name = str(nav_obj.get("Description") or "").strip() or UNKNOWN_MANAGER_NAME
            _manager_name_cache[ref_key] = manager_name
            return manager_name
    except Exception:
        pass

    _manager_name_cache[ref_key] = UNKNOWN_MANAGER_NAME
    return UNKNOWN_MANAGER_NAME


def _resolve_comment_flag_for_ref(
    ref_key: str,
    headers: dict,
    cache: dict,
    marker: str,
    *,
    doc: dict | None = None,
    use_cache: bool = True,
    first_lines: int | None = None,
) -> bool | None:
    if not ref_key:
        return None
    if use_cache and ref_key in cache:
        return cache[ref_key]

    row = doc or _fetch_doc_by_ref(ref_key, headers, timeout=DOC_TIMEOUT_SECONDS)
    if not row:
        return None

    cleaned = strip_html(str(row.get("Комментарий") or "")).replace("\r\n", "\n").replace("\r", "\n").upper()
    marker_upper = marker.upper()
    if first_lines is not None:
        lines = cleaned.split("\n")[:first_lines]
        result = any(marker_upper in line for line in lines)
    else:
        result = marker_upper in cleaned

    cache[ref_key] = result
    return result


def resolve_manager_filled_for_ref(
    ref_key: str,
    headers: dict,
    doc: dict | None = None,
    use_cache: bool = True,
) -> bool | None:
    if not ref_key:
        return None
    if use_cache and ref_key in _manager_filled_cache:
        return _manager_filled_cache[ref_key]

    row = doc or _fetch_doc_by_ref(ref_key, headers, timeout=DOC_TIMEOUT_SECONDS)
    if not row:
        return None

    manager_name = resolve_manager_name_for_ref(ref_key, headers, doc=row, use_cache=use_cache)
    if manager_name is None:
        return None

    manager_key = str(row.get("Менеджер_Key") or "").strip()
    if not manager_key or manager_key == ZERO_GUID:
        _manager_filled_cache[ref_key] = False
        return False

    result = is_manager_filled(manager_name)
    _manager_filled_cache[ref_key] = result
    return result


def resolve_product_specified_for_ref(
    ref_key: str,
    headers: dict,
    doc: dict | None = None,
    use_cache: bool = True,
) -> bool | None:
    if not ref_key:
        return None
    if use_cache and ref_key in _product_specified_cache:
        return _product_specified_cache[ref_key]

    row = doc or _fetch_doc_by_ref(ref_key, headers, timeout=DOC_TIMEOUT_SECONDS)
    if not row:
        return None

    # Some 1C endpoints return only a navigation link for goods rows.
    # Try loading first goods line directly from the nav link.
    goods_nav = str(row.get("Товары@navigationLinkUrl") or "").strip()
    if goods_nav:
        try:
            nav_resp = requests.get(
                f"{BASE}/{goods_nav}",
                headers=headers,
                params={"$top": "1", "$select": "Номенклатура_Key,Номенклатура"},
                timeout=NAV_TIMEOUT_SECONDS,
                verify=False,
            )
            if nav_resp.status_code == 200:
                payload = nav_resp.json() if isinstance(nav_resp.json(), dict) else {}
                values = payload.get("value") if isinstance(payload, dict) else None
                if isinstance(values, list) and values:
                    first_goods = values[0] if isinstance(values[0], dict) else {}
                    nav_nomenclature_key = str(first_goods.get("Номенклатура_Key") or "").strip()
                    nav_nomenclature_text = str(first_goods.get("Номенклатура") or "").strip()
                    if (nav_nomenclature_key and nav_nomenclature_key != ZERO_GUID) or nav_nomenclature_text:
                        _product_specified_cache[ref_key] = True
                        return True
        except Exception:
            pass

    goods = row.get("Товары")
    if not isinstance(goods, list) or not goods:
        _product_specified_cache[ref_key] = False
        return False

    dict_rows = [item for item in goods if isinstance(item, dict)]
    if not dict_rows:
        _product_specified_cache[ref_key] = False
        return False

    def line_no(item: dict) -> int:
        try:
            return int(str(item.get("LineNumber") or "0"))
        except Exception:
            return 0

    top_row = min(dict_rows, key=line_no)
    nomenclature_key = str(top_row.get("Номенклатура_Key") or "").strip()
    nomenclature_text = str(top_row.get("Номенклатура") or "").strip()
    result = bool((nomenclature_key and nomenclature_key != ZERO_GUID) or nomenclature_text)
    _product_specified_cache[ref_key] = result
    return result


def resolve_price_filled_for_ref(
    ref_key: str,
    headers: dict,
    doc: dict | None = None,
    use_cache: bool = True,
) -> bool | None:
    """Check if the first product row has a valid price (not 0 and not 1)."""
    if not ref_key:
        return None
    if use_cache and ref_key in _price_filled_cache:
        return _price_filled_cache[ref_key]

    row = doc or _fetch_doc_by_ref(ref_key, headers, timeout=DOC_TIMEOUT_SECONDS)
    if not row:
        return None

    def _check_price(goods_row: dict) -> bool:
        try:
            price = float(goods_row.get("Цена") or 0)
        except (ValueError, TypeError):
            price = 0.0
        return price > 1

    # Try navigation link first
    goods_nav = str(row.get("Товары@navigationLinkUrl") or "").strip()
    if goods_nav:
        try:
            nav_resp = requests.get(
                f"{BASE}/{goods_nav}",
                headers=headers,
                params={"$top": "1", "$select": "Цена,LineNumber"},
                timeout=NAV_TIMEOUT_SECONDS,
                verify=False,
            )
            if nav_resp.status_code == 200:
                payload = nav_resp.json() if isinstance(nav_resp.json(), dict) else {}
                values = payload.get("value") if isinstance(payload, dict) else None
                if isinstance(values, list) and values:
                    result = _check_price(values[0])
                    _price_filled_cache[ref_key] = result
                    return result
        except Exception:
            pass

    # Fallback: inline Товары array
    goods = row.get("Товары")
    if not isinstance(goods, list) or not goods:
        _price_filled_cache[ref_key] = False
        return False

    dict_rows = [item for item in goods if isinstance(item, dict)]
    if not dict_rows:
        _price_filled_cache[ref_key] = False
        return False

    def line_no(item: dict) -> int:
        try:
            return int(str(item.get("LineNumber") or "0"))
        except Exception:
            return 0

    top_row = min(dict_rows, key=line_no)
    result = _check_price(top_row)
    _price_filled_cache[ref_key] = result
    return result


def resolve_kp_sent_for_ref(
    ref_key: str,
    headers: dict,
    doc: dict | None = None,
    use_cache: bool = True,
) -> bool | None:
    return _resolve_comment_flag_for_ref(
        ref_key,
        headers,
        _kp_sent_cache,
        "КП ОТПРАВЛЕНО",
        doc=doc,
        use_cache=use_cache,
        first_lines=5,
    )


def resolve_problem_for_ref(
    ref_key: str,
    headers: dict,
    doc: dict | None = None,
    use_cache: bool = True,
) -> bool | None:
    return _resolve_comment_flag_for_ref(
        ref_key,
        headers,
        _problem_cache,
        "ПРОБЛЕМА",
        doc=doc,
        use_cache=use_cache,
    )


def resolve_shipment_pending_for_ref(
    ref_key: str,
    headers: dict,
    doc: dict | None = None,
    use_cache: bool = True,
) -> bool | None:
    return _resolve_comment_flag_for_ref(
        ref_key,
        headers,
        _shipment_pending_cache,
        "ОТГРУЗИТЬ",
        doc=doc,
        use_cache=use_cache,
    )


def resolve_rejected_for_ref(
    ref_key: str,
    headers: dict,
    doc: dict | None = None,
    use_cache: bool = True,
) -> bool | None:
    return _resolve_comment_flag_for_ref(
        ref_key,
        headers,
        _rejected_cache,
        "ОТКАЗ",
        doc=doc,
        use_cache=use_cache,
    )


def resolve_edo_sent_for_ref(
    ref_key: str,
    headers: dict,
    doc: dict | None = None,
    use_cache: bool = True,
) -> bool | None:
    return _resolve_comment_flag_for_ref(
        ref_key,
        headers,
        _edo_sent_cache,
        "В ЭДО ОТПРАВЛЕНО",
        doc=doc,
        use_cache=use_cache,
    )


def resolve_receipt_confirmed_for_ref(
    ref_key: str,
    headers: dict,
    doc: dict | None = None,
    use_cache: bool = True,
) -> bool | None:
    return _resolve_comment_flag_for_ref(
        ref_key,
        headers,
        _receipt_confirmed_cache,
        "КЛИЕНТ КП УВИДЕЛ",
        doc=doc,
        use_cache=use_cache,
        first_lines=5,
    )


def rows_fingerprint(rows: list) -> str:
    return json.dumps(rows, ensure_ascii=False, sort_keys=True)


def score_customer_candidate(nav_obj: dict) -> int:
    description = str(nav_obj.get("Description") or "").strip()
    if not description:
        return 0

    score = 1
    upper = description.upper()
    if any(token in upper for token in ["ООО", "ИП", "АО", "ПАО", "ЗАО"]):
        score += 4

    digit_like = 0
    for value in nav_obj.values():
        if isinstance(value, str):
            only_digits = "".join(ch for ch in value if ch.isdigit())
            if len(only_digits) in (9, 10, 12):
                digit_like += 1
    if digit_like >= 2:
        score += 3

    if len(description) > 4:
        score += 1
    return score


def _fetch_doc_by_ref_once(ref_key: str, headers: dict, timeout: float) -> dict:
    """Single-attempt fetch — no retries, used for fast partial refresh."""
    try:
        doc_resp = requests.get(
            f"{BASE}/{ENTITY}(guid'{ref_key}')",
            headers=headers,
            timeout=timeout,
            verify=False,
        )
        if doc_resp.status_code == 200:
            doc = doc_resp.json()
            return doc if isinstance(doc, dict) else {}
    except Exception:
        pass
    return {}


def _fetch_doc_by_ref(ref_key: str, headers: dict, timeout: float = DOC_TIMEOUT_SECONDS) -> dict:
    for attempt in range(3):
        try:
            doc_resp = requests.get(
                f"{BASE}/{ENTITY}(guid'{ref_key}')",
                headers=headers,
                timeout=timeout,
                verify=False,
            )
            if doc_resp.status_code != 200:
                time.sleep(0.4 * (attempt + 1))
                continue
            doc = doc_resp.json()
            return doc if isinstance(doc, dict) else {}
        except Exception:
            time.sleep(0.4 * (attempt + 1))
    return {}


def _get_json_with_retry(
    url: str,
    headers: dict,
    *,
    params: dict | None = None,
    timeout: float = 20,
    retries: int = 4,
) -> tuple[dict | None, str | None]:
    last_error = None
    for attempt in range(retries):
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=timeout,
                verify=False,
            )
            if response.status_code >= 500:
                last_error = f"HTTP {response.status_code}: {response.text[:300]}"
                time.sleep(0.4 * (attempt + 1))
                continue
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                return payload, None
            return {}, None
        except Exception as exc:
            last_error = str(exc)
            time.sleep(0.4 * (attempt + 1))
    return None, last_error


def _load_order_cache() -> None:
    """Load persisted order→KP cache from disk once per process lifetime."""
    global _order_to_kp_cache, _order_cache_loaded
    if _order_cache_loaded:
        return
    with _order_cache_lock:
        if _order_cache_loaded:
            return
        try:
            if os.path.exists(ORDER_CACHE_FILE):
                with open(ORDER_CACHE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    _order_to_kp_cache = data
        except Exception:
            pass
        _order_cache_loaded = True


def _save_order_cache() -> None:
    """Persist order→KP cache to disk (best-effort)."""
    try:
        os.makedirs(os.path.dirname(ORDER_CACHE_FILE), exist_ok=True)
        tmp = ORDER_CACHE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_order_to_kp_cache, f, ensure_ascii=False)
        os.replace(tmp, ORDER_CACHE_FILE)
    except Exception:
        pass


def _load_payment_seed() -> None:
    """Load persisted payment seed from disk once per process lifetime."""
    global _payment_seed, _payment_seed_loaded
    if _payment_seed_loaded:
        return
    with _payment_seed_lock:
        if _payment_seed_loaded:
            return
        try:
            if os.path.exists(PAYMENT_SEED_FILE):
                with open(PAYMENT_SEED_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    _payment_seed = data
        except Exception:
            pass
        _payment_seed_loaded = True


def _save_payment_seed() -> None:
    """Persist payment seed to disk (best-effort)."""
    try:
        os.makedirs(os.path.dirname(PAYMENT_SEED_FILE), exist_ok=True)
        tmp = PAYMENT_SEED_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_payment_seed, f, ensure_ascii=False, indent=2)
        os.replace(tmp, PAYMENT_SEED_FILE)
    except Exception:
        pass


def _parse_odata_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()

    # 1C may emit legacy MS JSON date format: /Date(1713187200000+0300)/
    match = re.match(r"^/Date\(([-+]?\d+)([-+]\d{4})?\)/$", text)
    if match:
        try:
            millis = int(match.group(1))
            return datetime.utcfromtimestamp(millis / 1000.0)
        except Exception:
            return None

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        pass

    for pattern in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, pattern)
        except Exception:
            continue
    return None


def _iterate_tail_pages(entity_name: str, headers: dict, select_fields: list[str], page_size: int = 200):
    raw_count = ""
    try:
        response = requests.get(
            f"{BASE}/{entity_name}/$count",
            headers=headers,
            timeout=GROUP_CHECK_TIMEOUT_SECONDS,
            verify=False,
        )
        if response.status_code != 200:
            return
        raw_count = response.text.strip()
        total_count = int(raw_count)
    except Exception:
        return

    if total_count <= 0:
        return

    skip = ((total_count - 1) // page_size) * page_size
    select_expr = ",".join(select_fields)

    while True:
        payload, error = _get_json_with_retry(
            f"{BASE}/{entity_name}",
            headers,
            params={"$select": select_expr, "$top": str(page_size), "$skip": str(skip)},
            timeout=GROUP_CHECK_TIMEOUT_SECONDS,
            retries=2,
        )
        if error or not isinstance(payload, dict):
            return

        batch = payload.get("value", [])
        if not batch:
            return

        yield batch

        batch_dates = [_parse_odata_datetime(item.get("Date")) for item in batch]
        batch_dates = [d for d in batch_dates if d is not None]
        if batch_dates and max(batch_dates) < TARGET_START:
            return

        if skip == 0:
            return
        skip = max(0, skip - page_size)


def _collect_tail_pages(
    entity_name: str,
    headers: dict,
    select_fields: list[str],
    page_size: int = 200,
    timeout: float = GROUP_CHECK_TIMEOUT_SECONDS,
) -> tuple[list[list], bool]:
    pages: list[list] = []
    try:
        response = requests.get(
            f"{BASE}/{entity_name}/$count",
            headers=headers,
            timeout=timeout,
            verify=False,
        )
        if response.status_code != 200:
            return pages, False
        total_count = int(response.text.strip())
    except Exception:
        return pages, False

    if total_count <= 0:
        return pages, True

    skip = ((total_count - 1) // page_size) * page_size
    select_expr = ",".join(select_fields)

    while True:
        payload, error = _get_json_with_retry(
            f"{BASE}/{entity_name}",
            headers,
            params={"$select": select_expr, "$top": str(page_size), "$skip": str(skip)},
            timeout=timeout,
            retries=2,
        )
        if error or not isinstance(payload, dict):
            return pages, False

        batch = payload.get("value", [])
        if not isinstance(batch, list):
            return pages, False
        if not batch:
            return pages, True

        pages.append(batch)

        batch_dates = [_parse_odata_datetime(item.get("Date")) for item in batch if isinstance(item, dict)]
        batch_dates = [d for d in batch_dates if d is not None]
        if batch_dates and max(batch_dates) < TARGET_START:
            return pages, True

        if skip == 0:
            return pages, True
        skip = max(0, skip - page_size)


def _collect_tail_pages_with_field_fallback(
    entity_name: str,
    headers: dict,
    select_field_candidates: list[list[str]],
    page_size: int = 200,
    timeout: float = GROUP_CHECK_TIMEOUT_SECONDS,
) -> tuple[list[list], bool, list[str]]:
    best_pages: list[list] = []
    best_fields: list[str] = []

    for fields in select_field_candidates:
        pages, complete = _collect_tail_pages(
            entity_name,
            headers,
            fields,
            page_size=page_size,
            timeout=timeout,
        )
        if complete:
            return pages, True, fields
        if len(pages) > len(best_pages):
            best_pages = pages
            best_fields = fields

    return best_pages, False, best_fields


def _extract_order_refs_from_payment_breakdown(item: dict) -> set[str]:
    refs: set[str] = set()
    breakdown = item.get("РасшифровкаПлатежа")
    if not isinstance(breakdown, list):
        return refs

    for line in breakdown:
        if not isinstance(line, dict):
            continue
        basis_ref = str(line.get("ОснованиеПлатежа") or "")
        basis_type = str(line.get("ОснованиеПлатежа_Type") or "")
        if basis_ref and (not basis_type or basis_type.endswith("Document_ЗаказКлиента")):
            refs.add(basis_ref)


def _fetch_orders_by_number_hints(
    number_hints: set[str], headers: dict, kp_ref_set: set[str]
) -> dict[str, str]:
    """Scan tail pages of Document_ЗаказКлиента (small page_size, long timeout)
    and match against known number hints extracted from payment purposes.
    Returns {order_ref: kp_ref} for any found matches to our target KPs.
    """
    result: dict[str, str] = {}
    if not number_hints:
        return result

    # Build compact number variants from hints for faster matching
    # e.g. digits "198" -> patterns: "198", "0198", "000198", etc.
    hint_patterns: set[str] = set()
    for digits in number_hints:
        hint_patterns.add(digits)
        hint_patterns.add(digits.zfill(3))
        hint_patterns.add(digits.zfill(6))

    # Scan tail pages with small page_size to avoid timeouts
    pages, complete = _collect_tail_pages(
        "Document_ЗаказКлиента",
        headers,
        ["Ref_Key", "Date", "Number", "ДокументОснование", "ДокументОснование_Type"],
        page_size=5,  # small page, high timeout
        timeout=60.0,
    )

    for batch in pages:
        for item in batch:
            if not isinstance(item, dict):
                continue
            base_type = str(item.get("ДокументОснование_Type") or "")
            base_ref = str(item.get("ДокументОснование") or "")
            order_ref = str(item.get("Ref_Key") or "")
            order_number = str(item.get("Number") or "")

            # Check if order number matches any of our hints
            order_compact = "".join(c for c in order_number if c.isdigit())
            if order_compact not in hint_patterns:
                continue

            # Check if this order's base is one of our target КП
            if (
                order_ref
                and base_type.endswith("Document_КоммерческоеПредложениеКлиенту")
                and base_ref in kp_ref_set
            ):
                result[order_ref] = base_ref

    return result

    return refs


def _enrich_group_flags_bulk(rows: list[dict], headers: dict) -> None:
    target_refs = [str(r.get("refKey") or "") for r in rows]
    target_refs = [r for r in target_refs if r]
    if not target_refs:
        return

    kp_ref_set = set(target_refs)

    kp_to_orders: dict[str, set[str]] = {kp: set() for kp in kp_ref_set}
    order_to_kp: dict[str, str] = {}
    order_short_numbers: dict[str, str] = {}
    order_compact_numbers: dict[str, str] = {}

    order_pages, orders_complete = _collect_tail_pages(
        "Document_ЗаказКлиента",
        headers,
        ["Ref_Key", "Date", "Number", "ДокументОснование", "ДокументОснование_Type"],
        page_size=50,
        timeout=max(120.0, GROUP_CHECK_TIMEOUT_SECONDS),
    )
    log(f"[orders] scan: complete={orders_complete}, pages={len(order_pages)}, rows={sum(len(p) for p in order_pages)}")
    _load_order_cache()

    for batch in order_pages:
        for item in batch:
            base_type = str(item.get("ДокументОснование_Type") or "")
            base_ref = str(item.get("ДокументОснование") or "")
            order_ref = str(item.get("Ref_Key") or "")
            if (
                order_ref
                and base_type.endswith("Document_КоммерческоеПредложениеКлиенту")
                and base_ref in kp_ref_set
            ):
                kp_to_orders[base_ref].add(order_ref)
                order_to_kp[order_ref] = base_ref
                order_number = str(item.get("Number") or "")
                digits_trim = "".join(ch for ch in order_number if ch.isdigit()).lstrip("0")
                if digits_trim:
                    order_short_numbers[order_ref] = digits_trim
                compact_number = "".join(ch for ch in order_number.lower() if ch.isalnum())
                if compact_number:
                    order_compact_numbers[order_ref] = compact_number

    # When orders scan is complete, update the persistent cache for all found entries.
    if orders_complete and order_to_kp:
        with _order_cache_lock:
            _order_to_kp_cache.update({
                ref: {"kp": kp, "num": order_short_numbers.get(ref, ""),
                       "compact": order_compact_numbers.get(ref, "")}
                for ref, kp in order_to_kp.items()
            })
        _save_order_cache()
        log(f"[orders-cache] saved {len(order_to_kp)} order→KP entries")

    target_order_refs = set(order_to_kp.keys())
    # Always merge persistent order cache for entries not seen in live scan
    # (covers seed entries like KP229 that exist in cache but not in current scan results).
    with _order_cache_lock:
        for order_ref, entry in _order_to_kp_cache.items():
            if order_ref not in order_to_kp:
                kp_ref = entry.get("kp", "")
                if kp_ref in kp_ref_set:
                    order_to_kp[order_ref] = kp_ref
                    kp_to_orders[kp_ref].add(order_ref)
                    num = entry.get("num", "")
                    if num:
                        order_short_numbers[order_ref] = num
                    compact = entry.get("compact", "")
                    if compact:
                        order_compact_numbers[order_ref] = compact
    target_order_refs = set(order_to_kp.keys())
    if target_order_refs:
        log(f"[orders-cache] merged cache: {len(target_order_refs)} total order→KP entries for {len(kp_ref_set)} KPs")

    if not target_order_refs:
        # Last resort: scan payment purposes, extract order number hints,
        # then try tail-pages on ЗаказКлиента for those specific numbers.
        log(f"[orders-lazy] entering last-resort: target_refs empty, {len(kp_ref_set)} KPs to match")
        purpose_pages, _, _ = _collect_tail_pages_with_field_fallback(
            "Document_ПоступлениеБезналичныхДенежныхСредств",
            headers,
            [["Ref_Key", "НазначениеПлатежа"]],
            page_size=20,
            timeout=max(GROUP_CHECK_TIMEOUT_SECONDS, 12.0),
        )
        purpose_number_hints: set[str] = set()
        for batch in purpose_pages:
            for item in batch:
                purpose = str(item.get("НазначениеПлатежа") or "").lower()
                for m in re.finditer(r"\bут[\s\-_/]*0*(\d+)\b", purpose):
                    digits = m.group(1).lstrip("0") or "0"
                    if digits and digits != "0":
                        purpose_number_hints.add(digits)
        log(f"[orders-lazy] extracted {len(purpose_number_hints)} number hints from {len([i for b in purpose_pages for i in b])} payments: {sorted(purpose_number_hints)[:10]}")
        if purpose_number_hints:
            lazy_orders = _fetch_orders_by_number_hints(purpose_number_hints, headers, kp_ref_set)
            log(f"[orders-lazy] tail-page scan found {len(lazy_orders)} order→KP matches")
            for order_ref, kp_ref in lazy_orders.items():
                order_to_kp[order_ref] = kp_ref
                kp_to_orders[kp_ref].add(order_ref)
            target_order_refs = set(order_to_kp.keys())
            if target_order_refs:
                log(f"[orders-lazy] now have {len(target_order_refs)} target orders for {len(kp_ref_set)} KPs")

    invoice_order_refs: set[str] = set()
    invoices_complete = False
    if not target_order_refs:
        # Preserve current invoice flags when order links cannot be resolved,
        # but continue with payments scan so block-3 rule can still promote paymentReceived.
        log("[orders] no order links resolved; preserving invoice flags, continue with block-3 payment scan")
    else:
        invoice_pages, invoices_complete = _collect_tail_pages(
            "Document_РеализацияТоваровУслуг",
            headers,
            ["Ref_Key", "Date", "ЗаказКлиента", "ЗаказКлиента_Type"],
            page_size=50,
            timeout=max(120.0, GROUP_CHECK_TIMEOUT_SECONDS),
        )
        if not invoices_complete and not invoice_pages:
            log("[invoices] scan unavailable; keeping invoiceCreated as-is")
        else:
            for batch in invoice_pages:
                for item in batch:
                    order_type = str(item.get("ЗаказКлиента_Type") or "")
                    order_ref = str(item.get("ЗаказКлиента") or "")
                    if order_type == "StandardODATA.Document_ЗаказКлиента" and order_ref in target_order_refs:
                        invoice_order_refs.add(order_ref)

    # Strict block3 rule: KP is matched only when any linked order number
    # is present in purpose numbers extracted from payment purpose text.
    block3_ui_kp_hits: set[str] = set()
    purpose_num_set: set[str] = set()
    payment_pages, payments_complete, _ = _collect_tail_pages_with_field_fallback(
        "Document_ПоступлениеБезналичныхДенежныхСредств",
        headers,
        [
            ["Ref_Key", "Date", "Number", "НазначениеПлатежа"],
        ],
        timeout=max(120.0, GROUP_CHECK_TIMEOUT_SECONDS),
    )
    if not payments_complete and not payment_pages:
        # Payment scan completely failed — no data to confirm any payment.
        # Continue to row update loop so stale cached True values are reset to False,
        # consistent with Block 3 showing "нет совпадений" when data unavailable.
        log("[payments] scan completely unavailable; treating all KPs as unpaid (no stale cache)")

    for batch in payment_pages:
        for item in batch:
            purpose = str(item.get("НазначениеПлатежа") or "").lower()
            if not purpose:
                continue
            # Extract purpose numbers exactly as in admin block3 payment-match table.
            # Matches: "УТ-226", "ПСУТ-226" and also "№ 226", "№226" (e.g. "СЧЕТ НА ОПЛАТУ № 226").
            for m in re.finditer(r"(?:[а-яa-z]*ут[\s\-_/]*|№\s*)0*(\d+)", purpose):
                purpose_num = (m.group(1) or "").lstrip("0")
                if purpose_num:
                    purpose_num_set.add(purpose_num)

    # Merge payment seed: covers payments missed by tail-page scan (e.g. early-numbered docs).
    _load_payment_seed()
    for seed_entry in _payment_seed:
        for num in seed_entry.get("purposeNums", []):
            if num:
                purpose_num_set.add(num)

    kp_invoice_map = {kp: False for kp in kp_ref_set}
    kp_payment_map = {kp: False for kp in kp_ref_set}

    for order_ref in invoice_order_refs:
        kp_ref = order_to_kp.get(order_ref)
        if kp_ref:
            kp_invoice_map[kp_ref] = True

    # Apply block3 UI logic: KP is in block3 when any of its order numbers
    # appears in purposeNum set extracted from payment purposes.
    # Keep this strict even on incomplete scans to match block3 behavior exactly.
    for order_ref, order_num in order_short_numbers.items():
        if not order_num or order_num not in purpose_num_set:
            continue
        kp_ref = order_to_kp.get(order_ref)
        if kp_ref and kp_ref in kp_ref_set:
            block3_ui_kp_hits.add(kp_ref)

    for kp_ref in block3_ui_kp_hits:
        kp_payment_map[kp_ref] = True

    for row in rows:
        kp_ref = row.get("refKey")
        if kp_ref in kp_ref_set:
            if orders_complete and invoices_complete:
                row["invoiceCreated"] = kp_invoice_map.get(kp_ref, False)
            elif kp_invoice_map.get(kp_ref, False):
                # Partial orders/invoices scan: only upgrade to True; do not force False.
                row["invoiceCreated"] = True
            row["paymentReceived"] = kp_payment_map.get(kp_ref, False) or bool(row.get("paymentReceived"))


def _normalize_kp_number(value: str) -> str:
    text = str(value or "")
    text = text.replace("ПСУТ-", "").replace("PSUT-", "")
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.lstrip("0") or digits


def _find_kp_ref_by_number(kp_number: str, headers: dict) -> str:
    target = _normalize_kp_number(kp_number)
    if not target:
        return ""

    pages, complete = _collect_tail_pages(
        ENTITY,
        headers,
        ["Ref_Key", "Number", "Date"],
    )
    if not complete:
        return ""

    for batch in pages:
        for item in batch:
            number = str(item.get("Number") or "")
            if _normalize_kp_number(number) == target:
                return str(item.get("Ref_Key") or "")
    return ""


def _trace_kp_group_chain(kp_ref: str, headers: dict) -> dict:
    trace: dict = {
        "kpRef": kp_ref,
        "orders": [],
        "payments": [],
    }

    order_refs: set[str] = set()
    order_numbers: dict[str, str] = {}
    order_short_numbers: dict[str, str] = {}
    order_compact_numbers: dict[str, str] = {}

    order_pages, orders_complete = _collect_tail_pages(
        "Document_ЗаказКлиента",
        headers,
        ["Ref_Key", "Date", "Number", "ДокументОснование", "ДокументОснование_Type"],
    )
    trace["ordersScanComplete"] = bool(orders_complete)
    if not orders_complete:
        return trace

    for batch in order_pages:
        for item in batch:
            base_type = str(item.get("ДокументОснование_Type") or "")
            base_ref = str(item.get("ДокументОснование") or "")
            order_ref = str(item.get("Ref_Key") or "")
            if not order_ref:
                continue
            if base_ref == kp_ref and base_type.endswith("Document_КоммерческоеПредложениеКлиенту"):
                order_refs.add(order_ref)
                number = str(item.get("Number") or "")
                order_numbers[order_ref] = number
                digits_trim = "".join(ch for ch in number if ch.isdigit()).lstrip("0")
                if digits_trim:
                    order_short_numbers[order_ref] = digits_trim
                compact = "".join(ch for ch in number.lower() if ch.isalnum())
                if compact:
                    order_compact_numbers[order_ref] = compact

    trace["orders"] = [
        {"ref": ref, "number": order_numbers.get(ref, "")}
        for ref in sorted(order_refs)
    ]

    if not order_refs:
        return trace

    payment_pages, payments_complete, payment_select_fields = _collect_tail_pages_with_field_fallback(
        "Document_ПоступлениеБезналичныхДенежныхСредств",
        headers,
        [
            [
                "Ref_Key",
                "Date",
                "Number",
                "ОбъектРасчетов_Key",
                "ДокументОснование",
                "ДокументОснование_Type",
                "НазначениеПлатежа",
                "РасшифровкаПлатежа",
            ],
            [
                "Ref_Key",
                "Date",
                "Number",
                "ОбъектРасчетов",
                "ДокументОснование",
                "ДокументОснование_Type",
                "НазначениеПлатежа",
                "РасшифровкаПлатежа",
            ],
            [
                "Ref_Key",
                "Date",
                "Number",
                "ЗаказКлиента",
                "ЗаказКлиента_Type",
                "ДокументОснование",
                "ДокументОснование_Type",
                "НазначениеПлатежа",
                "РасшифровкаПлатежа",
            ],
            [
                "Ref_Key",
                "Date",
                "Number",
                "ДокументОснование",
                "ДокументОснование_Type",
                "НазначениеПлатежа",
                "РасшифровкаПлатежа",
            ],
            [
                "Ref_Key",
                "Date",
                "Number",
                "ОбъектРасчетов_Key",
                "ДокументОснование",
                "ДокументОснование_Type",
                "НазначениеПлатежа",
            ],
            [
                "Ref_Key",
                "Date",
                "Number",
                "ОбъектРасчетов",
                "ДокументОснование",
                "ДокументОснование_Type",
                "НазначениеПлатежа",
            ],
            [
                "Ref_Key",
                "Date",
                "Number",
                "ЗаказКлиента",
                "ЗаказКлиента_Type",
                "ДокументОснование",
                "ДокументОснование_Type",
                "НазначениеПлатежа",
            ],
            [
                "Ref_Key",
                "Date",
                "Number",
                "ДокументОснование",
                "ДокументОснование_Type",
                "НазначениеПлатежа",
            ],
        ],
        timeout=max(GROUP_CHECK_TIMEOUT_SECONDS, 12.0),
    )
    trace["paymentsScanComplete"] = bool(payments_complete)
    trace["paymentsSelectFields"] = payment_select_fields
    if not payments_complete and not payment_pages:
        return trace

    matched = []
    for batch in payment_pages:
        for item in batch:
            pay_ref = str(item.get("Ref_Key") or "")
            pay_number = str(item.get("Number") or "")
            purpose = str(item.get("НазначениеПлатежа") or "")
            purpose_lower = purpose.lower()
            purpose_compact = "".join(ch for ch in purpose_lower if ch.isalnum())

            matched_order = ""
            matched_by = ""

            settlement_order = str(item.get("ОбъектРасчетов_Key") or item.get("ОбъектРасчетов") or "")
            if settlement_order in order_refs:
                matched_order = settlement_order
                matched_by = "ОбъектРасчетов"

            if not matched_order:
                direct_order = str(item.get("ЗаказКлиента") or "")
                direct_order_type = str(item.get("ЗаказКлиента_Type") or "")
                if direct_order in order_refs and (
                    not direct_order_type or direct_order_type.endswith("Document_ЗаказКлиента")
                ):
                    matched_order = direct_order
                    matched_by = "ЗаказКлиента"

            if not matched_order:
                base_type = str(item.get("ДокументОснование_Type") or "")
                base_ref = str(item.get("ДокументОснование") or "")
                if base_ref in order_refs and base_type.endswith("Document_ЗаказКлиента"):
                    matched_order = base_ref
                    matched_by = "ДокументОснование"

            if not matched_order:
                breakdown_order_refs = _extract_order_refs_from_payment_breakdown(item)
                for order_ref in order_refs:
                    if order_ref in breakdown_order_refs:
                        matched_order = order_ref
                        matched_by = "РасшифровкаПлатежа:ОснованиеПлатежа"
                        break

            if not matched_order:
                for order_ref in order_refs:
                    compact = order_compact_numbers.get(order_ref, "")
                    if compact and compact in purpose_compact:
                        matched_order = order_ref
                        matched_by = "НазначениеПлатежа:compact"
                        break

            if not matched_order:
                for order_ref in order_refs:
                    digits_trim = order_short_numbers.get(order_ref, "")
                    if not digits_trim:
                        continue
                    if re.search(rf"\b(?:[а-яa-z]*ут)[\s\-_/]*0*{re.escape(digits_trim)}\b", purpose_lower):
                        matched_order = order_ref
                        matched_by = "НазначениеПлатежа:ut-digits"
                        break

            if matched_order:
                matched.append(
                    {
                        "paymentRef": pay_ref,
                        "paymentNumber": pay_number,
                        "matchedOrderRef": matched_order,
                        "matchedOrderNumber": order_numbers.get(matched_order, ""),
                        "matchedBy": matched_by,
                    }
                )

    trace["payments"] = matched
    trace["hasPayment"] = bool(matched)
    return trace


def _build_payment_match_table(headers: dict) -> dict:
    """Scan all orders and payment docs, return table rows for the admin match UI."""
    _load_order_cache()

    # Target the latest 300 KPs from current runtime cache.
    target_kp_refs: set[str] = set()
    if _cached_rows:
        for row in _cached_rows[:300]:
            ref = str(row.get("refKey") or "").strip()
            if ref:
                target_kp_refs.add(ref)

    # --- orders ---
    order_pages, orders_complete = _collect_tail_pages(
        "Document_ЗаказКлиента",
        headers,
        ["Ref_Key", "Date", "Number", "ДокументОснование", "ДокументОснование_Type"],
        page_size=20,
        timeout=max(120.0, GROUP_CHECK_TIMEOUT_SECONDS),
    )

    # ref_key → {kp_ref, raw_number, short_number}
    order_info: dict[str, dict] = {}
    # kp_ref → list of order_refs
    kp_to_orders: dict[str, list[str]] = {}

    for batch in order_pages:
        for item in batch:
            base_type = str(item.get("ДокументОснование_Type") or "")
            base_ref = str(item.get("ДокументОснование") or "")
            order_ref = str(item.get("Ref_Key") or "")
            if not order_ref:
                continue
            if (
                base_ref
                and base_type.endswith("Document_КоммерческоеПредложениеКлиенту")
                and (not target_kp_refs or base_ref in target_kp_refs)
            ):
                raw_num = str(item.get("Number") or "")
                short = "".join(ch for ch in raw_num if ch.isdigit()).lstrip("0") or ""
                order_info[order_ref] = {"kp_ref": base_ref, "raw": raw_num, "short": short}
                kp_to_orders.setdefault(base_ref, []).append(order_ref)

    # Always merge persistent order cache for entries not seen in live scan.
    # This ensures seed entries (e.g. KP229 seed-ut-198/199) are included even
    # when orders_complete=True (live scan succeeded but doesn't cover all cached pairs).
    with _order_cache_lock:
        for order_ref, entry in _order_to_kp_cache.items():
            if order_ref not in order_info:
                kp_ref = entry.get("kp", "")
                if kp_ref and (not target_kp_refs or kp_ref in target_kp_refs):
                    short = entry.get("num", "")
                    order_info[order_ref] = {"kp_ref": kp_ref, "raw": short, "short": short}
                    kp_to_orders.setdefault(kp_ref, []).append(order_ref)

    # Map kp_ref → КП display number
    kp_number_map: dict[str, str] = {}
    if _cached_rows:
        for row in _cached_rows:
            ref = str(row.get("refKey") or "")
            num = str(row.get("number") or "")
            if ref:
                kp_number_map[ref] = _normalize_kp_number(num)

    # If runtime cache does not cover all target refs, try to backfill KP numbers
    # from the latest КП tail pages.
    missing_target_refs = {
        info.get("kp_ref", "")
        for info in order_info.values()
        if info.get("kp_ref") and not kp_number_map.get(info.get("kp_ref", ""))
    }
    if missing_target_refs:
        kp_pages, _ = _collect_tail_pages(
            ENTITY,
            headers,
            ["Ref_Key", "Number", "Date"],
            page_size=120,
            timeout=max(120.0, GROUP_CHECK_TIMEOUT_SECONDS),
        )
        for batch in kp_pages:
            for item in batch:
                ref = str(item.get("Ref_Key") or "")
                if ref in missing_target_refs:
                    kp_number_map[ref] = _normalize_kp_number(str(item.get("Number") or ""))

    # --- payments ---
    payment_pages, payments_complete, _ = _collect_tail_pages_with_field_fallback(
        "Document_ПоступлениеБезналичныхДенежныхСредств",
        headers,
        [
            ["Ref_Key", "Date", "Number", "НазначениеПлатежа"],
        ],
        timeout=max(GROUP_CHECK_TIMEOUT_SECONDS, 12.0),
    )

    # For each payment build: pay_short (cleaned number), purpose_number (extracted from НазначениеПлатежа)
    pay_rows: list[dict] = []
    for batch in payment_pages:
        for item in batch:
            pay_ref = str(item.get("Ref_Key") or "")
            pay_raw = str(item.get("Number") or "")
            pay_short = "".join(ch for ch in pay_raw if ch.isdigit()).lstrip("0") or pay_raw
            purpose = str(item.get("НазначениеПлатежа") or "")
            # Extract number from "УТ-198" / "ут 198" / "№ 226" / "СЧЕТ НА ОПЛАТУ № 226" etc.
            purpose_nums: list[str] = []
            for m in re.finditer(r"(?:[а-яa-z]*ут[\s\-_/]*|№\s*)0*(\d+)", purpose.lower()):
                d = m.group(1).lstrip("0") or "0"
                if d and d != "0":
                    purpose_nums.append(d)
            pay_rows.append({
                "payRef": pay_ref,
                "payShort": pay_short,
                "purpose": purpose,
                "purposeNums": purpose_nums,
            })

    # Merge payment seed: add seeded entries whose payShort is not already in live scan.
    _load_payment_seed()
    live_pay_shorts = {r["payShort"] for r in pay_rows}
    for seed_entry in _payment_seed:
        short = str(seed_entry.get("payShort") or "")
        if short and short not in live_pay_shorts:
            pay_rows.append({
                "payRef": f"seed-{short}",
                "payShort": short,
                "purpose": seed_entry.get("purpose", ""),
                "purposeNums": seed_entry.get("purposeNums", []),
            })

    # --- build table rows ---
    table_rows: list[dict] = []

    # For each order, try to match payments by purpose number
    matched_pay_refs: set[str] = set()

    # Build: order_short → list[order_ref] for quick lookup
    short_to_orders: dict[str, list[str]] = {}
    for oref, info in order_info.items():
        s = info["short"]
        if s:
            short_to_orders.setdefault(s, []).append(oref)

    for oref, info in sorted(order_info.items(), key=lambda x: x[1]["short"]):
        kp_ref = info["kp_ref"]
        kp_num = kp_number_map.get(kp_ref, "")
        order_short = info["short"]
        order_raw = info["raw"]

        # Find payments that reference this order's number in their purpose
        matched_payments = [
            p for p in pay_rows
            if order_short and order_short in p["purposeNums"]
        ]

        if matched_payments:
            for pay in matched_payments:
                matched_pay_refs.add(pay["payRef"])
                table_rows.append({
                    "kpNum": kp_num,
                    "orderNum": order_short or order_raw,
                    "payNum": pay["payShort"],
                    "purposeNum": ", ".join(pay["purposeNums"]),
                    "match": "СОВПАДЕНИЕ",
                })
        else:
            # Order exists but no payment matched
            table_rows.append({
                "kpNum": kp_num,
                "orderNum": order_short or order_raw,
                "payNum": "",
                "purposeNum": "",
                "match": "",
            })

    # Payments that didn't match any order
    for pay in pay_rows:
        if pay["payRef"] not in matched_pay_refs:
            table_rows.append({
                "kpNum": "",
                "orderNum": "",
                "payNum": pay["payShort"],
                "purposeNum": ", ".join(pay["purposeNums"]),
                "match": "",
            })

    # Sort: matched first, then by kpNum asc
    def _sort_key(r: dict):
        match_flag = 0 if r["match"] == "СОВПАДЕНИЕ" else 1
        try:
            kp_int = int(r["kpNum"]) if r["kpNum"] else 99999
        except ValueError:
            kp_int = 99999
        return (match_flag, kp_int, r["orderNum"], r["payNum"])

    table_rows.sort(key=_sort_key)

    return {
        "ordersScanComplete": orders_complete,
        "paymentsScanComplete": payments_complete,
        "rows": table_rows,
    }


def resolve_customer_name_for_ref(
    ref_key: str,
    headers: dict,
    doc: dict | None = None,
    use_cache: bool = True,
) -> str:
    if not ref_key:
        return ""
    if use_cache and ref_key in _customer_name_cache:
        return _customer_name_cache[ref_key]

    row = doc or _fetch_doc_by_ref(ref_key, headers, timeout=DOC_TIMEOUT_SECONDS)
    if not row:
        _customer_name_cache[ref_key] = ""
        return ""

    # Collect nav links: prefer "Контрагент", skip "Организация" (seller's own org)
    SELLER_KEYS = {"организация", "organisation", "organization"}
    CUSTOMER_KEYS = {"контрагент", "клиент", "покупатель"}

    priority_links = []
    fallback_links = []
    for k, v in row.items():
        if not k.endswith("@navigationLinkUrl"):
            continue
        field = k[: k.index("@")].lower()
        if any(s in field for s in SELLER_KEYS):
            continue  # skip seller's org link
        if any(c in field for c in CUSTOMER_KEYS):
            priority_links.append(v)
        else:
            fallback_links.append(v)

    # If priority (customer) links exist, only use them — never fall back to
    # unrelated links (e.g. Валюта → "руб.") when Контрагент is empty in 1C.
    nav_links = (priority_links if priority_links else fallback_links)[:NAV_LINK_LIMIT]

    best_description = ""
    best_score = 0

    for rel in nav_links:
        try:
            nav_resp = requests.get(
                f"{BASE}/{rel}",
                headers=headers,
                timeout=NAV_TIMEOUT_SECONDS,
                verify=False,
            )
            if nav_resp.status_code != 200:
                continue
            nav_obj = nav_resp.json()
            if not isinstance(nav_obj, dict):
                continue

            # If this is a priority (customer) link, use it immediately
            rel_url_lower = rel.lower()
            if any(c in rel_url_lower for c in CUSTOMER_KEYS):
                description = str(nav_obj.get("Description") or "").strip()
                if description:
                    best_description = description
                    break
                # Priority link exists but Description is empty → Клиент not filled
                continue

            candidate_score = score_customer_candidate(nav_obj)
            if candidate_score > best_score:
                best_score = candidate_score
                best_description = str(nav_obj.get("Description") or "").strip()
        except Exception:
            continue

    _customer_name_cache[ref_key] = best_description
    if best_description:
        return best_description

    # Fallback: read customer by direct *_Key fields if nav-link scanning failed.
    key_candidates = []
    for key_name in ("Контрагент_Key", "Клиент_Key"):
        key_value = str(row.get(key_name) or "").strip()
        if key_value and key_value != ZERO_GUID:
            key_candidates.append(key_value)

    for key_value in key_candidates:
        for catalog in ("Catalog_Контрагенты", "Catalog_Партнеры"):
            try:
                catalog_resp = requests.get(
                    f"{BASE}/{catalog}(guid'{key_value}')",
                    headers=headers,
                    timeout=NAV_TIMEOUT_SECONDS,
                    verify=False,
                )
                if catalog_resp.status_code != 200:
                    continue
                catalog_obj = catalog_resp.json() if isinstance(catalog_resp.json(), dict) else {}
                description = str(catalog_obj.get("Description") or "").strip()
                if description:
                    _customer_name_cache[ref_key] = description
                    return description
            except Exception:
                continue

    return best_description


def resolve_additional_info_for_ref(
    ref_key: str,
    headers: dict,
    doc: dict | None = None,
    use_cache: bool = True,
) -> str:
    if not ref_key:
        return ""
    cached_value = _additional_info_cache.get(ref_key, "")
    if use_cache and cached_value:
        return cached_value

    row = doc or _fetch_doc_by_ref(ref_key, headers, timeout=DOC_TIMEOUT_SECONDS)
    if not row:
        return cached_value

    comment_line = first_line(row.get("Комментарий") or "")
    if comment_line:
        _additional_info_cache[ref_key] = comment_line
        return comment_line

    best_line = ""
    best_score = -1

    for key, value in row.items():
        if not isinstance(value, str):
            continue

        line = first_line(value)
        if not line:
            continue

        key_l = str(key).lower()
        if key_l.endswith("@navigationlinkurl") or key_l.endswith("_key"):
            continue
        if line.startswith("http://") or line.startswith("https://"):
            continue
        if re.fullmatch(r"[0-9a-fA-F-]{36}", line):
            continue
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}.*", line):
            continue

        score = 1
        if key_l == "комментарий":
            score += 10
        elif key_l == "прочаядополнительнаяинформациятекст":
            score += 6
        elif key_l == "дополнительнаяинформацияклиентуhtml":
            score += 4
        if len(line) >= 12:
            score += 2
        if any(ch.isalpha() for ch in line):
            score += 2
        if "@" in line or "-" in line or " " in line:
            score += 1
        if "{" in line or "}" in line:
            score -= 2

        if score > best_score:
            best_score = score
            best_line = line

    if best_line:
        _additional_info_cache[ref_key] = best_line
        return best_line

    return cached_value


def resolve_status_kp_from_requisites(requisites: list, headers: dict) -> str:
    if not isinstance(requisites, list):
        return ""

    for req in requisites:
        if not isinstance(req, dict):
            continue
        if str(req.get("Свойство_Key") or "").lower() != STATUS_KP_PROPERTY_KEY.lower():
            continue

        text_value = str(req.get("ТекстоваяСтрока") or "").strip()
        if text_value:
            return text_value

        value_guid = str(req.get("Значение") or "").strip()
        if not value_guid:
            continue

        if value_guid in _status_kp_value_cache:
            return _status_kp_value_cache[value_guid]

        try:
            value_resp = requests.get(
                f"{BASE}/Catalog_ЗначенияСвойствОбъектов(guid'{value_guid}')",
                headers=headers,
                timeout=NAV_TIMEOUT_SECONDS,
                verify=False,
            )
            if value_resp.status_code == 200:
                value_obj = value_resp.json()
                description = str(value_obj.get("Description") or "").strip()
                _status_kp_value_cache[value_guid] = description
                return description
        except Exception:
            continue

    return ""


def load_rows_from_path(path: Path) -> list:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    for row in data:
        apply_storage_defaults(row)
    data.sort(key=lambda x: x.get("createdAt", ""), reverse=True)
    return data


def load_seed_rows() -> list:
    path = Path(SEED_DATA_FILE)
    if not path.exists():
        log("startup seed skipped: tracked seed file does not exist")
        return []

    rows = load_rows_from_path(path)
    log(f"startup seed loaded: {len(rows)} rows from tracked snapshot")
    return rows


def load_fresh_runtime_rows() -> list:
    path = Path(RUNTIME_DATA_FILE)
    meta_path = Path(RUNTIME_META_FILE)
    if not path.exists():
        log("runtime snapshot skipped: runtime data file does not exist")
        return []

    age_seconds = None
    if meta_path.exists():
        try:
            with meta_path.open("r", encoding="utf-8") as f:
                meta = json.load(f)
            generated_at_raw = str(meta.get("generatedAt") or "").strip()
            if generated_at_raw:
                generated_at = datetime.fromisoformat(generated_at_raw)
                age_seconds = max(0, time.time() - generated_at.timestamp())
            else:
                log("runtime metadata has empty generatedAt; using runtime file mtime")
        except Exception as exc:
            log(f"runtime metadata parse failed: {exc}; using runtime file mtime")
    else:
        log("runtime metadata file does not exist; using runtime file mtime")

    if age_seconds is None:
        try:
            age_seconds = max(0, time.time() - path.stat().st_mtime)
        except Exception:
            age_seconds = 0

    # Always load the runtime snapshot regardless of age on startup.
    # A stale-by-timestamp cache still has enriched flags that are far better
    # than falling back to the seed file (which has all-null flags).
    # The background refresh loop will update data immediately after startup.
    rows = load_rows_from_path(path)
    log(f"runtime snapshot loaded: {len(rows)} rows (age {int(age_seconds)}s)")
    return rows


def _parse_iso_datetime_utc(value: object) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _read_runtime_generated_at(meta_path: Path) -> Optional[datetime]:
    if not meta_path.exists():
        return None
    try:
        with meta_path.open("r", encoding="utf-8") as f:
            meta = json.load(f)
    except Exception:
        return None
    return _parse_iso_datetime_utc(meta.get("generatedAt"))


def _read_runtime_meta(meta_path: Path | None = None) -> dict:
    path = meta_path or Path(RUNTIME_META_FILE)
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            meta = json.load(f)
        return meta if isinstance(meta, dict) else {}
    except Exception:
        return {}


def _read_runtime_current_pointer(path: Path | None = None) -> dict:
    current_path = path or Path(RUNTIME_CURRENT_FILE)
    if not current_path.exists():
        return {}
    try:
        with current_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _write_runtime_current_pointer(pointer: dict, path: Path | None = None) -> None:
    current_path = path or Path(RUNTIME_CURRENT_FILE)
    current_path.parent.mkdir(parents=True, exist_ok=True)
    with current_path.open("w", encoding="utf-8") as f:
        json.dump(pointer or {}, f, ensure_ascii=False, indent=2)


def _github_runtime_ref() -> str:
    return GITHUB_RUNTIME_BRANCH or GITHUB_BRANCH


def _load_json_from_github_path(file_path: str) -> object | None:
    if not GITHUB_REPO or not file_path:
        return None

    gh_headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if GITHUB_TOKEN:
        gh_headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_path}"
    try:
        resp = requests.get(api_url, headers=gh_headers, params={"ref": _github_runtime_ref()}, timeout=20)
        if resp.status_code == 200:
            payload = resp.json()
            content_b64 = str(payload.get("content") or "").replace("\n", "")
            if content_b64:
                decoded = base64.b64decode(content_b64.encode("ascii")).decode("utf-8")
                return json.loads(decoded)
    except Exception as exc:
        log(f"github json API fetch failed ({file_path}): {exc}")

    raw_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{_github_runtime_ref()}/{file_path}"
    try:
        resp = requests.get(raw_url, timeout=20)
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception as exc:
        log(f"github json RAW fetch failed ({file_path}): {exc}")
        return None


def _push_json_to_github_path(file_path: str, payload: object, message: str) -> bool:
    if not GITHUB_TOKEN or not GITHUB_REPO or not file_path:
        log(f"GitHub push skipped ({file_path}): GITHUB_TOKEN or GITHUB_REPO not set")
        return False

    gh_headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_path}"
    content_b64 = base64.b64encode(json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")).decode("ascii")

    current_sha = ""
    try:
        resp = requests.get(api_url, headers=gh_headers, params={"ref": _github_runtime_ref()}, timeout=10)
        if resp.status_code == 200:
            current_sha = str(resp.json().get("sha") or "")
    except Exception as exc:
        log(f"GitHub SHA fetch failed ({file_path}): {exc}")

    body: dict = {
        "message": message,
        "content": content_b64,
        "branch": _github_runtime_ref(),
    }
    if current_sha:
        body["sha"] = current_sha

    try:
        resp = requests.put(api_url, headers=gh_headers, json=body, timeout=20)
        if resp.status_code in (200, 201):
            return True
        log(f"GitHub push failed ({file_path}): HTTP {resp.status_code}: {resp.text[:200]}")
        return False
    except Exception as exc:
        log(f"GitHub push error ({file_path}): {exc}")
        return False


def _build_runtime_version_paths(meta: dict) -> tuple[int, str, str]:
    cycle_version = _to_int_or_none(meta.get("cycleVersion")) or 0
    if cycle_version <= 0:
        raise RuntimeError("runtime metadata has no valid cycleVersion")
    suffix = f"v{cycle_version:06d}"
    cache_path = f"{GITHUB_RUNTIME_VERSIONS_DIR}/kp_runtime_cache_{suffix}.json"
    meta_path = f"{GITHUB_RUNTIME_VERSIONS_DIR}/kp_runtime_meta_{suffix}.json"
    return cycle_version, cache_path, meta_path


def _build_runtime_current_pointer(rows: list, meta: dict) -> dict:
    cycle_version, cache_path, meta_path = _build_runtime_version_paths(meta)
    return {
        "version": cycle_version,
        "status": "confirmed",
        "cachePath": cache_path,
        "metaPath": meta_path,
        "generatedAt": str(meta.get("generatedAt") or ""),
        "writeSource": str(meta.get("writeSource") or ""),
        "rowCount": len(rows),
        "rowsFingerprint": rows_fingerprint(rows),
        "branch": _github_runtime_ref(),
    }


def _load_runtime_rows_from_github_path(file_path: str) -> list:
    payload = _load_json_from_github_path(file_path)
    if not isinstance(payload, list):
        return []
    for row in payload:
        apply_storage_defaults(row)
    payload.sort(key=lambda x: x.get("createdAt", ""), reverse=True)
    return payload


def _load_runtime_meta_from_github_path(file_path: str) -> dict:
    payload = _load_json_from_github_path(file_path)
    return payload if isinstance(payload, dict) else {}


def _load_runtime_current_pointer_from_github() -> dict:
    payload = _load_json_from_github_path(GITHUB_RUNTIME_CURRENT_PATH)
    return payload if isinstance(payload, dict) else {}


def _write_local_confirmed_runtime(rows: list, meta_payload: dict, pointer: dict) -> None:
    _write_runtime_snapshot_files(rows, meta_payload)
    _write_runtime_current_pointer(pointer)


def _load_confirmed_runtime_from_github() -> tuple[list, dict, dict]:
    pointer = _load_runtime_current_pointer_from_github()
    if pointer:
        cache_path = str(pointer.get("cachePath") or "").strip()
        meta_path = str(pointer.get("metaPath") or "").strip()
        rows = _load_runtime_rows_from_github_path(cache_path) if cache_path else []
        meta = _load_runtime_meta_from_github_path(meta_path) if meta_path else {}
        expected_version = _to_int_or_none(pointer.get("version")) or 0
        meta_version = _to_int_or_none(meta.get("cycleVersion")) or 0
        expected_fp = str(pointer.get("rowsFingerprint") or "")
        if rows and meta and expected_version and meta_version == expected_version:
            if not expected_fp or rows_fingerprint(rows) == expected_fp:
                return rows, meta, pointer

    rows = _load_runtime_rows_from_github()
    meta = _load_runtime_meta_from_github()
    if rows and meta:
        return rows, meta, {}
    return [], {}, {}


def _publish_confirmed_runtime_snapshot_or_raise(candidate_rows: list | None = None, candidate_meta: dict | None = None) -> tuple[list, dict, dict]:
    rows = list(candidate_rows or load_rows_from_path(Path(RUNTIME_DATA_FILE)))
    if not rows:
        raise RuntimeError("runtime snapshot is empty after 1C refresh")

    meta = dict(candidate_meta or _read_runtime_meta())
    if not meta:
        raise RuntimeError("runtime metadata is missing after 1C refresh")

    pointer = _build_runtime_current_pointer(rows, meta)
    version = pointer.get("version")
    cache_path = str(pointer.get("cachePath") or "")
    meta_path = str(pointer.get("metaPath") or "")
    message_prefix = f"Runtime snapshot v{version}"

    if not _push_json_to_github_path(cache_path, rows, f"{message_prefix} cache [skip ci]"):
        raise RuntimeError("GitHub cache version push failed")
    if not _push_json_to_github_path(meta_path, meta, f"{message_prefix} meta [skip ci]"):
        raise RuntimeError("GitHub meta version push failed")

    github_rows = _load_runtime_rows_from_github_path(cache_path)
    github_meta = _load_runtime_meta_from_github_path(meta_path)
    if not github_rows or not github_meta:
        raise RuntimeError("GitHub version readback failed")
    if rows_fingerprint(github_rows) != str(pointer.get("rowsFingerprint") or ""):
        raise RuntimeError("GitHub version readback fingerprint mismatch")
    if (_to_int_or_none(github_meta.get("cycleVersion")) or 0) != (_to_int_or_none(pointer.get("version")) or 0):
        raise RuntimeError("GitHub version readback cycleVersion mismatch")

    if not _push_json_to_github_path(
        GITHUB_RUNTIME_CURRENT_PATH,
        pointer,
        f"Promote runtime current v{version} [skip ci]",
    ):
        raise RuntimeError("GitHub current pointer update failed")

    confirmed_pointer = _load_runtime_current_pointer_from_github()
    confirmed_version = _to_int_or_none(confirmed_pointer.get("version")) or 0
    if confirmed_version != (_to_int_or_none(pointer.get("version")) or 0):
        raise RuntimeError("GitHub current pointer readback mismatch")

    _write_local_confirmed_runtime(github_rows, github_meta, confirmed_pointer)
    return github_rows, github_meta, confirmed_pointer


def _sync_confirmed_runtime_cache_from_github_if_needed(reason: str, force: bool = False) -> bool:
    global _cached_rows, _cached_fp, _last_refresh_error, _last_confirmed_runtime_sync_check

    now = time.time()
    if not force and _cached_rows and (now - _last_confirmed_runtime_sync_check) < CONFIRMED_RUNTIME_SYNC_TTL_SECONDS:
        return True

    with _confirmed_runtime_sync_lock:
        now = time.time()
        if not force and _cached_rows and (now - _last_confirmed_runtime_sync_check) < CONFIRMED_RUNTIME_SYNC_TTL_SECONDS:
            return True

        github_rows, github_meta, github_pointer = _load_confirmed_runtime_from_github()
        local_pointer = _read_runtime_current_pointer()
        github_version = _to_int_or_none(github_pointer.get("version")) or 0
        local_version = _to_int_or_none(local_pointer.get("version")) or 0
        github_fp = str(github_pointer.get("rowsFingerprint") or "")

        if github_rows and github_meta:
            # If GitHub has no versioned pointer (legacy fallback), only update
            # when fingerprint actually differs — not just because version numbers differ.
            if github_version == 0 and local_version > 0:
                needs_update = force or not _cached_rows or (github_fp and _cached_fp != github_fp)
            else:
                needs_update = (
                    force
                    or not _cached_rows
                    or github_version != local_version
                    or (github_fp and _cached_fp != github_fp)
                )
            if needs_update:
                _write_local_confirmed_runtime(github_rows, github_meta, github_pointer or _build_runtime_current_pointer(github_rows, github_meta))
                _cached_rows = list(github_rows)
                _cached_fp = rows_fingerprint(_cached_rows)
                _last_refresh_error = None
                log(f"confirmed runtime synced from GitHub: rows={len(_cached_rows)}, reason={reason}, version={github_version or 'legacy'}")
            _last_confirmed_runtime_sync_check = now
            return True

        local_rows = load_fresh_runtime_rows()
        if local_rows:
            _cached_rows = list(local_rows)
            _cached_fp = rows_fingerprint(_cached_rows)
            _last_confirmed_runtime_sync_check = now
            return True

        return False


def _load_runtime_meta_from_github() -> dict:
    if not GITHUB_REPO:
        return {}

    gh_headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if GITHUB_TOKEN:
        gh_headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/data/kp_runtime_meta.json"
    try:
        resp = requests.get(api_url, headers=gh_headers, params={"ref": GITHUB_BRANCH}, timeout=20)
        if resp.status_code == 200:
            payload = resp.json()
            content_b64 = str(payload.get("content") or "").replace("\n", "")
            if content_b64:
                decoded = base64.b64decode(content_b64.encode("ascii")).decode("utf-8")
                meta = json.loads(decoded)
                return meta if isinstance(meta, dict) else {}
    except Exception as exc:
        log(f"github runtime meta API fetch failed: {exc}")

    raw_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/data/kp_runtime_meta.json"
    try:
        resp = requests.get(raw_url, timeout=20)
        if resp.status_code != 200:
            return {}
        meta = resp.json()
        return meta if isinstance(meta, dict) else {}
    except Exception as exc:
        log(f"github runtime meta RAW fetch failed: {exc}")
        return {}


def _to_int_or_none(value: object) -> int | None:
    try:
        parsed = int(value)
    except Exception:
        return None
    return parsed if parsed > 0 else None


def save_rows(
    rows: list,
    *,
    refresh_started_at: Optional[datetime] = None,
    write_source: str = "runtime-refresh",
    push_to_github: bool = True,
) -> bool:
    for row in rows:
        apply_storage_defaults(row)

    started_at = refresh_started_at or datetime.now(timezone.utc)
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    else:
        started_at = started_at.astimezone(timezone.utc)

    runtime_path = Path(RUNTIME_DATA_FILE)
    runtime_meta_path = Path(RUNTIME_META_FILE)
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_meta_path.parent.mkdir(parents=True, exist_ok=True)

    with _runtime_write_guard_lock:
        current_generated_at = _read_runtime_generated_at(runtime_meta_path)
        if current_generated_at and current_generated_at > started_at:
            log(
                "save_rows skipped: newer runtime snapshot already exists "
                f"(source={write_source}, current={current_generated_at.isoformat()}, "
                f"started={started_at.isoformat()})"
            )
            return False

        generated_at = datetime.now(timezone.utc)
        prev_meta = _read_runtime_meta(runtime_meta_path)
        
        # Ensure metadata has all required fields (for old files that lack new fields)
        if not prev_meta.get("cycleVersion"):
            prev_meta["cycleVersion"] = 0
        if not prev_meta.get("last1cLoadedVersion"):
            prev_meta["last1cLoadedVersion"] = 0
        if not prev_meta.get("last1cLoadedAt"):
            prev_meta["last1cLoadedAt"] = prev_meta.get("generatedAt") or ""
        
        prev_cycle = int(prev_meta.get("cycleVersion") or 0)
        prev_last_1c = int(prev_meta.get("last1cLoadedVersion") or 0)
        prev_last_1c_at = str(prev_meta.get("last1cLoadedAt") or prev_meta.get("generatedAt") or "")

        cycle_version = prev_cycle + 1
        is_live_1c_write = not str(write_source or "").startswith("github-recovery:")
        meta_payload = {
            "generatedAt": generated_at.isoformat(),
            "refreshStartedAt": started_at.isoformat(),
            "rowCount": len(rows),
            "writeSource": write_source,
            "cycleVersion": cycle_version,
            "last1cLoadedVersion": cycle_version if is_live_1c_write else prev_last_1c,
            "last1cLoadedAt": generated_at.isoformat() if is_live_1c_write else prev_last_1c_at,
        }

        with runtime_path.open("w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        with runtime_meta_path.open("w", encoding="utf-8") as f:
            json.dump(meta_payload, f, ensure_ascii=False, indent=2)

        if push_to_github:
            threading.Thread(
                target=_push_runtime_cache_to_github,
                args=(rows, meta_payload),
                daemon=True,
            ).start()
        return True


def build_known_rows_lookup() -> dict:
    known: dict = {}

    def _append(rows: list) -> None:
        for source_row in list(rows or []):
            number = source_row.get("number")
            if number and number not in known:
                known[number] = source_row

    # 1) In-memory cache (fast path for running API process)
    _append(list(_cached_rows))

    # 2) Disk snapshots (critical for standalone scripts like tools/refresh_seed.py)
    # so flags do not reset when process memory starts empty.
    for snapshot_path in (Path(RUNTIME_DATA_FILE), Path(SEED_DATA_FILE)):
        if not snapshot_path.exists():
            continue
        try:
            _append(load_rows_from_path(snapshot_path))
        except Exception as exc:
            log(f"known rows snapshot read failed ({snapshot_path}): {exc}")

    return known


def _build_date_filter() -> str:
    """Build OData $filter for TARGET_START..TARGET_END date range."""
    start_str = TARGET_START.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = TARGET_END.strftime("%Y-%m-%dT%H:%M:%S")
    return f"Date ge datetime'{start_str}' and Date le datetime'{end_str}'"


def get_total_count(headers: dict, odata_filter: str = "") -> int:
    url = f"{BASE}/{ENTITY}/$count"
    params = {}
    if odata_filter:
        params["$filter"] = odata_filter
    resp = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=120,
        verify=False,
    )
    resp.raise_for_status()
    return int(resp.text.strip())


def _save_stage_patch(stage_name: str, rows: list) -> None:
    """Persist stage deltas for diagnostics/replay without touching deploy flow."""
    try:
        patches_dir = Path("data") / "patches"
        patches_dir.mkdir(parents=True, exist_ok=True)
        patch_path = patches_dir / f"{stage_name}.json"
        with patch_path.open("w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        log(f"stage patch save failed ({stage_name}): {exc}")


def _fetch_latest_kp_base_batch(headers: dict, page_size: int = 300) -> tuple[int, int, list]:
    select_expr = "Ref_Key,Number,Date,Статус,СуммаДокумента"
    wanted = max(1, page_size)
    chunk_size = min(50, wanted)

    total_count = get_total_count(headers)
    if total_count <= 0:
        return total_count, 0, []

    skip = max(0, total_count - chunk_size)
    initial_skip = skip
    collected: list = []

    # 1C OData-specific stable strategy:
    # read from the tail in small pages and move backwards.
    while len(collected) < wanted:
        top = chunk_size
        payload, error = _get_json_with_retry(
            f"{BASE}/{ENTITY}",
            headers,
            params={
                "$select": select_expr,
                "$top": str(top),
                "$skip": str(skip),
            },
            timeout=BASE_BATCH_TIMEOUT_SECONDS,
            retries=3,
        )
        if error or not isinstance(payload, dict):
            if collected:
                break
            raise RuntimeError(error or "stage1 base batch request failed")

        batch = payload.get("value", [])
        if not isinstance(batch, list) or not batch:
            break

        collected.extend(batch)
        batch_dates = [_parse_odata_datetime(item.get("Date")) for item in batch if isinstance(item, dict)]
        batch_dates = [d for d in batch_dates if d is not None]
        if batch_dates and max(batch_dates) < TARGET_START:
            break

        if skip == 0:
            break
        skip = max(0, skip - chunk_size)

        if len(batch) < top:
            break

    return total_count, initial_skip, collected[:wanted]


def fetch_rows_from_odata(include_stage6: bool = True, page_size: int = 300) -> list:
    """Staged refresh pipeline.

    Old legacy path (multi-page backward scan with large skip loop) is removed.
    """
    headers = _build_headers()
    known_rows = build_known_rows_lookup()
    rows = []
    total_count = 0
    skip = 0

    base_batch: list = []
    stage1_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            total_count, skip, base_batch = _fetch_latest_kp_base_batch(headers, page_size=max(1, page_size))
            stage1_error = None
            break
        except Exception as exc:
            stage1_error = exc
            log(f"stage1_base attempt {attempt}/3 failed: {type(exc).__name__}: {exc}")
            if attempt < 3:
                time.sleep(2)

    if stage1_error is not None:
        message = f"stage1_base failed after retries: {type(stage1_error).__name__}: {stage1_error}"
        log(message)
        raise RuntimeError(message)

    if total_count <= 0:
        log(f"stage1_base: total_count={total_count}, aborting")
        return []

    log(f"stage1_base: total_count={total_count}, skip={skip}, rows={len(base_batch)}")

    docs_by_ref: dict[str, dict] = {}
    stage1_patch: list[dict] = []
    for item in base_batch:
        ref_key = str(item.get("Ref_Key") or "")
        number = str(item.get("Number") or "")
        dt_raw = item.get("Date") or ""
        status = str(item.get("Статус") or "")

        dt = _parse_odata_datetime(str(dt_raw))
        if dt is None:
            continue
        if not (TARGET_START <= dt <= TARGET_END):
            continue

        known_row = known_rows.get(number, {})
        row = {
            "refKey": ref_key,
            "number": number,
            "createdAt": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "customerName": known_row.get("customerName", ""),
            "managerName": known_row.get("managerName", UNKNOWN_MANAGER_NAME),
            "status": status,
            "managerFilled": known_row.get("managerFilled"),
            "productSpecified": known_row.get("productSpecified"),
            "priceFilled": known_row.get("priceFilled"),
            "kpSent": known_row.get("kpSent"),
            "receiptConfirmed": known_row.get("receiptConfirmed"),
            "edoSent": known_row.get("edoSent"),
            "rejected": known_row.get("rejected"),
            "problem": known_row.get("problem"),
            "shipmentPending": known_row.get("shipmentPending"),
            "statusKp": known_row.get("statusKp", ""),
            "additionalInfoFirstLine": known_row.get("additionalInfoFirstLine", ""),
            "invoiceCreated": known_row.get("invoiceCreated"),
            "paymentReceived": known_row.get("paymentReceived"),
            "statusHash": known_row.get("statusHash", ""),
        }
        apply_storage_defaults(row)
        rows.append(row)

        stage1_patch.append(
            {
                "refKey": ref_key,
                "number": number,
                "createdAt": row["createdAt"],
                "status": status,
                "additionalInfoFirstLine": row["additionalInfoFirstLine"],
            }
        )

    _save_stage_patch("stage1_base", stage1_patch)

    # Stage 2.5: fetch docs in parallel for per-doc stages.
    def _fetch_one(ref_key: str) -> tuple[str, dict]:
        if not ref_key:
            return ref_key, {}
        return ref_key, _fetch_doc_by_ref(ref_key, headers, timeout=max(DOC_TIMEOUT_SECONDS, 6.0))

    ref_keys = [str(row.get("refKey") or "") for row in rows]
    doc_ok = 0
    doc_fail = 0
    with ThreadPoolExecutor(max_workers=25) as pool:
        futures = {pool.submit(_fetch_one, rk): rk for rk in ref_keys}
        for future in as_completed(futures):
            rk, doc = future.result()
            docs_by_ref[rk] = doc
            if doc:
                doc_ok += 1
            else:
                doc_fail += 1
    log(f"stage2.5: fetched {doc_ok} ok, {doc_fail} failed/timeout out of {len(ref_keys)} docs")

    # Stage 2: quick flags from full comment payload.
    stage2_patch: list[dict] = []
    for row in rows:
        ref_key = str(row.get("refKey") or "")
        doc = docs_by_ref.get(ref_key) or {}
        comment_raw = str(doc.get("Комментарий") or "")
        comment_clean = strip_html(comment_raw).replace("\r\n", "\n").replace("\r", "\n").upper()
        comment_top = comment_clean.split("\n")[:5]
        payment_by_comment = any("ОПЛАТА ПРИШЛА" in line for line in comment_top)
        patch = {
            "refKey": ref_key,
            "kpSent": any("КП ОТПРАВЛЕНО" in line for line in comment_top),
            "receiptConfirmed": any("КЛИЕНТ КП УВИДЕЛ" in line for line in comment_top),
            "edoSent": "В ЭДО ОТПРАВЛЕНО" in comment_clean,
            "rejected": "ОТКАЗ" in comment_clean,
            "problem": "ПРОБЛЕМА" in comment_clean,
            "shipmentPending": "ОТГРУЗИТЬ" in comment_clean,
            "additionalInfoFirstLine": first_line(comment_raw) or row.get("additionalInfoFirstLine") or "",
        }
        if payment_by_comment:
            patch["paymentReceived"] = True
        row.update(patch)
        stage2_patch.append(patch)
    _save_stage_patch("stage2_comment_flags", stage2_patch)

    # Stage 3: customer.
    stage3_patch: list[dict] = []
    for row in rows:
        ref_key = str(row.get("refKey") or "")
        doc = docs_by_ref.get(ref_key) or {}
        if doc:
            customer_name = resolve_customer_name_for_ref(ref_key, headers, doc=doc, use_cache=False)
            if customer_name:
                row["customerName"] = customer_name
        patch = {
            "refKey": ref_key,
            "customerName": row.get("customerName") or "",
            "clientFilled": is_client_filled(row.get("customerName") or ""),
        }
        row.update(patch)
        stage3_patch.append(patch)
    _save_stage_patch("stage3_customer", stage3_patch)

    # Stage 4: manager.
    stage4_patch: list[dict] = []
    for row in rows:
        ref_key = str(row.get("refKey") or "")
        doc = docs_by_ref.get(ref_key) or {}
        if doc:
            manager_filled = resolve_manager_filled_for_ref(ref_key, headers, doc=doc, use_cache=False)
            if manager_filled is not None:
                row["managerFilled"] = manager_filled
                manager_name = resolve_manager_name_for_ref(ref_key, headers, doc=doc, use_cache=False)
                if manager_name:
                    row["managerName"] = manager_name
        patch = {
            "refKey": ref_key,
            "managerName": row.get("managerName") or UNKNOWN_MANAGER_NAME,
            "managerFilled": row.get("managerFilled"),
        }
        row.update(patch)
        stage4_patch.append(patch)
    _save_stage_patch("stage4_manager", stage4_patch)

    # Stage 5: goods/price.
    stage5_patch: list[dict] = []
    for row in rows:
        ref_key = str(row.get("refKey") or "")
        doc = docs_by_ref.get(ref_key) or {}
        if doc:
            product_specified = resolve_product_specified_for_ref(ref_key, headers, doc=doc, use_cache=False)
            price_filled = resolve_price_filled_for_ref(ref_key, headers, doc=doc, use_cache=False)
            if product_specified is not None:
                row["productSpecified"] = bool(product_specified)
            if price_filled is not None:
                row["priceFilled"] = bool(price_filled)
        patch = {
            "refKey": ref_key,
            "productSpecified": row.get("productSpecified"),
            "priceFilled": row.get("priceFilled"),
        }
        row.update(patch)
        stage5_patch.append(patch)
    _save_stage_patch("stage5_product_price", stage5_patch)

    # Stage 6: heavy group flags (orders/invoices/payments).
    stage6_patch: list[dict] = []
    if include_stage6:
        try:
            _enrich_group_flags_bulk(rows, headers)
        except Exception as exc:
            log(f"stage6_group_flags failed: {type(exc).__name__}: {exc}")

        for row in rows:
            stage6_patch.append(
                {
                    "refKey": row.get("refKey"),
                    "invoiceCreated": bool(row.get("invoiceCreated")),
                    "paymentReceived": bool(row.get("paymentReceived")),
                }
            )
        _save_stage_patch("stage6_group_flags", stage6_patch)
    else:
        _save_stage_patch("stage6_group_flags", [])
        log("stage6_group_flags skipped (fast mode)")

    for row in rows:
        apply_runtime_defaults(row)

    rows.sort(key=lambda x: x.get("createdAt", ""), reverse=True)
    log(f"staged refresh success: {len(rows)} rows")
    return rows


def _partial_refresh_from_cached_rows(
    rows: list[dict],
    headers: dict,
    start_idx: int,
) -> tuple[list[dict], int, int]:
    if not rows:
        return rows, 0, 0

    refreshed: list[dict] = [dict(r) for r in rows]
    refs = [str(r.get("refKey") or "") for r in refreshed if r.get("refKey")]
    if not refs:
        return refreshed, 0, 0

    total_refs = len(refs)
    chunk = max(1, min(FAST_PARTIAL_CHUNK_SIZE, total_refs))
    start = max(0, min(start_idx, total_refs - 1))
    indices = [(start + i) % total_refs for i in range(chunk)]
    target_refs = {refs[i] for i in indices if refs[i]}
    next_idx = (start + chunk) % total_refs

    def _fetch_one(ref_key: str) -> tuple[str, dict]:
        if not ref_key:
            return ref_key, {}
        return ref_key, _fetch_doc_by_ref_once(ref_key, headers, timeout=FAST_PARTIAL_DOC_TIMEOUT)

    docs_by_ref: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=max(1, FAST_PARTIAL_WORKERS)) as pool:
        futures = {pool.submit(_fetch_one, rk): rk for rk in target_refs}
        for future in as_completed(futures):
            rk, doc = future.result()
            docs_by_ref[rk] = doc

    touched = 0
    for row in refreshed:
        ref_key = str(row.get("refKey") or "")
        doc = docs_by_ref.get(ref_key) or {}
        if not doc:
            continue
        touched += 1

        raw_comment = str(doc.get("Комментарий") or "")
        row["additionalInfoFirstLine"] = first_line(raw_comment) or row.get("additionalInfoFirstLine") or ""

        comment_clean = strip_html(raw_comment).replace("\r\n", "\n").replace("\r", "\n").upper()
        comment_top = comment_clean.split("\n")[:5]
        row["kpSent"] = any("КП ОТПРАВЛЕНО" in line for line in comment_top)
        row["receiptConfirmed"] = any("КЛИЕНТ КП УВИДЕЛ" in line for line in comment_top)
        row["edoSent"] = "В ЭДО ОТПРАВЛЕНО" in comment_clean
        row["rejected"] = "ОТКАЗ" in comment_clean
        row["problem"] = "ПРОБЛЕМА" in comment_clean
        row["shipmentPending"] = "ОТГРУЗИТЬ" in comment_clean

        apply_runtime_defaults(row)

    refreshed.sort(key=lambda x: x.get("createdAt", ""), reverse=True)
    return refreshed, touched, next_idx


def refresh_cache_and_file(
    allow_partial_fallback: bool = True,
    include_stage6: bool = True,
    page_size: int = 300,
    use_known_cache: bool = True,
    push_to_github: bool = True,
    update_live_cache: bool = True,
) -> bool:
    """Returns True if refresh actually ran, False if skipped (another cycle holds the lock)."""
    global _cached_rows, _cached_fp, _last_refresh, _last_refresh_error
    refresh_started_at = datetime.now(timezone.utc)

    if not _refresh_run_lock.acquire(blocking=False):
        log("refresh skipped: another refresh cycle is running")
        return False
    if not _refresh_lock.acquire(blocking=False):
        _refresh_run_lock.release()
        log("refresh skipped: previous full cycle is still running")
        return False

    try:
        try:
            fetched = fetch_rows_from_odata(include_stage6=include_stage6, page_size=page_size)
            if fetched:
                saved = save_rows(
                    fetched,
                    refresh_started_at=refresh_started_at,
                    write_source="full-refresh",
                    push_to_github=push_to_github,
                )
                if not saved:
                    latest_rows = load_fresh_runtime_rows()
                    if update_live_cache and latest_rows:
                        _cached_rows = latest_rows
                        _cached_fp = rows_fingerprint(latest_rows)
                    _last_refresh_error = "full refresh skipped: newer runtime snapshot already exists"
                    log(_last_refresh_error)
                    return
                if update_live_cache:
                    _cached_rows = fetched
                    _cached_fp = rows_fingerprint(fetched)
                    _last_refresh = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")
                    _last_refresh_error = None
                log(f"refresh success: {len(fetched)} rows")
                # Disabled runtime cache auto-push to GitHub: it creates a deploy loop
                # on Render (new commit -> new deploy -> new commit...).
                return

            # Stage1 returned 0 rows: do NOT fallback to old cached data (may be from 2015, 2016, etc).
            # Only allow partial refresh if explicitly requested AND we have explicitly vetted target-period cache.
            if allow_partial_fallback and _cached_rows:
                # CRITICAL: only use cache if all rows are in target period to prevent returning 2015+ data.
                try:
                    valid_cached = []
                    for r in _cached_rows:
                        try:
                            dt = datetime.strptime(r.get("createdAt", ""), "%Y-%m-%d %H:%M:%S")
                            if TARGET_START <= dt <= TARGET_END:
                                valid_cached.append(r)
                        except (ValueError, TypeError):
                            pass
                    
                    if not valid_cached:
                        # Cache has no rows in target period: it's stale/old, cannot use it.
                        _last_refresh_error = "refresh returned 0 rows; cached rows are outside target period (skipped fallback)"
                        log(_last_refresh_error)
                        return

                    headers = _build_headers()
                    partial_rows, touched, _ = _partial_refresh_from_cached_rows(valid_cached, headers, 0)
                    if touched > 0:
                        saved = save_rows(
                            partial_rows,
                            refresh_started_at=refresh_started_at,
                            write_source="full-refresh-partial-fallback",
                            push_to_github=push_to_github,
                        )
                        if not saved:
                            latest_rows = load_fresh_runtime_rows()
                            if update_live_cache and latest_rows:
                                _cached_rows = latest_rows
                                _cached_fp = rows_fingerprint(latest_rows)
                            _last_refresh_error = "partial fallback skipped: newer runtime snapshot already exists"
                            log(_last_refresh_error)
                            return
                        if update_live_cache:
                            _cached_rows = partial_rows
                            _cached_fp = rows_fingerprint(partial_rows)
                            _last_refresh = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")
                            _last_refresh_error = None
                        log(f"partial refresh success from cached refs: touched={touched}, rows={len(partial_rows)}")
                        return
                    else:
                        log("partial refresh: no docs touched, keeping cache")
                        return
                except Exception as partial_exc:
                    log(f"partial refresh failed: {partial_exc}")
            elif not allow_partial_fallback:
                log("partial fallback skipped for this refresh run")

            _last_refresh_error = "refresh returned 0 rows"
            log("refresh returned 0 rows, keeping last successful live cache")
        except Exception as exc:
            _last_refresh_error = str(exc)
            log(f"refresh failed, keeping last successful live cache: {exc}")
    finally:
        _refresh_lock.release()
        _refresh_run_lock.release()
    return True


def refresh_cached_rows_only() -> dict:
    global _cached_rows, _cached_fp, _last_refresh, _last_refresh_error, _partial_refresh_cursor
    global _last_comment_refresh, _last_comment_refresh_error

    if not _cached_rows:
        return {"ok": False, "skipped": "empty-cache"}
    if not _refresh_run_lock.acquire(blocking=False):
        log("fast partial refresh skipped: another refresh cycle is running")
        return {"ok": False, "skipped": "another-refresh-running"}
    if not _partial_refresh_lock.acquire(blocking=False):
        _refresh_run_lock.release()
        log("fast partial refresh skipped: already running")
        return {"ok": False, "skipped": "already-running"}

    try:
        refresh_started_at = datetime.now(timezone.utc)
        headers = _build_headers()
        partial_rows, touched, next_idx = _partial_refresh_from_cached_rows(
            _cached_rows,
            headers,
            _partial_refresh_cursor,
        )
        _partial_refresh_cursor = next_idx
        if touched > 0:
            saved = save_rows(
                partial_rows,
                refresh_started_at=refresh_started_at,
                write_source="fast-partial-refresh",
            )
            if not saved:
                latest_rows = load_fresh_runtime_rows()
                if latest_rows:
                    _cached_rows = latest_rows
                    _cached_fp = rows_fingerprint(latest_rows)
                _last_comment_refresh_error = "fast partial skipped: newer runtime snapshot already exists"
                log(_last_comment_refresh_error)
                return {
                    "ok": False,
                    "error": _last_comment_refresh_error,
                    "nextIdx": _partial_refresh_cursor,
                }
            _cached_rows = partial_rows
            _cached_fp = rows_fingerprint(partial_rows)
            _last_refresh = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")
            _last_refresh_error = None
            _last_comment_refresh = _last_refresh
            _last_comment_refresh_error = None
            log(
                "fast partial refresh success: "
                f"touched={touched}, rows={len(partial_rows)}, next_idx={_partial_refresh_cursor}"
            )
            return {
                "ok": True,
                "touched": touched,
                "rows": len(partial_rows),
                "nextIdx": _partial_refresh_cursor,
            }
        else:
            _last_comment_refresh = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")
            _last_comment_refresh_error = None
            log(f"fast partial refresh: no docs touched, next_idx={_partial_refresh_cursor}")
            return {
                "ok": True,
                "touched": 0,
                "rows": len(partial_rows),
                "nextIdx": _partial_refresh_cursor,
            }
    except Exception as exc:
        _last_comment_refresh_error = f"{type(exc).__name__}: {exc}"
        log(f"fast partial refresh failed: {type(exc).__name__}: {exc}")
        return {"ok": False, "error": _last_comment_refresh_error}
    finally:
        _partial_refresh_lock.release()
        _refresh_run_lock.release()


def refresh_comment_first_line_only() -> dict:
    global _cached_rows, _cached_fp, _last_refresh, _last_refresh_error

    if not _cached_rows:
        return {"ok": False, "error": "empty-cache"}
    refresh_started_at = datetime.now(timezone.utc)

    headers = _build_headers()
    refreshed: list[dict] = [dict(r) for r in _cached_rows]
    refs = [str(r.get("refKey") or "") for r in refreshed if r.get("refKey")]
    if not refs:
        return {"ok": False, "error": "no-ref-keys"}

    def _fetch_one(ref_key: str) -> tuple[str, dict]:
        if not ref_key:
            return ref_key, {}
        return ref_key, _fetch_doc_by_ref_once(ref_key, headers, timeout=FAST_PARTIAL_DOC_TIMEOUT)

    docs_by_ref: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=max(1, FAST_PARTIAL_WORKERS)) as pool:
        futures = {pool.submit(_fetch_one, rk): rk for rk in refs}
        for future in as_completed(futures):
            rk, doc = future.result()
            docs_by_ref[rk] = doc

    touched = 0
    for row in refreshed:
        ref_key = str(row.get("refKey") or "")
        doc = docs_by_ref.get(ref_key) or {}
        if not doc:
            continue
        touched += 1
        raw_comment = str(doc.get("Комментарий") or "")
        row["additionalInfoFirstLine"] = first_line(raw_comment) or row.get("additionalInfoFirstLine") or ""
        apply_runtime_defaults(row)

    refreshed.sort(key=lambda x: x.get("createdAt", ""), reverse=True)
    saved = save_rows(
        refreshed,
        refresh_started_at=refresh_started_at,
        write_source="comment-first-line-refresh",
    )
    if not saved:
        latest_rows = load_fresh_runtime_rows()
        if latest_rows:
            _cached_rows = latest_rows
            _cached_fp = rows_fingerprint(latest_rows)
        _last_refresh_error = "comment refresh skipped: newer runtime snapshot already exists"
        log(_last_refresh_error)
        return {"ok": False, "error": _last_refresh_error}
    _cached_rows = refreshed
    _cached_fp = rows_fingerprint(refreshed)
    _last_refresh = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")
    _last_refresh_error = None
    log(f"comment-first-line refresh success: touched={touched}, rows={len(refreshed)}")
    return {"ok": True, "touched": touched, "rows": len(refreshed)}


def cache_is_stale() -> bool:
    if not _last_refresh:
        return True
    try:
        last = datetime.strptime(_last_refresh, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return True
    age = (datetime.now() - last).total_seconds()
    return age >= STALE_REFRESH_AFTER_SECONDS


async def trigger_refresh_if_stale() -> None:
    if not cache_is_stale():
        return
    task = getattr(app.state, "on_demand_refresh_task", None)
    if task and not task.done():
        return
    app.state.on_demand_refresh_task = asyncio.create_task(asyncio.to_thread(refresh_cache_and_file))


async def refresh_loop() -> None:
    # Wait before first refresh to allow Render health-check to pass.
    # Render health-check timeout is ~30s, so we delay well after that.
    await asyncio.sleep(35)
    while True:
        started_at = time.time()
        try:
            await asyncio.to_thread(refresh_cache_and_file)
        except Exception as exc:
            log(f"refresh loop error: {type(exc).__name__}: {exc}")
        elapsed = max(0.0, time.time() - started_at)
        log(f"refresh loop tick finished in {elapsed:.1f}s")
        await asyncio.sleep(REFRESH_SECONDS)


async def fast_partial_refresh_loop() -> None:
    # Wait before first refresh to allow Render health-check to pass.
    # Render health-check timeout is ~30s, so we delay well after that.
    await asyncio.sleep(37)
    while True:
        started_at = time.time()
        try:
            await asyncio.to_thread(refresh_cached_rows_only)
        except Exception as exc:
            log(f"fast partial loop error: {type(exc).__name__}: {exc}")
        elapsed = max(0.0, time.time() - started_at)
        log(f"fast partial loop tick finished in {elapsed:.1f}s")
        await asyncio.sleep(FAST_PARTIAL_REFRESH_SECONDS)


@app.on_event("startup")
async def on_startup() -> None:
    global _cached_rows, _cached_fp, _last_refresh, _order_to_kp_cache
    if ADMIN_SESSION_SECRET_IS_EPHEMERAL:
        log("WARNING: ADMIN_SESSION_SECRET is not configured; using ephemeral runtime secret")
    if USER_SESSION_SECRET_IS_EPHEMERAL:
        log("WARNING: USER_SESSION_SECRET is not configured; using ephemeral runtime secret")

    # Load from disk synchronously — fast, no network. This is enough for Render health check.
    _cached_rows = load_fresh_runtime_rows()
    if not _cached_rows:
        _cached_rows = load_seed_rows()
    _cached_fp = rows_fingerprint(_cached_rows)
    _last_refresh = None

    # All blocking network operations (GitHub sync, 1C enrich) run in a background task
    # so startup completes immediately and Render health check passes without delay.
    async def _post_startup_sync():
        global _cached_rows, _cached_fp
        try:
            await asyncio.to_thread(_sync_confirmed_runtime_cache_from_github_if_needed, "startup", True)
        except Exception as exc:
            log(f"[startup] GitHub sync failed (non-blocking): {type(exc).__name__}: {exc}")
        try:
            headers = _build_headers()
            await asyncio.to_thread(_enrich_group_flags_bulk, _cached_rows, headers)
            _cached_fp = rows_fingerprint(_cached_rows)
            log(f"[startup] enriched {len(_cached_rows)} rows with group flags")
            # Persist enriched rows locally so the next GitHub sync doesn't overwrite
            # paymentReceived/invoiceCreated values that were just set by enrichment.
            try:
                meta = _read_runtime_meta() or {"generatedAt": datetime.now(timezone.utc).isoformat(), "rowCount": len(_cached_rows)}
                pointer = _build_runtime_current_pointer(_cached_rows, meta)
                _write_local_confirmed_runtime(_cached_rows, meta, pointer)
                log(f"[startup] enriched rows saved to local confirmed runtime (v{pointer.get('version')})")
            except Exception as save_exc:
                log(f"[startup] saving enriched rows failed (non-blocking): {type(save_exc).__name__}: {save_exc}")
        except Exception as exc:
            log(f"[startup] group flags enrichment failed (non-blocking): {type(exc).__name__}: {exc}")

    app.state.post_startup_task = asyncio.create_task(_post_startup_sync())

    # Do NOT await refresh here: blocking startup prevents health-check from reaching the app.
    # Background refresh loops are optional and disabled by default.
    if ENABLE_BACKGROUND_REFRESH:
        app.state.refresh_task = asyncio.create_task(refresh_loop())
        app.state.fast_partial_refresh_task = asyncio.create_task(fast_partial_refresh_loop())
        log("background refresh loops enabled")
    else:
        app.state.refresh_task = None
        app.state.fast_partial_refresh_task = None
        log("background refresh loops disabled: waiting for manual refresh")


@app.on_event("shutdown")
async def on_shutdown() -> None:
    for attr in ("refresh_task", "fast_partial_refresh_task"):
        task = getattr(app.state, attr, None)
        if task:
            task.cancel()


@app.get("/")
async def root(request: Request):
    try:
        user = _get_user_from_request(request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    role = str(user.get("role") or "manager").strip().lower()
    return RedirectResponse(url="/admin/dashboard" if role == "admin" else "/dashboard", status_code=302)


@app.get("/login")
async def login_page():
    return FileResponse(
        "login.html",
        media_type="text/html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/logout")
async def logout_page():
    response = RedirectResponse(url="/login", status_code=302)
    _clear_session_cookies(response)
    return response


@app.get("/dashboard")
async def dashboard(request: Request):
    try:
        user = _get_user_from_request(request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    return FileResponse(
        "dashboard.html",
        media_type="text/html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/admin/dashboard")
async def admin_dashboard(request: Request):
    # Prefer explicit admin session cookie to avoid mixed-cookie downgrade
    # (e.g. old manager user-session + fresh admin-session in same browser).
    admin_username = _get_admin_username(request)
    if admin_username:
        user = {
            "username": admin_username,
            "role": "admin",
            "allowedManagers": "*",
        }
    else:
        try:
            user = _get_user_from_request(request)
        except HTTPException:
            return RedirectResponse(url="/login", status_code=302)

    role = str(user.get("role") or "manager").lower()
    if role != "admin":
        return RedirectResponse(url="/dashboard", status_code=302)

    response = FileResponse(
        "index.html",
        media_type="text/html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )

    # Keep frontend auth flows consistent: admin dashboard also gets user-session cookie.
    # This avoids blank UI when only admin cookie is present (e.g. after /api/admin/login).
    response.set_cookie(
        key=USER_SESSION_COOKIE,
        value=_issue_user_token(str(user.get("username") or "")),
        httponly=True,
        samesite="lax",
        secure=True,
        max_age=max(300, USER_SESSION_TTL_SECONDS),
    )

    return response


@app.get("/admin/rights")
async def admin_rights():
    return FileResponse("admin_rights.html", media_type="text/html")


@app.get("/healthz")
async def healthz():
    return {
        "ok": True,
        "rows": len(_cached_rows),
        "backgroundRefreshEnabled": ENABLE_BACKGROUND_REFRESH,
        "lastRefresh": _last_refresh,
        "lastRefreshError": _last_refresh_error,
        "lastCommentRefresh": _last_comment_refresh,
        "lastCommentRefreshError": _last_comment_refresh_error,
    }


@app.get("/api/kp/version-info")
async def kp_version_info(request: Request):
    _get_user_from_request(request)

    local_meta = _read_runtime_meta()
    local_pointer = _read_runtime_current_pointer()
    github_meta = _load_runtime_meta_from_github()
    github_pointer = _load_runtime_current_pointer_from_github()

    # Ensure all required fields exist with proper defaults
    if "cycleVersion" not in local_meta or not local_meta.get("cycleVersion"):
        local_meta["cycleVersion"] = 0
    if "last1cLoadedVersion" not in local_meta or not local_meta.get("last1cLoadedVersion"):
        local_meta["last1cLoadedVersion"] = 0
    if "last1cLoadedAt" not in local_meta or not local_meta.get("last1cLoadedAt"):
        local_meta["last1cLoadedAt"] = local_meta.get("generatedAt") or ""

    current_cycle_version = _to_int_or_none(local_pointer.get("version")) or _to_int_or_none(local_meta.get("cycleVersion")) or 0
    last_1c_loaded_version = _to_int_or_none(local_meta.get("last1cLoadedVersion")) or 0
    last_github_backup_version = _to_int_or_none(github_pointer.get("version")) or _to_int_or_none(github_meta.get("cycleVersion")) or 0

    return {
        "frontendRecommendedVersion": current_cycle_version,
        "currentRuntimeVersion": current_cycle_version,
        "last1cLoadedVersion": last_1c_loaded_version,
        "last1cLoadedAt": str(local_meta.get("last1cLoadedAt") or local_meta.get("generatedAt") or ""),
        "lastGithubBackupVersion": last_github_backup_version,
        "runtimeWriteSource": str(local_meta.get("writeSource") or local_pointer.get("writeSource") or ""),
        "githubWriteSource": str(github_pointer.get("writeSource") or github_meta.get("writeSource") or ""),
        "runtimeGeneratedAt": str(local_meta.get("generatedAt") or local_pointer.get("generatedAt") or ""),
        "githubGeneratedAt": str(github_pointer.get("generatedAt") or github_meta.get("generatedAt") or ""),
    }


@app.post("/api/kp/refresh")
async def manual_refresh(request: Request):
    username = "anonymous"
    try:
        user = _get_user_from_request(request)
        username = str(user.get("username") or "anonymous")
    except HTTPException:
        # Allow manual refresh even without a valid auth cookie.
        # This endpoint mutates only runtime cache, not access rights.
        pass

    client_host = request.client.host if request.client else "unknown"

    with _manual_refresh_state_lock:
        if _manual_refresh_state.get("running"):
            state = dict(_manual_refresh_state)
            return JSONResponse(
                status_code=202,
                content={
                    "ok": True,
                    "message": "manual refresh is already running",
                    **state,
                    "rows": len(_cached_rows),
                    "lastRefresh": _last_refresh,
                    "lastRefreshError": _last_refresh_error,
                },
            )

        _manual_refresh_state.update(
            {
                "running": True,
                "requestedAt": datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
                "requestedBy": username,
                "requestedFrom": client_host,
                "startedAt": None,
                "finishedAt": None,
                "lastError": None,
                "confirmedVersion": None,
            }
        )

    async def _run_manual_refresh() -> None:
        global _cached_rows, _cached_fp, _last_refresh, _last_refresh_error, _last_confirmed_runtime_sync_check

        _set_manual_refresh_state(startedAt=datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"))
        log(f"manual refresh requested by {username} from {client_host}")

        # Hard deadline: github push + readback add up to ~3 min on top of the main refresh.
        TOTAL_HARD_DEADLINE = max(120, MANUAL_REFRESH_TIMEOUT_SECONDS) + 300
        deadline_task: asyncio.Task | None = None

        async def _deadline_killer():
            await asyncio.sleep(TOTAL_HARD_DEADLINE)
            log(f"[refresh] hard deadline {TOTAL_HARD_DEADLINE}s reached — forcing running=False")
            _set_manual_refresh_state(
                running=False,
                finishedAt=datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
                lastOk=False,
                lastError="manual refresh hard deadline exceeded",
            )

        deadline_task = asyncio.create_task(_deadline_killer())

        previous_rows, previous_meta, previous_pointer = _load_confirmed_runtime_from_github()
        if not previous_rows:
            previous_rows = load_fresh_runtime_rows() or list(_cached_rows)
        if not previous_meta:
            previous_meta = _read_runtime_meta() or {
                "generatedAt": datetime.now(timezone.utc).isoformat(),
                "rowCount": len(previous_rows),
            }
        if not previous_pointer and previous_rows and previous_meta:
            try:
                previous_pointer = _build_runtime_current_pointer(previous_rows, previous_meta)
            except Exception:
                previous_pointer = {}

        previous_last_refresh = _last_refresh
        previous_last_refresh_error = _last_refresh_error
        previous_last_confirmed_sync_check = _last_confirmed_runtime_sync_check

        try:
            ran = await asyncio.wait_for(
                asyncio.to_thread(refresh_cache_and_file, True, True, 300, True, False, False),
                timeout=max(60, MANUAL_REFRESH_TIMEOUT_SECONDS),
            )

            if not ran:
                # Another refresh cycle was already running. Wait for it to complete
                # (up to the hard deadline), then pick up its result from disk.
                log("[refresh] cycle was skipped — waiting for running cycle to complete")
                wait_deadline = time.time() + max(120, MANUAL_REFRESH_TIMEOUT_SECONDS)
                while time.time() < wait_deadline:
                    await asyncio.sleep(5)
                    lock_free = _refresh_run_lock.acquire(blocking=False)
                    if lock_free:
                        _refresh_run_lock.release()
                        break
                log("[refresh] running cycle completed; proceeding to publish from disk")

            candidate_rows = load_rows_from_path(Path(RUNTIME_DATA_FILE))
            candidate_meta = _read_runtime_meta()
            github_rows, _, github_pointer = await asyncio.to_thread(
                _publish_confirmed_runtime_snapshot_or_raise,
                candidate_rows,
                candidate_meta,
            )
            _cached_rows = list(github_rows)
            _cached_fp = rows_fingerprint(_cached_rows)
            _last_confirmed_runtime_sync_check = time.time()
            _last_refresh_error = None
            _last_refresh = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")

            confirmed_version = github_pointer.get("version") if github_pointer else None
            _set_manual_refresh_state(confirmedVersion=confirmed_version, lastOk=True, lastError=None)
            log(
                "manual refresh finished: "
                f"rows={len(_cached_rows)}, lastRefresh={_last_refresh}, confirmedVersion={confirmed_version}, source=github-current, user={username}, host={client_host}"
            )
        except Exception as exc:
            _cached_rows = list(previous_rows)
            _cached_fp = rows_fingerprint(_cached_rows)
            _write_local_confirmed_runtime(_cached_rows, previous_meta, previous_pointer)
            _last_refresh = previous_last_refresh
            _last_refresh_error = previous_last_refresh_error
            _last_confirmed_runtime_sync_check = previous_last_confirmed_sync_check

            if isinstance(exc, asyncio.TimeoutError):
                _set_manual_refresh_state(
                    confirmedVersion=previous_pointer.get("version") if previous_pointer else None,
                    lastOk=False,
                    lastError="manual refresh timed out",
                )
                log("manual refresh timed out")
            else:
                _set_manual_refresh_state(
                    confirmedVersion=previous_pointer.get("version") if previous_pointer else None,
                    lastOk=False,
                    lastError=str(exc),
                )
                log(f"manual refresh crashed and kept previous confirmed snapshot: {type(exc).__name__}: {exc}")
        finally:
            if deadline_task and not deadline_task.done():
                deadline_task.cancel()
            _set_manual_refresh_state(
                running=False,
                finishedAt=datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
            )

    asyncio.create_task(_run_manual_refresh())
    return JSONResponse(
        status_code=202,
        content={
            "ok": True,
            "message": "manual refresh started",
            **_manual_refresh_snapshot(),
        },
    )


@app.get("/api/kp/refresh/status")
async def manual_refresh_status():
    return _manual_refresh_snapshot()


@app.post("/api/debug/comments-only-refresh")
async def debug_comments_only_refresh():
    result = await asyncio.to_thread(refresh_cached_rows_only)
    return result


@app.get("/api/debug/logs")
async def debug_logs():
    return {"lines": list(_log_buffer)}


@app.get("/api/debug/runtime-state")
async def debug_runtime_state():
    """Show current runtime cache state and GitHub pointer for diagnostics."""
    local_pointer = _read_runtime_current_pointer()
    local_meta = _read_runtime_meta()
    github_pointer = await asyncio.to_thread(_load_runtime_current_pointer_from_github)
    return {
        "cachedRows": len(_cached_rows),
        "cachedFp": _cached_fp[:12] if _cached_fp else None,
        "lastRefresh": _last_refresh,
        "lastRefreshError": _last_refresh_error,
        "manualRefreshState": dict(_manual_refresh_state),
        "localMeta": local_meta,
        "localPointer": local_pointer,
        "githubPointer": github_pointer,
        "githubRuntimeBranch": _github_runtime_ref(),
        "githubRuntimeCurrentPath": GITHUB_RUNTIME_CURRENT_PATH,
        "githubRuntimeVersionsDir": GITHUB_RUNTIME_VERSIONS_DIR,
        "githubRepo": GITHUB_REPO,
        "githubTokenSet": bool(GITHUB_TOKEN),
        "logTail": list(_log_buffer)[-30:],
    }


@app.get("/api/debug/orders-test")
async def debug_orders_test():
    """Test Document_ЗаказКлиента fetch strategies for KP 229."""
    KP_REF = "6c133ed3-2290-11f1-8d55-bc97e15eb091"
    result: dict = {"kpRef": KP_REF, "steps": []}
    headers = _build_headers()
    entity = "Document_ЗаказКлиента"

    def _run():
        # Step 1: $count
        try:
            r = requests.get(f"{BASE}/{entity}/$count", headers=headers, timeout=15, verify=False)
            total = int(r.text.strip()) if r.status_code == 200 else 0
            result["steps"].append({"step": "count", "status": r.status_code, "total": total})
        except Exception as e:
            result["steps"].append({"step": "count", "error": str(e)})
            return

        # Step 2: top=50 skip=0 (first page, no orderby)
        try:
            r2 = requests.get(f"{BASE}/{entity}", headers=headers,
                params={"$select": "Ref_Key,Date,Number,ДокументОснование,ДокументОснование_Type",
                        "$top": "50", "$skip": "0"},
                timeout=20, verify=False)
            items = r2.json().get("value", []) if r2.ok else []
            matches = [i for i in items if i.get("ДокументОснование") == KP_REF]
            result["steps"].append({"step": "top50_skip0", "status": r2.status_code,
                                     "fetched": len(items), "matches": len(matches),
                                     "sample_date": items[0].get("Date") if items else None})
        except Exception as e:
            result["steps"].append({"step": "top50_skip0", "error": str(e)})

        # Step 3: orderby Date desc top=200 skip=0
        try:
            r3 = requests.get(f"{BASE}/{entity}", headers=headers,
                params={"$select": "Ref_Key,Date,Number,ДокументОснование,ДокументОснование_Type",
                        "$top": "200", "$skip": "0", "$orderby": "Date desc"},
                timeout=30, verify=False)
            items3 = r3.json().get("value", []) if r3.ok else []
            matches3 = [i for i in items3 if i.get("ДокументОснование") == KP_REF]
            result["steps"].append({"step": "orderby_date_desc_top200", "status": r3.status_code,
                                     "fetched": len(items3), "matches": len(matches3),
                                     "matched_numbers": [i.get("Number") for i in matches3],
                                     "first_date": items3[0].get("Date") if items3 else None})
        except Exception as e:
            result["steps"].append({"step": "orderby_date_desc_top200", "error": str(e)})

    await asyncio.to_thread(_run)
    return result


@app.get("/api/debug/odata-test")
async def debug_odata_test():
    """Diagnostic endpoint: test OData connectivity step by step."""
    result: dict = {"steps": []}
    headers = _build_headers()

    # Step 1: $count
    try:
        r = requests.get(
            f"{BASE}/{ENTITY}/$count",
            headers=headers,
            timeout=30,
            verify=False,
        )
        result["steps"].append({
            "step": "$count",
            "status": r.status_code,
            "body": r.text[:200],
        })
        total_count = int(r.text.strip()) if r.status_code == 200 else 0
    except Exception as exc:
        result["steps"].append({"step": "$count", "error": str(exc)})
        return result

    # Step 2: fetch last page (same logic as fetch_rows_from_odata)
    page_size = 300
    skip = ((total_count - 1) // page_size) * page_size if total_count > 0 else 0
    try:
        r2 = requests.get(
            f"{BASE}/{ENTITY}",
            headers=headers,
            params={
                "$select": "Ref_Key,Number,Date",
                "$top": str(page_size),
                "$skip": str(skip),
            },
            timeout=120,
            verify=False,
        )
        batch = r2.json().get("value", []) if r2.status_code == 200 else []
        result["steps"].append({
            "step": f"fetch skip={skip}",
            "status": r2.status_code,
            "batchLen": len(batch),
            "firstNumber": batch[0].get("Number") if batch else None,
            "lastNumber": batch[-1].get("Number") if batch else None,
        })
    except Exception as exc:
        result["steps"].append({"step": f"fetch skip={skip}", "error": str(exc)})

    # Step 3: try full fetch with Cyrillic fields (same as fetch_rows_from_odata)
    try:
        r3 = requests.get(
            f"{BASE}/{ENTITY}",
            headers=headers,
            params={
                "$select": "Ref_Key," + ",".join(LIGHT_SELECT_FIELDS),
                "$top": str(page_size),
                "$skip": str(skip),
            },
            timeout=120,
            verify=False,
        )
        if r3.status_code == 200:
            batch3 = r3.json().get("value", [])
            matched = 0
            for it in batch3:
                dt_raw = it.get("Date", "")
                try:
                    dt = datetime.fromisoformat(str(dt_raw).replace("Z", "+00:00")).replace(tzinfo=None)
                    if TARGET_START <= dt <= TARGET_END:
                        matched += 1
                except Exception:
                    pass
            result["steps"].append({
                "step": "fetch_full_fields",
                "status": r3.status_code,
                "batchLen": len(batch3),
                "matchedInRange": matched,
            })
        else:
            result["steps"].append({
                "step": "fetch_full_fields",
                "status": r3.status_code,
                "body": r3.text[:300],
            })
    except Exception as exc:
        result["steps"].append({"step": "fetch_full_fields", "error": str(exc)})

    # Step 4: report config
    result["config"] = {
        "BASE": BASE,
        "ENTITY": ENTITY,
        "TARGET_START": str(TARGET_START),
        "TARGET_END": str(TARGET_END),
        "totalCount": total_count,
        "REFRESH_SECONDS": REFRESH_SECONDS,
        "lastRefresh": _last_refresh,
        "lastRefreshError": _last_refresh_error,
    }
    return result


@app.get("/version")
async def version():
    return {
        "ok": True,
        "commit": APP_COMMIT_SHA or None,
        "branch": APP_BRANCH or None,
        "startedAt": _app_started_at,
    }


@app.get("/render-status")
async def render_status():
    with _render_status_lock:
        cached_at = _render_status_cache.get("updatedAt")
        if cached_at and (time.time() - cached_at) < RENDER_STATUS_TTL:
            return {
                "status": _render_status_cache["status"],
                "updatedAt": _render_status_cache["updatedAt_iso"],
            }

    if not RENDER_API_KEY:
        return {"status": "unknown", "updatedAt": None, "error": "RENDER_API_KEY not set"}

    try:
        resp = requests.get(
            "https://api.render.com/v1/services",
            params={"name": RENDER_SERVICE_NAME, "limit": "1"},
            headers={"Authorization": f"Bearer {RENDER_API_KEY}", "Accept": "application/json"},
            timeout=8,
        )
        resp.raise_for_status()
        data = resp.json()
        status = "unknown"

        if isinstance(data, list) and data:
            first = data[0] if isinstance(data[0], dict) else {}
            if isinstance(first.get("service"), dict):
                status = str(first["service"].get("status") or "unknown")
            else:
                status = str(first.get("status") or "unknown")
        elif isinstance(data, dict):
            # Some API variants wrap items under "services" or return a single service object.
            services = data.get("services")
            if isinstance(services, list) and services:
                first = services[0] if isinstance(services[0], dict) else {}
                status = str(first.get("status") or "unknown")
            else:
                status = str(data.get("status") or "unknown")
    except Exception as exc:
        log(f"[render-status] error: {exc}")
        return {"status": "error", "updatedAt": None, "error": str(exc)}

    now_iso = datetime.now().isoformat()
    with _render_status_lock:
        _render_status_cache["status"] = status
        _render_status_cache["updatedAt"] = time.time()
        _render_status_cache["updatedAt_iso"] = now_iso

    return {"status": status, "updatedAt": now_iso}


def format_row_for_client(row: dict) -> dict:
    """Format row for API response: remove ПСУТ- prefix and time from date."""
    formatted = row.copy()
    if "number" in formatted:
        number = str(formatted["number"]).replace("ПСУТ-", "")
        formatted["number"] = number.lstrip("0") or "0"
    if "createdAt" in formatted:
        formatted["createdAt"] = formatted["createdAt"].split(" ")[0]
    return formatted


def build_rows_with_computed_status(rows: list[dict]) -> list[dict]:
    with _status_rules_lock:
        rules_text = load_status_rules_text()
    rules = _parse_status_rules_text(rules_text)

    output = []
    for row in rows:
        formatted = format_row_for_client(row)
        formatted["statusKpComputed"] = _compute_status_for_row(formatted, rules)
        output.append(formatted)
    return output


@app.get("/api/kp/all")
async def get_all_kp(request: Request):
    user = _get_user_from_request(request)
    _sync_confirmed_runtime_cache_from_github_if_needed("api-kp-all")
    if not _cached_rows:
        raise HTTPException(status_code=503, detail="KP data is not available yet")

    return build_rows_with_computed_status(_filter_rows_for_user(_cached_rows, user))


@app.get("/api/debug/kp/{kp_number}/payment-chain")
async def debug_kp_payment_chain(kp_number: str):
    normalized_input = _normalize_kp_number(kp_number)
    if not normalized_input:
        raise HTTPException(status_code=400, detail="kp_number is required")

    if not _cached_rows:
        raise HTTPException(status_code=503, detail="KP data is not available yet")

    target_row = None
    for row in _cached_rows:
        if _normalize_kp_number(row.get("number") or "") == normalized_input:
            target_row = row
            break

    if not target_row:
        raise HTTPException(status_code=404, detail=f"KP {kp_number} not found in cache")

    headers = _build_headers()
    kp_ref = str(target_row.get("refKey") or "").strip()
    if not kp_ref:
        kp_ref = await asyncio.to_thread(
            _find_kp_ref_by_number,
            str(target_row.get("number") or kp_number),
            headers,
        )

    if not kp_ref:
        raise HTTPException(status_code=404, detail=f"KP {kp_number} refKey not found in 1C")

    trace = await asyncio.to_thread(_trace_kp_group_chain, kp_ref, headers)

    return {
        "ok": True,
        "inputKpNumber": kp_number,
        "kp": {
            "refKey": kp_ref,
            "number": target_row.get("number"),
            "invoiceCreated": target_row.get("invoiceCreated"),
            "paymentReceived": target_row.get("paymentReceived"),
            "statusKp": target_row.get("statusKp"),
        },
        "trace": trace,
    }


@app.get("/api/status-rules")
async def get_status_rules():
    with _status_rules_lock:
        rules_text = load_status_rules_text()

    path = _status_rules_path()
    updated_at = None
    try:
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            updated_at = str(payload.get("updatedAt") or "") or None
    except Exception:
        updated_at = None

    return {
        "rulesText": rules_text,
        "updatedAt": updated_at,
    }


@app.post("/api/admin/login")
async def admin_login(payload: AdminLoginPayload):
    username = str(payload.username or "").strip()
    password = str(payload.password or "")
    if username != ADMIN_USER or not _admin_password_ok(password):
        raise HTTPException(status_code=401, detail="Invalid admin credentials")

    token = _issue_admin_token(username)
    response = JSONResponse({"ok": True, "username": username})
    response.set_cookie(
        key=ADMIN_SESSION_COOKIE,
        value=token,
        httponly=True,
        samesite="lax",
        secure=True,
        max_age=max(300, ADMIN_SESSION_TTL_SECONDS),
    )
    return response


@app.post("/api/admin/logout")
async def admin_logout():
    response = JSONResponse({"ok": True})
    _clear_session_cookies(response)
    return response


@app.get("/api/admin/payment-match-table")
async def admin_payment_match_table(request: Request):
    # Accept either an admin-session cookie (from /admin/rights login)
    # or a user-session cookie with role=admin (from /admin/dashboard login).
    ok = False
    if _get_admin_username(request):
        ok = True
    else:
        try:
            user = _get_user_from_request(request)
            if str(user.get("role") or "").lower() == "admin":
                ok = True
        except HTTPException:
            pass
    if not ok:
        raise HTTPException(status_code=401, detail="Admin auth required")
    headers = _build_headers()
    result = await asyncio.to_thread(_build_payment_match_table, headers)
    return {"ok": True, **result}


@app.post("/api/admin/seed-payment")
async def admin_seed_payment(request: Request):
    """Fetch a payment document from 1C by its short number and add it to the persistent seed cache.
    Body: {"payNumber": "76"}
    """
    ok = False
    if _get_admin_username(request):
        ok = True
    else:
        try:
            user = _get_user_from_request(request)
            if str(user.get("role") or "").lower() == "admin":
                ok = True
        except HTTPException:
            pass
    if not ok:
        raise HTTPException(status_code=401, detail="Admin auth required")

    body = await request.json()
    pay_number_raw = str(body.get("payNumber") or "").strip()
    if not pay_number_raw:
        raise HTTPException(status_code=400, detail="payNumber is required")

    pay_short = "".join(ch for ch in pay_number_raw if ch.isdigit()).lstrip("0") or pay_number_raw

    def _fetch_and_seed() -> dict:
        headers = _build_headers()
        # Scan payment pages to find the one with this number.
        pages, complete = _collect_tail_pages(
            "Document_ПоступлениеБезналичныхДенежныхСредств",
            headers,
            ["Ref_Key", "Number", "Date", "НазначениеПлатежа"],
            page_size=200,
            timeout=max(GROUP_CHECK_TIMEOUT_SECONDS, 60.0),
        )
        found = None
        for batch in pages:
            for item in batch:
                raw = str(item.get("Number") or "")
                short = "".join(ch for ch in raw if ch.isdigit()).lstrip("0") or raw
                if short == pay_short:
                    found = item
                    break
            if found:
                break

        if not found:
            return {"found": False, "payShort": pay_short, "scanComplete": complete}

        purpose = str(found.get("НазначениеПлатежа") or "")
        purpose_nums: list[str] = []
        for m in re.finditer(r"(?:[а-яa-z]*ут[\s\-_/]*|№\s*)0*(\d+)", purpose.lower()):
            d = m.group(1).lstrip("0") or "0"
            if d and d != "0":
                purpose_nums.append(d)

        entry = {
            "payShort": pay_short,
            "purpose": purpose,
            "purposeNums": purpose_nums,
        }

        global _payment_seed, _payment_seed_loaded
        _load_payment_seed()
        with _payment_seed_lock:
            # Replace existing entry with same payShort, or append.
            _payment_seed = [e for e in _payment_seed if str(e.get("payShort") or "") != pay_short]
            _payment_seed.append(entry)
        _save_payment_seed()
        log(f"[payment-seed] saved payment {pay_short}: purposeNums={purpose_nums}")
        return {"found": True, "payShort": pay_short, "purpose": purpose, "purposeNums": purpose_nums, "scanComplete": complete}

    result = await asyncio.to_thread(_fetch_and_seed)
    return {"ok": True, **result}


@app.get("/api/admin/session")
async def admin_session(request: Request):
    username = _get_admin_username(request)
    return {"ok": bool(username), "username": username}


@app.post("/api/auth/login")
async def user_login(payload: UserLoginPayload):
    username = str(payload.username or "").strip()
    password = str(payload.password or "")
    if not _user_password_ok(username, password):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    user = _resolve_effective_user(username)
    if not user:
        raise HTTPException(status_code=401, detail="User access is not configured")

    token = _issue_user_token(str(user.get("username") or username))
    response = JSONResponse(
        {
            "ok": True,
            "user": {
                "username": user.get("username"),
                "role": user.get("role"),
            },
        }
    )
    response.set_cookie(
        key=USER_SESSION_COOKIE,
        value=token,
        httponly=True,
        samesite="lax",
        secure=True,
        max_age=max(300, USER_SESSION_TTL_SECONDS),
    )
    return response


@app.post("/api/auth/logout")
async def user_logout():
    response = JSONResponse({"ok": True})
    _clear_session_cookies(response)
    return response


@app.get("/api/auth/session")
async def user_session(request: Request):
    try:
        user = _get_user_from_request(request)
        return {
            "ok": True,
            "user": {
                "username": user.get("username"),
                "role": user.get("role"),
                "allowedManagers": user.get("allowedManagers"),
            },
        }
    except HTTPException:
        return {"ok": False, "user": None}


@app.get("/api/admin/rights")
async def admin_get_rights(request: Request):
    _require_admin(request)
    return load_access_rights()


@app.put("/api/admin/rights")
async def admin_put_rights(payload: AccessRightsPayload, request: Request):
    _require_admin(request)
    saved = save_access_rights(payload.users)
    asyncio.create_task(asyncio.to_thread(_push_access_rights_to_github, saved))
    return {"ok": True, **saved}


@app.put("/api/status-rules")
async def put_status_rules(payload: StatusRulesPayload):
    text = str(payload.rulesText or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="rulesText must not be empty")

    updated_at = datetime.now().isoformat()
    try:
        with _status_rules_lock:
            save_status_rules_text(text)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to save rules: {exc}")

    # Push to GitHub in background so it survives the next deploy
    asyncio.create_task(asyncio.to_thread(_push_rules_to_github, text, updated_at))

    return {
        "ok": True,
        "updatedAt": updated_at,
    }


@app.post("/api/kp/new-request")
async def create_kp_from_new_request(payload: NewRequestPayload, request: Request):
    _get_user_from_request(request)
    request_text = str(payload.requestText or "").strip()
    if len(request_text) < 3:
        raise HTTPException(status_code=400, detail="Request text is too short")

    result = await asyncio.to_thread(_create_kp_in_1c_from_request, request_text)
    await asyncio.to_thread(refresh_cache_and_file)
    return result


@app.websocket("/ws/kp")
async def ws_kp(websocket: WebSocket):
    user = _get_user_from_websocket(websocket)
    if not user:
        await websocket.close(code=4401)
        return

    await websocket.accept()
    previous_fp = ""

    try:
        while True:
            _sync_confirmed_runtime_cache_from_github_if_needed("ws-kp")
            if not _cached_rows:
                await asyncio.sleep(2)
                continue

            current_fp = rows_fingerprint(_cached_rows)
            if current_fp != previous_fp:
                previous_fp = current_fp
                await websocket.send_json(
                    {
                        "type": "rows",
                        "updatedAt": _last_refresh,
                        "rows": build_rows_with_computed_status(_filter_rows_for_user(_cached_rows, user)),
                    }
                )
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        return


app.mount("/", StaticFiles(directory=".", html=True), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
