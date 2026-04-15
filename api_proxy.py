#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import base64
import collections
import hashlib
import json
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Optional

import requests
import urllib3
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
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
STATUS_RULES_FILE = os.getenv("STATUS_RULES_FILE", "data/status_rules.json")
SEED_MAX_AGE_SECONDS = int(os.getenv("SEED_MAX_AGE_SECONDS", "600"))
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "10"))
STALE_REFRESH_AFTER_SECONDS = int(os.getenv("STALE_REFRESH_AFTER_SECONDS", "20"))
ENRICH_PER_REFRESH = int(os.getenv("ENRICH_PER_REFRESH", "60"))
FORCE_INFO_REFRESH_TOP_ROWS = int(os.getenv("FORCE_INFO_REFRESH_TOP_ROWS", "20"))
GROUP_ENRICH_INTERVAL_SECONDS = int(os.getenv("GROUP_ENRICH_INTERVAL_SECONDS", "300"))
DOC_TIMEOUT_SECONDS = float(os.getenv("DOC_TIMEOUT_SECONDS", "1.5"))
NAV_TIMEOUT_SECONDS = float(os.getenv("NAV_TIMEOUT_SECONDS", "0.8"))
GROUP_CHECK_TIMEOUT_SECONDS = float(os.getenv("GROUP_CHECK_TIMEOUT_SECONDS", "8"))
NAV_LINK_LIMIT = int(os.getenv("NAV_LINK_LIMIT", "4"))
STATUS_KP_PROPERTY_KEY = os.getenv(
    "STATUS_KP_PROPERTY_KEY",
    "e1c7a0e4-4f8d-11f0-8d50-bc97e15eb091",
)
RENDER_API_KEY = os.getenv("RENDER_API_KEY", "")
RENDER_SERVICE_NAME = os.getenv("RENDER_SERVICE_NAME", "onec-kp-realtime")
RENDER_STATUS_TTL = int(os.getenv("RENDER_STATUS_TTL", "30"))
KEEP_ALIVE_INTERVAL = int(os.getenv("KEEP_ALIVE_INTERVAL", "300"))  # self-ping every 5 min
STATUS_RULES_TEXT_ENV = os.getenv("STATUS_RULES_TEXT", "").strip()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
GITHUB_REPO = os.getenv("GITHUB_REPO", "pavel9619229-cmyk/KP").strip()
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main").strip()
GITHUB_RULES_PATH = os.getenv("GITHUB_RULES_PATH", "data/status_rules.json").strip()
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
_last_group_enrich = None

_TZ_MSK = timezone(timedelta(hours=3))
_app_started_at = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")
_customer_name_cache = {}
_additional_info_cache = {}
_status_kp_value_cache = {}
_status_kp_catalog_value_key_cache = {}
_manager_filled_cache = {}
_product_specified_cache = {}
_kp_sent_cache = {}
_receipt_confirmed_cache = {}
_edo_sent_cache = {}
_rejected_cache = {}
_problem_cache = {}
_shipment_pending_cache = {}
_refresh_lock = threading.Lock()
_render_status_cache: dict = {"status": None, "updatedAt": None}
_last_cache_push: Optional[datetime] = None
CACHE_PUSH_MIN_INTERVAL = 3600  # push runtime cache to GitHub at most once per hour
_render_status_lock = threading.Lock()
_status_rules_lock = threading.Lock()
_enrich_cursor = 0

DEFAULT_STATUS_RULES_TEXT = """# Формат 1 (простой):
# статус СТАТУС устанавливается, если Поле - ДА, Поле - НЕТ
#
# Поля:
# Проблема, Отказ, Накладная создана, Оплата получена,
# В ЭДО отправлено, Отгрузить, Клиент КП увидел, КП отправлено,
# Клиент заполнен, Менеджер заполнен, Товар указан
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
статус ОТПРАВИТЬ КЛИЕНТУ устанавливается, если Клиент заполнен - ДА, Менеджер заполнен - ДА, Товар указан - ДА
"""

ZERO_GUID = "00000000-0000-0000-0000-000000000000"
UNKNOWN_MANAGER_NAME = os.getenv("UNKNOWN_MANAGER_NAME", "НЕ ОПРЕДЕЛЕН")
UNKNOWN_CUSTOMER_NAME = os.getenv("UNKNOWN_CUSTOMER_NAME", "НЕ ОПРЕДЕЛЕН")
NEW_REQUEST_STATUS_TEXT = os.getenv("NEW_REQUEST_STATUS_TEXT", "1. НОВЫЙ ЗАПРОС")

STORAGE_DEFAULTS = {
    "statusKp": "",
    "managerFilled": None,
    "productSpecified": None,
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


def _status_rules_path() -> Path:
    return Path(STATUS_RULES_FILE)


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


def _push_runtime_cache_to_github(rows: list) -> None:
    """Push kp_runtime_cache.json + kp_runtime_meta.json to GitHub so enriched
    data survives the next Render deploy. Throttled to once per hour. [skip ci]."""
    global _last_cache_push

    if not GITHUB_TOKEN or not GITHUB_REPO:
        return

    now = datetime.now()
    if _last_cache_push and (now - _last_cache_push).total_seconds() < CACHE_PUSH_MIN_INTERVAL:
        return  # throttled

    gh_headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    def _push_one(file_path: str, content_bytes: bytes, label: str) -> None:
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
            else:
                log(f"GitHub cache push failed ({label}): HTTP {r.status_code}: {r.text[:200]}")
        except Exception as exc:
            log(f"GitHub cache push error ({label}): {exc}")

    try:
        cache_bytes = json.dumps(rows, ensure_ascii=False, indent=2).encode("utf-8")
        _push_one("data/kp_runtime_cache.json", cache_bytes, "kp_runtime_cache.json")
        meta = {"generatedAt": now.isoformat(), "rowCount": len(rows)}
        meta_bytes = json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8")
        _push_one("data/kp_runtime_meta.json", meta_bytes, "kp_runtime_meta.json")
        _last_cache_push = now
    except Exception as exc:
        log(f"GitHub cache push unexpected error: {exc}")


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

    row["clientFilled"] = is_client_filled(row.get("customerName"))
    for key, default_value in STORAGE_DEFAULTS.items():
        if key not in row:
            row[key] = default_value
    return row


def apply_runtime_defaults(row: dict) -> dict:
    row["clientFilled"] = is_client_filled(row.get("customerName"))
    return row


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

    manager_key = str(row.get("Менеджер_Key") or "").strip()
    if not manager_key or manager_key == ZERO_GUID:
        _manager_filled_cache[ref_key] = False
        return False

    nav_link = str(row.get("Менеджер@navigationLinkUrl") or "").strip()
    if not nav_link:
        _manager_filled_cache[ref_key] = True
        return True

    try:
        nav_resp = requests.get(
            f"{BASE}/{nav_link}",
            headers=headers,
            timeout=NAV_TIMEOUT_SECONDS,
            verify=False,
        )
        if nav_resp.status_code == 200:
            nav_obj = nav_resp.json() if isinstance(nav_resp.json(), dict) else {}
            manager_name = str(nav_obj.get("Description") or "").strip()
            result = is_manager_filled(manager_name)
            _manager_filled_cache[ref_key] = result
            return result
    except Exception:
        pass

    _manager_filled_cache[ref_key] = True
    return True


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


def _parse_odata_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
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
    )
    if not orders_complete:
        # No confirmed fresh data from 1C: preserve current flags.
        return

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

    target_order_refs = set(order_to_kp.keys())
    if not target_order_refs:
        # Fresh scan confirmed that no linked orders exist in range.
        for row in rows:
            if row.get("refKey") in kp_ref_set:
                row["invoiceCreated"] = False
                row["paymentReceived"] = False
        return

    invoice_order_refs: set[str] = set()
    invoice_pages, invoices_complete = _collect_tail_pages(
        "Document_РеализацияТоваровУслуг",
        headers,
        ["Ref_Key", "Date", "ЗаказКлиента", "ЗаказКлиента_Type"],
    )
    if not invoices_complete:
        return

    for batch in invoice_pages:
        for item in batch:
            order_type = str(item.get("ЗаказКлиента_Type") or "")
            order_ref = str(item.get("ЗаказКлиента") or "")
            if order_type == "StandardODATA.Document_ЗаказКлиента" and order_ref in target_order_refs:
                invoice_order_refs.add(order_ref)

    payment_order_refs: set[str] = set()
    payment_pages, payments_complete, _ = _collect_tail_pages_with_field_fallback(
        "Document_ПоступлениеБезналичныхДенежныхСредств",
        headers,
        [
            [
                "Ref_Key",
                "Date",
                "ОбъектРасчетов_Key",
                "ДокументОснование",
                "ДокументОснование_Type",
                "НазначениеПлатежа",
                "РасшифровкаПлатежа",
            ],
            [
                "Ref_Key",
                "Date",
                "ОбъектРасчетов",
                "ДокументОснование",
                "ДокументОснование_Type",
                "НазначениеПлатежа",
                "РасшифровкаПлатежа",
            ],
            [
                "Ref_Key",
                "Date",
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
                "ДокументОснование",
                "ДокументОснование_Type",
                "НазначениеПлатежа",
                "РасшифровкаПлатежа",
            ],
            ["Ref_Key", "Date", "ОбъектРасчетов_Key", "ДокументОснование", "ДокументОснование_Type", "НазначениеПлатежа"],
            ["Ref_Key", "Date", "ОбъектРасчетов", "ДокументОснование", "ДокументОснование_Type", "НазначениеПлатежа"],
            ["Ref_Key", "Date", "ЗаказКлиента", "ЗаказКлиента_Type", "ДокументОснование", "ДокументОснование_Type", "НазначениеПлатежа"],
            ["Ref_Key", "Date", "ДокументОснование", "ДокументОснование_Type", "НазначениеПлатежа"],
        ],
        timeout=max(GROUP_CHECK_TIMEOUT_SECONDS, 12.0),
    )
    if not payments_complete and not payment_pages:
        return

    for batch in payment_pages:
        for item in batch:
            settlement_order = str(item.get("ОбъектРасчетов_Key") or item.get("ОбъектРасчетов") or "")
            if settlement_order in target_order_refs:
                payment_order_refs.add(settlement_order)
                continue

            direct_order = str(item.get("ЗаказКлиента") or "")
            direct_order_type = str(item.get("ЗаказКлиента_Type") or "")
            if direct_order in target_order_refs and (
                not direct_order_type or direct_order_type.endswith("Document_ЗаказКлиента")
            ):
                payment_order_refs.add(direct_order)
                continue

            base_type = str(item.get("ДокументОснование_Type") or "")
            base_ref = str(item.get("ДокументОснование") or "")
            if base_ref in target_order_refs and base_type.endswith("Document_ЗаказКлиента"):
                payment_order_refs.add(base_ref)
                continue

            breakdown_order_refs = _extract_order_refs_from_payment_breakdown(item)
            if breakdown_order_refs:
                matched_breakdown_ref = next((ref for ref in breakdown_order_refs if ref in target_order_refs), "")
                if matched_breakdown_ref:
                    payment_order_refs.add(matched_breakdown_ref)
                    continue

            # Fallback for 1C group documents: payment purpose often references short order number.
            # Match compact full order number first (e.g. "ПСУТ-000226" -> "псут000226"),
            # then explicit "...ут-<number>" style to avoid broad false positives.
            purpose = str(item.get("НазначениеПлатежа") or "").lower()
            if not purpose:
                continue
            purpose_compact = "".join(ch for ch in purpose if ch.isalnum())
            for order_ref, digits_trim in order_short_numbers.items():
                if order_ref in payment_order_refs:
                    continue
                compact_order = order_compact_numbers.get(order_ref, "")
                if compact_order and compact_order in purpose_compact:
                    payment_order_refs.add(order_ref)
                    break
                if re.search(rf"\b(?:[а-яa-z]*ут)[\s\-_/]*0*{re.escape(digits_trim)}\b", purpose):
                    payment_order_refs.add(order_ref)
                    break

    kp_invoice_map = {kp: False for kp in kp_ref_set}
    kp_payment_map = {kp: False for kp in kp_ref_set}

    for order_ref in invoice_order_refs:
        kp_ref = order_to_kp.get(order_ref)
        if kp_ref:
            kp_invoice_map[kp_ref] = True

    for order_ref in payment_order_refs:
        kp_ref = order_to_kp.get(order_ref)
        if kp_ref:
            kp_payment_map[kp_ref] = True

    for row in rows:
        kp_ref = row.get("refKey")
        if kp_ref in kp_ref_set:
            row["invoiceCreated"] = kp_invoice_map.get(kp_ref, False)
            if payments_complete:
                row["paymentReceived"] = kp_payment_map.get(kp_ref, False)
            elif kp_payment_map.get(kp_ref, False):
                # Partial payment scan: only upgrade to True; do not force False.
                row["paymentReceived"] = True


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
    if not meta_path.exists():
        log("runtime snapshot skipped: runtime metadata file does not exist")
        return []

    try:
        with meta_path.open("r", encoding="utf-8") as f:
            meta = json.load(f)
        generated_at_raw = str(meta.get("generatedAt") or "").strip()
        generated_at = datetime.fromisoformat(generated_at_raw)
        age_seconds = max(0, time.time() - generated_at.timestamp())
    except Exception as exc:
        log(f"runtime snapshot skipped: cannot read runtime metadata: {exc}")
        return []

    # Always load the runtime snapshot regardless of age on startup.
    # A stale-by-timestamp cache still has enriched flags that are far better
    # than falling back to the seed file (which has all-null flags).
    # The background refresh loop will update data immediately after startup.
    rows = load_rows_from_path(path)
    log(f"runtime snapshot loaded: {len(rows)} rows (age {int(age_seconds)}s)")
    return rows


def save_rows(rows: list) -> None:
    for row in rows:
        apply_storage_defaults(row)
    runtime_path = Path(RUNTIME_DATA_FILE)
    runtime_meta_path = Path(RUNTIME_META_FILE)
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_meta_path.parent.mkdir(parents=True, exist_ok=True)
    with runtime_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    with runtime_meta_path.open("w", encoding="utf-8") as f:
        json.dump({"generatedAt": datetime.now().isoformat()}, f, ensure_ascii=False, indent=2)


def build_known_rows_lookup() -> dict:
    known = {}
    for source_row in list(_cached_rows):
        number = source_row.get("number")
        if number and number not in known:
            known[number] = source_row
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
        timeout=30,
        verify=False,
    )
    resp.raise_for_status()
    return int(resp.text.strip())


def fetch_rows_from_odata() -> list:
    headers = _build_headers()
    known_rows = build_known_rows_lookup()

    rows = []
    page_size = 50
    _fetch_pages = 0
    _fetch_batches_empty = 0
    _fetch_network_errors = 0

    total_count = get_total_count(headers)
    if total_count <= 0:
        log(f"fetch_rows_from_odata: total_count={total_count}, aborting")
        return []

    skip = max(0, total_count - page_size)
    log(f"fetch_rows_from_odata: total_count={total_count}, starting skip={skip}, page_size={page_size}")

    while True:
        _fetch_pages += 1
        params = {
            "$select": "Ref_Key," + ",".join(LIGHT_SELECT_FIELDS),
            "$top": str(page_size),
            "$skip": str(skip),
        }

        resp = None
        for _ in range(3):
            try:
                resp = requests.get(
                    f"{BASE}/{ENTITY}",
                    headers=headers,
                    params=params,
                    timeout=120,
                    verify=False,
                )
                if resp.status_code == 200:
                    break
            except Exception as exc:
                _fetch_network_errors += 1
                log(f"fetch_rows_from_odata: error at skip={skip}: {type(exc).__name__}: {exc}")
                resp = None
            time.sleep(2)

        if resp is None or resp.status_code != 200:
            sc = resp.status_code if resp else "None"
            body_hint = ""
            if resp is not None:
                try:
                    body_hint = f" body={resp.text[:200]}"
                except Exception:
                    pass
            log(f"fetch_rows_from_odata: breaking at skip={skip}, status={sc}{body_hint}, pages={_fetch_pages}, net_errors={_fetch_network_errors}")
            break

        batch = resp.json().get("value", [])
        if not batch:
            _fetch_batches_empty += 1
            log(f"fetch_rows_from_odata: empty batch at skip={skip}, done")
            break

        batch_dates = []

        for item in batch:
            ref_key = item.get("Ref_Key") or ""
            values = [item.get(f) or "" for f in LIGHT_SELECT_FIELDS]
            number, dt_raw, status, requisites, comment = values[0], values[1], values[2], values[3], values[4]
            try:
                dt = datetime.fromisoformat(str(dt_raw).replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                continue

            batch_dates.append(dt)

            if TARGET_START <= dt <= TARGET_END:
                known_row = known_rows.get(number, {})

                # Detect changes in 1C requisites by hashing Статус + ДополнительныеРеквизиты.
                # If the hash changed compared to what we cached, force re-enrich this row.
                current_hash = hashlib.md5(
                    (str(status) + str(requisites)).encode("utf-8")
                ).hexdigest()[:10]
                requisites_changed = current_hash != known_row.get("statusHash", "")

                status_kp = known_row.get("statusKp", "")
                if not status_kp or requisites_changed:
                    status_kp = resolve_status_kp_from_requisites(requisites, headers)

                previous_info = known_row.get("additionalInfoFirstLine", "")
                current_info = first_line(comment)
                if current_info:
                    previous_info = current_info

                comment_clean = strip_html(str(comment or "")).replace("\r\n", "\n").replace("\r", "\n").upper()
                comment_top_lines = comment_clean.split("\n")[:5]
                kp_sent_from_comment = any("КП ОТПРАВЛЕНО" in line for line in comment_top_lines)
                receipt_from_comment = any("КЛИЕНТ КП УВИДЕЛ" in line for line in comment_top_lines)
                edo_from_comment = "В ЭДО ОТПРАВЛЕНО" in comment_clean
                problem_from_comment = "ПРОБЛЕМА" in comment_clean
                shipment_from_comment = "ОТГРУЗИТЬ" in comment_clean

                product_specified = known_row.get("productSpecified")
                if product_specified is not True and looks_like_product_hint(previous_info):
                    product_specified = True

                rejected_flag = known_row.get("rejected")
                if has_reject_marker(comment, status_kp):
                    rejected_flag = True
                elif "ОТКАЗ" in comment_clean:
                    rejected_flag = True
                else:
                    rejected_flag = False

                rows.append(
                    {
                        "refKey": str(ref_key),
                        "number": number,
                        "createdAt": dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "customerName": "" if requisites_changed else known_row.get("customerName", ""),
                        "status": status,
                        "managerFilled": known_row.get("managerFilled"),
                        "productSpecified": product_specified,
                        "kpSent": kp_sent_from_comment,
                        "receiptConfirmed": receipt_from_comment,
                        "edoSent": edo_from_comment,
                        "rejected": rejected_flag,
                        "problem": problem_from_comment,
                        "shipmentPending": shipment_from_comment,
                        "statusKp": status_kp,
                        "additionalInfoFirstLine": previous_info,
                        "invoiceCreated": known_row.get("invoiceCreated"),
                        "paymentReceived": known_row.get("paymentReceived"),
                        "statusHash": current_hash,
                    }
                )

        if batch_dates and min(batch_dates) < TARGET_START:
            break

        if skip == 0:
            break

        skip = max(0, skip - page_size)

    log(f"fetch_rows_from_odata: {len(rows)} matched rows, {_fetch_pages} pages, {_fetch_network_errors} net errors")
    rows.sort(key=lambda x: x["createdAt"], reverse=True)

    global _last_group_enrich
    _enrich_group_flags_bulk(rows, headers)
    _last_group_enrich = datetime.now()

    for index, row in enumerate(rows):
        ref_key = row.get("refKey", "")
        if not ref_key:
            continue

        need_customer = not (row.get("customerName") or "").strip()
        need_info = not (row.get("additionalInfoFirstLine") or "").strip()
        need_manager = row.get("managerFilled") is None
        need_product = row.get("productSpecified") is not True
        need_kp_sent = False
        need_receipt = False
        need_edo = False
        need_rejected = False
        need_problem = False
        need_shipment = False
        if not need_customer and not need_info and not need_manager and not need_product and not need_kp_sent and not need_receipt and not need_edo and not need_rejected and not need_problem and not need_shipment:
            continue

        doc = {}
        if need_customer or need_info or need_manager or need_product or need_kp_sent or need_receipt or need_edo or need_rejected or need_problem or need_shipment:
            doc = _fetch_doc_by_ref(ref_key, headers, timeout=DOC_TIMEOUT_SECONDS)
            if not doc and (need_customer or need_info or need_manager or need_product or need_kp_sent or need_receipt or need_edo or need_rejected or need_problem or need_shipment):
                continue

        if need_info:
            line = resolve_additional_info_for_ref(
                ref_key,
                headers,
                doc=doc,
                use_cache=True,
            )
            if line:
                row["additionalInfoFirstLine"] = line

        if need_customer:
            customer = resolve_customer_name_for_ref(
                ref_key,
                headers,
                doc=doc,
                use_cache=True,
            )
            if customer:
                row["customerName"] = customer

        if need_manager:
            manager_filled = resolve_manager_filled_for_ref(
                ref_key,
                headers,
                doc=doc,
                use_cache=True,
            )
            if manager_filled is not None:
                row["managerFilled"] = manager_filled

        if need_product:
            product_specified = resolve_product_specified_for_ref(
                ref_key,
                headers,
                doc=doc,
                use_cache=True,
            )
            if product_specified is not True and looks_like_product_hint(
                row.get("additionalInfoFirstLine") or doc.get("ДополнительнаяИнформация") or doc.get("Комментарий")
            ):
                product_specified = True
            if product_specified is not None:
                row["productSpecified"] = product_specified

        if need_kp_sent:
            kp_sent = resolve_kp_sent_for_ref(
                ref_key,
                headers,
                doc=doc,
                use_cache=False,
            )
            if kp_sent is not None:
                row["kpSent"] = kp_sent

        if need_receipt:
            receipt_confirmed = resolve_receipt_confirmed_for_ref(
                ref_key,
                headers,
                doc=doc,
                use_cache=False,
            )
            if receipt_confirmed is not None:
                row["receiptConfirmed"] = receipt_confirmed

        if need_edo:
            edo_sent = resolve_edo_sent_for_ref(
                ref_key,
                headers,
                doc=doc,
                use_cache=False,
            )
            if edo_sent is not None:
                row["edoSent"] = edo_sent

        if need_rejected:
            rejected = resolve_rejected_for_ref(
                ref_key,
                headers,
                doc=doc,
                use_cache=False,
            )
            if rejected is not None:
                row["rejected"] = rejected

        if need_problem:
            problem = resolve_problem_for_ref(
                ref_key,
                headers,
                doc=doc,
                use_cache=False,
            )
            if problem is not None:
                row["problem"] = problem

        if need_shipment:
            shipment_pending = resolve_shipment_pending_for_ref(
                ref_key,
                headers,
                doc=doc,
                use_cache=False,
            )
            if shipment_pending is not None:
                row["shipmentPending"] = shipment_pending

    for row in rows:
        apply_runtime_defaults(row)

    rows.sort(key=lambda x: x["createdAt"], reverse=True)
    return rows


def refresh_cache_and_file() -> None:
    global _cached_rows, _cached_fp, _last_refresh, _last_refresh_error

    with _refresh_lock:
        try:
            fetched = fetch_rows_from_odata()
            if fetched:
                save_rows(fetched)
                _cached_rows = fetched
                _cached_fp = rows_fingerprint(fetched)
                _last_refresh = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")
                _last_refresh_error = None
                log(f"refresh success: {len(fetched)} rows")
                # Disabled runtime cache auto-push to GitHub: it creates a deploy loop
                # on Render (new commit -> new deploy -> new commit...).
                return

            _last_refresh_error = "refresh returned 0 rows"
            log("refresh returned 0 rows, keeping last successful live cache")
        except Exception as exc:
            _last_refresh_error = str(exc)
            log(f"refresh failed, keeping last successful live cache: {exc}")


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
    while True:
        try:
            await asyncio.to_thread(refresh_cache_and_file)
        except Exception:
            pass
        await asyncio.sleep(REFRESH_SECONDS)


async def keep_alive_loop() -> None:
    """Self-ping to prevent Render free tier from sleeping after 15 min of inactivity."""
    render_url = os.getenv("RENDER_EXTERNAL_URL", "").strip()
    if not render_url:
        log("keep_alive: RENDER_EXTERNAL_URL not set, skipping self-ping")
        return
    ping_url = f"{render_url.rstrip('/')}/healthz"
    log(f"keep_alive: will ping {ping_url} every {KEEP_ALIVE_INTERVAL}s")
    while True:
        await asyncio.sleep(KEEP_ALIVE_INTERVAL)
        try:
            await asyncio.to_thread(
                lambda: requests.get(ping_url, timeout=10, verify=False)
            )
        except Exception as exc:
            log(f"keep_alive: ping failed: {exc}")


@app.on_event("startup")
async def on_startup() -> None:
    global _cached_rows, _cached_fp, _last_refresh
    _cached_rows = load_fresh_runtime_rows()
    if not _cached_rows:
        _cached_rows = load_seed_rows()
    _cached_fp = rows_fingerprint(_cached_rows)
    _last_refresh = None
    # Start refresh loop immediately — first iteration fires right away.
    # Do NOT await refresh here: blocking startup prevents Render's health-check
    # from reaching the app, causing the deploy to appear stuck.
    app.state.refresh_task = asyncio.create_task(refresh_loop())
    app.state.keep_alive_task = asyncio.create_task(keep_alive_loop())


@app.on_event("shutdown")
async def on_shutdown() -> None:
    for attr in ("refresh_task", "keep_alive_task"):
        task = getattr(app.state, attr, None)
        if task:
            task.cancel()


@app.get("/")
async def root():
    return FileResponse("index.html", media_type="text/html")


@app.get("/dashboard")
async def dashboard():
    return FileResponse("dashboard.html", media_type="text/html")


@app.get("/healthz")
async def healthz():
    return {
        "ok": True,
        "rows": len(_cached_rows),
        "lastRefresh": _last_refresh,
        "lastRefreshError": _last_refresh_error,
    }


@app.get("/api/debug/logs")
async def debug_logs():
    return {"lines": list(_log_buffer)}


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
    page_size = 50
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
            timeout=45,
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
            timeout=45,
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


@app.get("/api/kp/all")
async def get_all_kp():
    if not _cached_rows:
        await asyncio.to_thread(refresh_cache_and_file)
    elif _last_refresh is None:
        # Snapshot is available, so return it immediately and refresh live data in background.
        await trigger_refresh_if_stale()
    await trigger_refresh_if_stale()
    if not _cached_rows:
        raise HTTPException(status_code=503, detail="KP data is not available yet")

    with _status_rules_lock:
        rules_text = load_status_rules_text()
    rules = _parse_status_rules_text(rules_text)

    output = []
    for row in _cached_rows:
        formatted = format_row_for_client(row)
        formatted["statusKpComputed"] = _compute_status_for_row(formatted, rules)
        output.append(formatted)
    return output


@app.get("/api/debug/kp/{kp_number}/payment-chain")
async def debug_kp_payment_chain(kp_number: str):
    normalized_input = _normalize_kp_number(kp_number)
    if not normalized_input:
        raise HTTPException(status_code=400, detail="kp_number is required")

    if not _cached_rows:
        await asyncio.to_thread(refresh_cache_and_file)
    await trigger_refresh_if_stale()

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
async def create_kp_from_new_request(payload: NewRequestPayload):
    request_text = str(payload.requestText or "").strip()
    if len(request_text) < 3:
        raise HTTPException(status_code=400, detail="Request text is too short")

    result = await asyncio.to_thread(_create_kp_in_1c_from_request, request_text)
    await asyncio.to_thread(refresh_cache_and_file)
    return result


@app.websocket("/ws/kp")
async def ws_kp(websocket: WebSocket):
    await websocket.accept()
    previous_fp = ""

    try:
        while True:
            if not _cached_rows:
                await trigger_refresh_if_stale()
                await asyncio.sleep(2)
                continue

            current_fp = _cached_fp
            if current_fp != previous_fp:
                previous_fp = current_fp
                await websocket.send_json(
                    {
                        "type": "rows",
                        "updatedAt": _last_refresh,
                        "rows": [format_row_for_client(row) for row in _cached_rows],
                    }
                )
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        return


app.mount("/", StaticFiles(directory=".", html=True), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
