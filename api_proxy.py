#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import base64
import collections
import hashlib
import hmac
import json
import multiprocessing
import os
import re
import smtplib
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
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

_cors_origins_raw = os.getenv("CORS_ALLOWED_ORIGINS", "").strip()
if _cors_origins_raw:
    CORS_ALLOWED_ORIGINS = [x.strip() for x in _cors_origins_raw.split(",") if x.strip()]
else:
    CORS_ALLOWED_ORIGINS = [
        "https://onec-kp-realtime.onrender.com",
        "http://127.0.0.1:4173",
        "http://localhost:4173",
    ]

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOWED_ORIGINS,
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
CREATE_ODATA_USERNAME = os.getenv("CREATE_ODATA_USERNAME", "").strip() or USERNAME
CREATE_ODATA_PASSWORD = os.getenv("CREATE_ODATA_PASSWORD", "").strip() or PASSWORD
CREATE_MANAGER_NAME = os.getenv("CREATE_MANAGER_NAME", "").strip() or CREATE_ODATA_USERNAME
CREATE_MANAGER_KEY = os.getenv("CREATE_MANAGER_KEY", "").strip()
ENTITY = os.getenv("ODATA_ENTITY", "Document_КоммерческоеПредложениеКлиенту")
SEED_DATA_FILE = os.getenv(
    "SEED_DATA_FILE",
    os.getenv("DATA_FILE", "data/kp_2026_march_april.json"),
)
RUNTIME_DATA_FILE = os.getenv("RUNTIME_DATA_FILE", "data/kp_runtime_cache.json")
RUNTIME_META_FILE = os.getenv("RUNTIME_META_FILE", "data/kp_runtime_meta.json")
RUNTIME_CURRENT_FILE = os.getenv("RUNTIME_CURRENT_FILE", "data/kp_runtime_current.json")
MANUAL_REFRESH_STATE_FILE = os.getenv("MANUAL_REFRESH_STATE_FILE", "data/manual_refresh_state.json")
MANUAL_REFRESH_CHECKPOINT_FILE = os.getenv("MANUAL_REFRESH_CHECKPOINT_FILE", "data/manual_refresh_checkpoint.json")
STATUS_RULES_FILE = os.getenv("STATUS_RULES_FILE", "data/status_rules.json")
COMMENT_AUTOMATION_RULES_FILE = os.getenv("COMMENT_AUTOMATION_RULES_FILE", "data/comment_automation_rules.json")
COMMENT_AUTOMATION_STATE_FILE = os.getenv("COMMENT_AUTOMATION_STATE_FILE", "data/comment_automation_state.json")
SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip()
SMTP_FROM = os.getenv("SMTP_FROM", "").strip()
SMTP_SENDER = SMTP_FROM or SMTP_USERNAME
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").strip().lower() in {"1", "true", "yes", "on"}
SMTP_USE_SSL = os.getenv("SMTP_USE_SSL", "").strip().lower() in {"1", "true", "yes", "on"}
SMTP_TIMEOUT_SECONDS = float(os.getenv("SMTP_TIMEOUT_SECONDS", "20"))
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
STAGE25_RETRY_TIMEOUT_SECONDS = float(os.getenv("STAGE25_RETRY_TIMEOUT_SECONDS", "12.0"))
# Diagnostic-only third pass for the handful of docs that still fail after the
# retry pass — a much longer timeout to find out WHY (genuine slowness vs
# 1C document lock vs HTTP error) instead of just logging "failed".
STAGE25_PROBE_TIMEOUT_SECONDS = float(os.getenv("STAGE25_PROBE_TIMEOUT_SECONDS", "30.0"))
STAGE25_PROBE_WORKERS = int(os.getenv("STAGE25_PROBE_WORKERS", "1"))
STAGE25_PROBE_ATTEMPTS = int(os.getenv("STAGE25_PROBE_ATTEMPTS", "2"))
STAGE25_PROBE_BACKOFF_SECONDS = float(os.getenv("STAGE25_PROBE_BACKOFF_SECONDS", "1.5"))
STAGE25_PROBE_ENABLED = os.getenv("STAGE25_PROBE_ENABLED", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
STAGE25_WORKERS = int(os.getenv("STAGE25_WORKERS", "8"))
STAGE25_RETRY_WORKERS = int(os.getenv("STAGE25_RETRY_WORKERS", "8"))
STAGE25_RETRY_MAX_DOCS = int(os.getenv("STAGE25_RETRY_MAX_DOCS", "60"))
STAGE34_WORKERS = int(os.getenv("STAGE34_WORKERS", "20"))
NAV_TIMEOUT_SECONDS = float(os.getenv("NAV_TIMEOUT_SECONDS", "0.8"))
BASE_BATCH_TIMEOUT_SECONDS = float(os.getenv("BASE_BATCH_TIMEOUT_SECONDS", "120"))
MANUAL_REFRESH_TIMEOUT_SECONDS = int(os.getenv("MANUAL_REFRESH_TIMEOUT_SECONDS", "900"))
# Hard kill deadline for the OS subprocess doing the actual 1C fetch work.
# Must stay BELOW the outer asyncio.wait_for timeout (MANUAL_REFRESH_TIMEOUT_SECONDS + 180)
# so the subprocess (and the _refresh_run_lock it holds) is actually terminated
# before the API layer gives up waiting and reports "manual refresh timed out".
# Otherwise the lock stays held by an orphaned process long after the API
# already reported the cycle as finished.
REFRESH_SUBPROCESS_TIMEOUT_SECONDS = float(
    os.getenv("REFRESH_SUBPROCESS_TIMEOUT_SECONDS", str(MANUAL_REFRESH_TIMEOUT_SECONDS))
)
# <=0 means full target window (no top-N cap).
MANUAL_REFRESH_PAGE_SIZE = int(os.getenv("MANUAL_REFRESH_PAGE_SIZE", "0"))
# Explicit top-N cap requested by user on 2026-07-27 to reduce stage2.5+ per-doc
# workload and increase chance of completing a full refresh cycle before a
# Render restart/cold-start interrupts it. <=0 disables the cap (full window).
STAGE1_ROW_LIMIT = int(os.getenv("STAGE1_ROW_LIMIT", "300"))
REQUIRE_LIVE_REFRESH_AFTER_STARTUP = os.getenv("REQUIRE_LIVE_REFRESH_AFTER_STARTUP", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
STARTUP_ENRICH_ENABLED = os.getenv("STARTUP_ENRICH_ENABLED", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
MANUAL_REFRESH_INCLUDE_STAGE6 = os.getenv("MANUAL_REFRESH_INCLUDE_STAGE6", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
COLD_START_DOC_ENRICH_LIMIT = int(os.getenv("COLD_START_DOC_ENRICH_LIMIT", "40"))
GROUP_CHECK_TIMEOUT_SECONDS = float(os.getenv("GROUP_CHECK_TIMEOUT_SECONDS", "8"))
GROUP_SCAN_MAX_PAGES = int(os.getenv("GROUP_SCAN_MAX_PAGES", "80"))
GROUP_SCAN_MAX_SECONDS = float(os.getenv("GROUP_SCAN_MAX_SECONDS", "240"))
PAYMENTS_ONLY_WAIT_SECONDS = int(os.getenv("PAYMENTS_ONLY_WAIT_SECONDS", "180"))
PAYMENTS_ONLY_PAUSE_SECONDS = int(os.getenv("PAYMENTS_ONLY_PAUSE_SECONDS", "300"))
# Hard wall-clock deadline for the payments-only worker thread (orders+
# invoices+payments matching). requests' per-read timeout does not bound
# total request duration if a server trickles data slowly, so this outer
# deadline exists purely to stop the reported state from staying
# running=true forever if the underlying thread hangs. It cannot force-kill
# the OS thread itself (Python has no safe API for that) — it only frees up
# the *reported* running/lastOk/lastError state for observability and to
# unblock the caller; the true mutual-exclusion lock (_partial_refresh_lock)
# is intentionally left untouched so a genuinely still-running worker cannot
# race with a new one over shared row mutation.
PAYMENTS_ONLY_HARD_DEADLINE_SECONDS = int(os.getenv("PAYMENTS_ONLY_HARD_DEADLINE_SECONDS", "900"))
NAV_LINK_LIMIT = int(os.getenv("NAV_LINK_LIMIT", "4"))
ORDERS_HINT_SCAN_MAX_PAGES = int(os.getenv("ORDERS_HINT_SCAN_MAX_PAGES", "80"))
ORDERS_HINT_SCAN_PAGE_SIZE = int(os.getenv("ORDERS_HINT_SCAN_PAGE_SIZE", "20"))
ORDERS_HINT_HEAD_SCAN_MAX_PAGES = int(os.getenv("ORDERS_HINT_HEAD_SCAN_MAX_PAGES", "20"))
PAYMENT_MATCH_SELECT_FIELD_CANDIDATES = [
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
]
STATUS_KP_PROPERTY_KEY = os.getenv(
    "STATUS_KP_PROPERTY_KEY",
    "e1c7a0e4-4f8d-11f0-8d50-bc97e15eb091",
)
RENDER_API_KEY = os.getenv("RENDER_API_KEY", "")
RENDER_SERVICE_NAME = os.getenv("RENDER_SERVICE_NAME", "onec-kp-realtime")
LOCAL_STAGE4_AGENT_TOKEN = os.getenv("LOCAL_STAGE4_AGENT_TOKEN", "").strip()
RENDER_STATUS_TTL = int(os.getenv("RENDER_STATUS_TTL", "30"))
STATUS_RULES_TEXT_ENV = os.getenv("STATUS_RULES_TEXT", "").strip()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
GITHUB_REPO = os.getenv("GITHUB_REPO", "pavel9619229-cmyk/KP").strip()
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main").strip()
GITHUB_RUNTIME_BRANCH = os.getenv("GITHUB_RUNTIME_BRANCH", GITHUB_BRANCH).strip() or GITHUB_BRANCH
GITHUB_RUNTIME_CURRENT_PATH = os.getenv("GITHUB_RUNTIME_CURRENT_PATH", "data/kp_runtime_current.json").strip()
GITHUB_RUNTIME_VERSIONS_DIR = os.getenv("GITHUB_RUNTIME_VERSIONS_DIR", "data/runtime_versions").strip().strip("/")
RUNTIME_STRICT_GITHUB_POINTER = os.getenv("RUNTIME_STRICT_GITHUB_POINTER", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
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
# Leave TARGET_END empty for open-ended loading (recommended).
# Optional format for explicit bound: YYYY-MM-DD or full ISO datetime.
_target_end_raw = os.getenv("TARGET_END", "").strip()
TARGET_END: Optional[datetime] = None
if _target_end_raw:
    _target_end_norm = _target_end_raw + "T23:59:59" if len(_target_end_raw) == 10 else _target_end_raw
    try:
        TARGET_END = datetime.fromisoformat(_target_end_norm.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception as exc:
        raise RuntimeError(f"Invalid TARGET_END value: '{_target_end_raw}'") from exc

if TARGET_END is not None:
    # Safety rail: prevents accidental short hardcoded windows (e.g. "end of May").
    min_safe_end = datetime.now() + timedelta(days=365)
    if TARGET_END < min_safe_end:
        raise RuntimeError(
            "TARGET_END is too close and will cut off new KP records. "
            "Leave TARGET_END empty for open-ended loading."
        )

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
_refresh_run_lock_state: dict = {"owner": None, "startedAt": None, "releasedAt": None}
_partial_refresh_lock_state: dict = {"owner": None, "startedAt": None, "releasedAt": None}
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
_comment_automation_rules_lock = threading.Lock()
_runtime_write_guard_lock = threading.Lock()
_confirmed_runtime_sync_lock = threading.Lock()
_refresh_pause_lock = threading.Lock()
_refresh_coordination_lock = threading.Lock()
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
# Proposed process 1/4 (stage1_base + stage2.5 + stage2 + stage3 + stage4 +
# stage5, i.e. everything except the heavy stage6 orders/invoices/payments
# scan): a separate, independently-triggerable refresh cycle with its own
# state, so it never collides with the main manual-refresh button's state.
_stage1_4_refresh_state_lock = threading.Lock()
_stage1_4_refresh_state: dict = {
    "running": False,
    "requestedAt": None,
    "requestedBy": None,
    "requestedFrom": None,
    "startedAt": None,
    "finishedAt": None,
    "lastOk": None,
    "lastError": None,
    "confirmedVersion": None,
}
_payments_only_state_lock = threading.Lock()
_payments_only_state: dict = {
    "running": False,
    "requestedAt": None,
    "requestedBy": None,
    "startedAt": None,
    "finishedAt": None,
    "lastOk": None,
    "lastError": None,
    "waitedSeconds": None,
    "paymentReceivedCount": None,
    "invoiceCreatedCount": None,
    "confirmedVersion": None,
}
# Proposed process 4/4 (платежи, Document_ПоступлениеБезналичныхДенежныхСредств):
# a thin, separately-triggerable wrapper around the existing payments-only
# refresh (refresh_payments_only_for_cached_rows), with its own state so it
# never collides with the main "Обновить" button's or payments-only button's
# reported state. Reuses payments-only logic as-is (orders+invoices+payments
# matching + seed promotions) — does not change how "Оплата получена" is computed.
_stage4_4_refresh_state_lock = threading.Lock()
_stage4_4_refresh_state: dict = {
    "running": False,
    "requestedAt": None,
    "requestedBy": None,
    "requestedFrom": None,
    "startedAt": None,
    "finishedAt": None,
    "lastOk": None,
    "lastError": None,
    "waitedSeconds": None,
    "paymentReceivedCount": None,
    "invoiceCreatedCount": None,
    "confirmedVersion": None,
}
# Queue state for local 4/4 bridge (Render UI -> local watchdog -> GitHub publish).
_stage4_4_local_queue_lock = threading.Lock()
_stage4_4_local_queue_state: dict = {
    "running": False,
    "phase": "idle",  # idle | queued | claimed | running | success | error
    "taskId": None,
    "requestedAt": None,
    "requestedBy": None,
    "requestedFrom": None,
    "claimedAt": None,
    "claimedBy": None,
    "startedAt": None,
    "finishedAt": None,
    "lastOk": None,
    "lastError": None,
    "waitedSeconds": None,
    "paymentReceivedCount": None,
    "invoiceCreatedCount": None,
    "confirmedVersion": None,
    "resultSource": None,
}
_single_kp_seed_queue_lock = threading.Lock()
_single_kp_seed_queue: set[str] = set()
_resume_interrupted_manual_refresh = False

_startup_live_refresh_lock = threading.Lock()
_startup_live_refresh_state: dict = {
    "required": REQUIRE_LIVE_REFRESH_AFTER_STARTUP,
    "running": False,
    "completed": False,
    "ok": None,
    "startedAt": None,
    "finishedAt": None,
    "lastError": None,
}

_refresh_pause_state: dict = {
    "pausedUntilTs": 0.0,
    "pausedUntil": None,
    "reason": None,
    "requestedBy": None,
}

def _manual_refresh_snapshot() -> dict:
    with _manual_refresh_state_lock:
        state = dict(_manual_refresh_state)
    checkpoint = _load_refresh_checkpoint()
    state["checkpointStage"] = checkpoint.get("stage") if checkpoint.get("inProgress") else None
    state["checkpointUpdatedAt"] = checkpoint.get("updatedAt") if checkpoint.get("inProgress") else None
    state["rows"] = len(_cached_rows)
    state["lastRefresh"] = _last_refresh
    state["lastRefreshError"] = _last_refresh_error
    return state


def _stage1_4_refresh_snapshot() -> dict:
    with _stage1_4_refresh_state_lock:
        state = dict(_stage1_4_refresh_state)
    state["rows"] = len(_cached_rows)
    state["lastRefresh"] = _last_refresh
    state["lastRefreshError"] = _last_refresh_error
    return state


def _set_stage1_4_refresh_state(**updates: object) -> None:
    with _stage1_4_refresh_state_lock:
        _stage1_4_refresh_state.update(updates)


def _stage1_4_blocks_runtime_writer(owner: str) -> bool:
    with _stage1_4_refresh_state_lock:
        running = bool(_stage1_4_refresh_state.get("running"))
    normalized_owner = str(owner or "")
    return running and not normalized_owner.startswith(("manual-refresh-1of4:", "stage1-4-"))


def _payments_only_snapshot() -> dict:
    with _payments_only_state_lock:
        state = dict(_payments_only_state)
    state["rows"] = len(_cached_rows)
    state["lastRefresh"] = _last_refresh
    state["lastRefreshError"] = _last_refresh_error
    return state


def _set_payments_only_state(**updates: object) -> None:
    with _payments_only_state_lock:
        _payments_only_state.update(updates)


def _stage4_4_refresh_snapshot() -> dict:
    with _stage4_4_refresh_state_lock:
        state = dict(_stage4_4_refresh_state)
    state["rows"] = len(_cached_rows)
    state["lastRefresh"] = _last_refresh
    state["lastRefreshError"] = _last_refresh_error
    return state


def _set_stage4_4_refresh_state(**updates: object) -> None:
    with _stage4_4_refresh_state_lock:
        _stage4_4_refresh_state.update(updates)


def _stage4_4_local_queue_snapshot() -> dict:
    with _stage4_4_local_queue_lock:
        state = dict(_stage4_4_local_queue_state)
    state["rows"] = len(_cached_rows)
    state["lastRefresh"] = _last_refresh
    state["lastRefreshError"] = _last_refresh_error
    return state


def _set_stage4_4_local_queue_state(**updates: object) -> None:
    with _stage4_4_local_queue_lock:
        _stage4_4_local_queue_state.update(updates)


def _queue_single_kp_seed_promotion(kp_number: str) -> None:
    normalized = _normalize_kp_number(kp_number)
    if not normalized:
        return
    with _single_kp_seed_queue_lock:
        _single_kp_seed_queue.add(normalized)


def _take_single_kp_seed_queue() -> set[str]:
    with _single_kp_seed_queue_lock:
        queued = set(_single_kp_seed_queue)
        _single_kp_seed_queue.clear()
    return queued


def _startup_live_refresh_snapshot() -> dict:
    with _startup_live_refresh_lock:
        return dict(_startup_live_refresh_state)


def _set_startup_live_refresh_state(**updates: object) -> None:
    with _startup_live_refresh_lock:
        _startup_live_refresh_state.update(updates)


def _refresh_pause_snapshot() -> dict:
    with _refresh_pause_lock:
        state = dict(_refresh_pause_state)
    if float(state.get("pausedUntilTs") or 0.0) <= time.time():
        state["pausedUntilTs"] = 0.0
        state["pausedUntil"] = None
        state["reason"] = None
        state["requestedBy"] = None
    return state


def _set_refresh_pause(seconds: int, reason: str, requested_by: str) -> None:
    pause_seconds = max(0, int(seconds))
    until_ts = time.time() + pause_seconds if pause_seconds > 0 else 0.0
    until_iso = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S") if pause_seconds <= 0 else datetime.fromtimestamp(until_ts, _TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")
    with _refresh_pause_lock:
        _refresh_pause_state.update(
            {
                "pausedUntilTs": until_ts,
                "pausedUntil": until_iso if pause_seconds > 0 else None,
                "reason": reason if pause_seconds > 0 else None,
                "requestedBy": requested_by if pause_seconds > 0 else None,
            }
        )


def _clear_refresh_pause(reason: str | None = None) -> None:
    with _refresh_pause_lock:
        active_reason = _refresh_pause_state.get("reason")
        if reason and active_reason and active_reason != reason:
            return
        _refresh_pause_state.update(
            {
                "pausedUntilTs": 0.0,
                "pausedUntil": None,
                "reason": None,
                "requestedBy": None,
            }
        )


def _is_refresh_paused() -> bool:
    snapshot = _refresh_pause_snapshot()
    return float(snapshot.get("pausedUntilTs") or 0.0) > time.time()


def _lock_state_snapshot(lock_state: dict) -> dict:
    return {
        "owner": lock_state.get("owner"),
        "startedAt": lock_state.get("startedAt"),
        "releasedAt": lock_state.get("releasedAt"),
    }


def _set_lock_owner(lock_state: dict, owner: str | None) -> None:
    lock_state["owner"] = owner
    if owner:
        lock_state["startedAt"] = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")
        lock_state["releasedAt"] = None
    else:
        lock_state["releasedAt"] = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")
        lock_state["startedAt"] = None


def _clear_lock_owner(lock_state: dict) -> None:
    _set_lock_owner(lock_state, None)


def _startup_live_refresh_gate_open() -> bool:
    state = _startup_live_refresh_snapshot()
    # If cache is already populated (seed/runtime snapshot), allow serving rows.
    if _cached_rows:
        return True
    if not bool(state.get("required")):
        return True
    return bool(state.get("completed")) and bool(state.get("ok"))


def _startup_live_refresh_gate_detail() -> str:
    state = _startup_live_refresh_snapshot()
    if not bool(state.get("required")):
        return "live refresh gate is disabled"
    if bool(state.get("running")):
        return "startup live refresh is running"
    if bool(state.get("completed")) and not bool(state.get("ok")):
        return str(state.get("lastError") or "startup live refresh failed")
    return "startup live refresh has not completed yet"


def _is_runtime_snapshot_stale_for_recovery(max_age_hours: int = 6) -> bool:
    """Best-effort stale check for startup recovery refresh."""
    try:
        meta = _read_runtime_meta() or {}
        stamp = str(meta.get("last1cLoadedAt") or meta.get("generatedAt") or "").strip()
        if not stamp:
            return True

        dt: datetime | None = None
        try:
            dt = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        except Exception:
            pass
        if dt is None:
            try:
                dt = datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S")
            except Exception:
                return True

        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

        age = datetime.utcnow() - dt
        return age > timedelta(hours=max_age_hours)
    except Exception:
        return True


def _set_manual_refresh_state(**updates: object) -> None:
    with _manual_refresh_state_lock:
        _manual_refresh_state.update(updates)
        try:
            path = Path(MANUAL_REFRESH_STATE_FILE)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as f:
                json.dump(_manual_refresh_state, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            log(f"manual refresh state save failed: {exc}")


def _load_manual_refresh_state() -> dict:
    try:
        path = Path(MANUAL_REFRESH_STATE_FILE)
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


_manual_refresh_state.update(_load_manual_refresh_state())
if _manual_refresh_state.get("running"):
    _resume_interrupted_manual_refresh = True
    _set_manual_refresh_state(
        running=False,
        finishedAt=datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
        lastOk=False,
        lastError="manual refresh was interrupted by server restart",
    )


REFRESH_STAGE_ORDER = [
    "stage1_base",
    "stage2_comment_flags",
    "stage3_customer",
    "stage4_manager",
    "stage5_product_price",
    "stage6_group_flags",
]


def _load_refresh_checkpoint() -> dict:
    try:
        path = Path(MANUAL_REFRESH_CHECKPOINT_FILE)
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _has_pending_refresh_checkpoint() -> bool:
    checkpoint = _load_refresh_checkpoint()
    return bool(checkpoint.get("inProgress")) and isinstance(checkpoint.get("rows"), list)


def _save_refresh_checkpoint(stage: str, rows: list, include_stage6: bool, page_size: int) -> None:
    try:
        path = Path(MANUAL_REFRESH_CHECKPOINT_FILE)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "inProgress": True,
            "stage": stage,
            "rows": rows,
            "includeStage6": bool(include_stage6),
            "pageSize": int(page_size),
            "updatedAt": datetime.now(timezone.utc).isoformat(),
        }
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    except Exception as exc:
        log(f"refresh checkpoint save failed ({stage}): {exc}")


def _clear_refresh_checkpoint() -> None:
    try:
        path = Path(MANUAL_REFRESH_CHECKPOINT_FILE)
        if path.exists():
            path.unlink()
    except Exception as exc:
        log(f"refresh checkpoint cleanup failed: {exc}")


def _stage_completed(stage_name: str, checkpoint_stage: str | None) -> bool:
    if not checkpoint_stage or checkpoint_stage not in REFRESH_STAGE_ORDER:
        return False
    try:
        return REFRESH_STAGE_ORDER.index(stage_name) <= REFRESH_STAGE_ORDER.index(checkpoint_stage)
    except Exception:
        return False

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
статус ОТГРУЖЕНО, ОФОРМЛЕНО И ОПЛАЧЕНО устанавливается, если Оплата получена - ДА, В ЭДО отправлено - ДА
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


def _build_headers(username: str | None = None, password: str | None = None) -> dict:
    auth_username = USERNAME if username is None else username
    auth_password = PASSWORD if password is None else password
    creds = base64.b64encode(f"{auth_username}:{auth_password}".encode("utf-8")).decode("ascii")
    return {
        "Authorization": f"Basic {creds}",
        "Accept": "application/json",
    }


def _build_create_headers() -> dict:
    return _build_headers(CREATE_ODATA_USERNAME, CREATE_ODATA_PASSWORD)


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


class LocalStage44Report(BaseModel):
    taskId: str
    ok: bool
    error: Optional[str] = None
    paymentReceivedCount: Optional[int] = None
    invoiceCreatedCount: Optional[int] = None
    confirmedVersion: Optional[int] = None
    source: Optional[str] = None
    runner: Optional[str] = None


def _status_rules_path() -> Path:
    return Path(STATUS_RULES_FILE)


def _comment_automation_rules_path() -> Path:
    return Path(COMMENT_AUTOMATION_RULES_FILE)


def _comment_automation_state_path() -> Path:
    return Path(COMMENT_AUTOMATION_STATE_FILE)


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


def _manager_name_is_known(value: str | None) -> bool:
    manager = str(value or "").strip()
    if not manager:
        return False
    normalized = manager.casefold().replace("ё", "е")
    return normalized not in {"не определен", "неопределен"}


def _coerce_manager_filled(row: dict) -> bool:
    if _manager_name_is_known(_row_manager_name(row)):
        return True
    explicit = row.get("managerFilled")
    return bool(explicit) if explicit is not None else False


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


def _require_local_stage4_agent_auth(request: Request) -> str:
    token = str(request.headers.get("X-Local-Agent-Token") or "").strip()
    if not LOCAL_STAGE4_AGENT_TOKEN:
        raise HTTPException(status_code=503, detail="LOCAL_STAGE4_AGENT_TOKEN is not configured")
    if not token or not hmac.compare_digest(token, LOCAL_STAGE4_AGENT_TOKEN):
        raise HTTPException(status_code=401, detail="Invalid local agent token")
    runner_id = str(request.headers.get("X-Local-Agent-Id") or "").strip()
    return runner_id or "local-agent"


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


def load_comment_automation_rules() -> dict:
    path = _comment_automation_rules_path()
    default_payload = {"rules": [], "updatedAt": None}
    if not path.exists():
        return default_payload
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            return default_payload
        rules = payload.get("rules")
        if not isinstance(rules, list):
            rules = []
        updated_at = str(payload.get("updatedAt") or "") or None
        return {"rules": rules, "updatedAt": updated_at}
    except Exception as exc:
        log(f"comment automation rules read failed: {exc}")
        return default_payload


def _load_comment_automation_state() -> dict:
    path = _comment_automation_state_path()
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            return payload
    except Exception as exc:
        log(f"comment automation state read failed: {exc}")
    return {}


def _save_comment_automation_state(state: dict) -> None:
    path = _comment_automation_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(state or {}, f, ensure_ascii=False, indent=2)


def _render_rule_template(template: str, values: dict) -> str:
    result = str(template or "")
    for key, value in (values or {}).items():
        result = result.replace("{" + str(key) + "}", str(value or ""))
    return result.strip()


def _to_dative_case(name: str, overrides: dict | None = None) -> str:
    source_name = str(name or "").strip()
    if not source_name:
        return source_name

    mapped = overrides.get(source_name) if isinstance(overrides, dict) else None
    if mapped:
        return str(mapped).strip()

    parts = source_name.split()
    if not parts:
        return source_name

    first = parts[0]
    lowered = first.lower()
    if lowered.endswith("ия"):
        parts[0] = first[:-2] + "ии"
    elif lowered.endswith("а"):
        parts[0] = first[:-1] + "е"
    elif lowered.endswith("я"):
        parts[0] = first[:-1] + "е"
    elif lowered.endswith("й"):
        parts[0] = first[:-1] + "ю"
    elif lowered.endswith("ь"):
        parts[0] = first[:-1] + "ю"
    else:
        parts[0] = first + "у"
    return " ".join(parts)


def _looks_like_email(value: str) -> bool:
    return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", str(value or "").strip()))


def _resolve_manager_email_from_rights(manager_name: str) -> str:
    wanted = _normalize_manager_name_for_acl(manager_name)
    if not wanted:
        return ""

    rights = load_access_rights()
    exact_candidates: list[str] = []
    wildcard_candidates: list[str] = []

    for user in rights.get("users", []):
        if not isinstance(user, dict):
            continue
        if str(user.get("role") or "").strip().lower() != "manager":
            continue
        username = str(user.get("username") or "").strip()
        if not _looks_like_email(username):
            continue
        allowed = user.get("allowedManagers")
        if isinstance(allowed, list):
            allowed_norm = {_normalize_manager_name_for_acl(v) for v in allowed}
            if wanted in allowed_norm:
                exact_candidates.append(username)
        elif allowed == "*":
            wildcard_candidates.append(username)

    if len(exact_candidates) == 1:
        return exact_candidates[0]
    if len(exact_candidates) > 1:
        return ""
    if len(wildcard_candidates) == 1:
        return wildcard_candidates[0]
    return ""


def _resolve_manager_email_for_rule(manager_name: str, rule: dict) -> str:
    name = str(manager_name or "").strip()
    if _looks_like_email(name):
        return name

    mapping = rule.get("managerEmailByName")
    if isinstance(mapping, dict):
        direct = str(mapping.get(name) or "").strip()
        if _looks_like_email(direct):
            return direct
        normalized_map = {
            _normalize_manager_name_for_acl(k): str(v).strip() for k, v in mapping.items() if _looks_like_email(str(v).strip())
        }
        mapped = normalized_map.get(_normalize_manager_name_for_acl(name), "")
        if _looks_like_email(mapped):
            return mapped

        if bool(rule.get("useAccessRightsFallback", True)) is False:
            return ""

    return _resolve_manager_email_from_rights(name)


def _resolve_recipient_email_for_send_to_client(manager_name: str, rule: dict, user: dict) -> tuple[str, str]:
    recipient = _resolve_manager_email_for_rule(manager_name, rule)
    if _looks_like_email(recipient):
        return recipient, "manager"

    requester_email = str((user or {}).get("username") or "").strip()
    if _looks_like_email(requester_email):
        return requester_email, "requester"

    return "", "none"


def _patch_comment_prefix_line(ref_key: str, existing_comment: str, prefix_line: str, headers: dict) -> bool:
    ref_key = str(ref_key or "").strip()
    first_prefix = str(prefix_line or "").strip()
    if not ref_key or not first_prefix:
        return False

    existing = str(existing_comment or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = existing.split("\n") if existing else []
    current_first = first_line(existing)
    if current_first == first_prefix:
        return True

    body = existing
    if lines:
        first_clean = strip_html(lines[0]).strip().lower()
        marker = "отправлен емайл с напоминанием записать текущую ситуацию"
        if marker in first_clean:
            body = "\n".join(lines[1:]).lstrip("\n")

    new_comment = first_prefix if not body.strip() else f"{first_prefix}\n{body}"
    response = requests.patch(
        f"{BASE}/{ENTITY}(guid'{ref_key}')",
        headers={**headers, "Content-Type": "application/json; charset=utf-8"},
        json={"Комментарий": new_comment},
        timeout=20,
        verify=False,
    )
    if response.status_code in (200, 204):
        return True

    log(
        "Comment rule patch failed for "
        f"{ref_key}: HTTP {response.status_code}: {response.text[:300]}"
    )
    return False


SEND_TO_CLIENT_STATUS = "ОТПРАВИТЬ КЛИЕНТУ"


def _build_send_to_client_comment_instruction(manager_name: str) -> str:
    manager = str(manager_name or "").strip() or "Менеджер"
    return (
        f"({manager}, прошу отправить данное КП клиенту, связаться с клиентом "
        "и записать в КП результат и статус)"
    )


def _build_send_to_client_email_subject(kp_number: str) -> str:
    number = str(kp_number or "").strip()
    if number:
        return f"КП {number}: ОТПРАВИТЬ КЛИЕНТУ"
    return "КП: ОТПРАВИТЬ КЛИЕНТУ"


def _prepend_instruction_to_first_comment_line(existing_comment: str, instruction: str) -> tuple[str, bool]:
    """Prepends instruction to the left side of the first line, preserving other lines."""
    comment = str(existing_comment or "").replace("\r\n", "\n").replace("\r", "\n")
    phrase = str(instruction or "").strip()
    if not phrase:
        return comment, False

    if not comment:
        return phrase, True

    lines = comment.split("\n")
    first = lines[0].strip()
    # Idempotency guard: do not prepend the same phrase twice.
    if first.startswith(phrase):
        return comment, False

    lines[0] = f"{phrase} {first}".strip()
    return "\n".join(lines), True


def _process_send_to_client_status_for_user(user: dict) -> dict:
    if not _cached_rows:
        return {
            "ok": False,
            "detail": "KP data is not available yet",
            "processed": 0,
            "matched": 0,
            "updated": 0,
            "skipped": 0,
            "failed": 0,
            "errors": [],
        }

    with _status_rules_lock:
        rules = _parse_status_rules_text(load_status_rules_text())

    visible_rows = _filter_rows_for_user(_cached_rows, user)
    target_rows: list[dict] = []
    for row in visible_rows:
        if _compute_status_for_row(row, rules) == SEND_TO_CLIENT_STATUS:
            target_rows.append(row)

    if not target_rows:
        return {
            "ok": True,
            "detail": "Нет КП для обработки",
            "processed": 0,
            "matched": 0,
            "updated": 0,
            "skipped": 0,
            "failed": 0,
            "errors": [],
        }

    headers = _build_headers()
    send_to_client_rule: dict = {}
    with _comment_automation_rules_lock:
        payload = load_comment_automation_rules()
    rules = payload.get("rules") if isinstance(payload, dict) else []
    if isinstance(rules, list):
        for item in rules:
            if not isinstance(item, dict):
                continue
            if str(item.get("id") or "").strip() == "client_thinking_reminder_v1":
                send_to_client_rule = item
                break

    updated = 0
    skipped = 0
    failed = 0
    errors: list[dict] = []

    for row in target_rows:
        ref_key = str(row.get("refKey") or "").strip()
        kp_number = str(row.get("number") or "").strip()
        manager_name = str(row.get("managerName") or row.get("manager") or "").strip() or "Менеджер"
        if not ref_key:
            failed += 1
            errors.append({"number": kp_number, "error": "empty refKey"})
            continue

        doc = _fetch_doc_by_ref(ref_key, headers, timeout=max(DOC_TIMEOUT_SECONDS, 8.0))
        if not doc:
            failed += 1
            errors.append({"number": kp_number, "error": "document fetch failed"})
            continue

        existing_comment = str(doc.get("Комментарий") or "")
        instruction = _build_send_to_client_comment_instruction(manager_name)
        new_comment, changed = _prepend_instruction_to_first_comment_line(existing_comment, instruction)
        if not changed:
            skipped += 1
            continue

        response = requests.patch(
            f"{BASE}/{ENTITY}(guid'{ref_key}')",
            headers={**headers, "Content-Type": "application/json; charset=utf-8"},
            json={"Комментарий": new_comment},
            timeout=20,
            verify=False,
        )
        if response.status_code in (200, 204):
            updated += 1
            manager_email, recipient_source = _resolve_recipient_email_for_send_to_client(
                manager_name,
                send_to_client_rule,
                user,
            )
            if not manager_email:
                failed += 1
                errors.append(
                    {
                        "number": kp_number,
                        "error": f"manager email is not resolved ({manager_name})",
                    }
                )
                continue

            email_text = first_line(new_comment) or instruction
            sent, err = _send_email(
                manager_email,
                _build_send_to_client_email_subject(kp_number),
                email_text,
            )
            if not sent:
                failed += 1
                errors.append(
                    {
                        "number": kp_number,
                        "error": f"email send failed: {err}",
                    }
                )
            elif recipient_source == "requester":
                errors.append(
                    {
                        "number": kp_number,
                        "warning": f"manager email unresolved ({manager_name}); sent to requester {manager_email}",
                    }
                )
            continue

        failed += 1
        errors.append(
            {
                "number": kp_number,
                "error": f"patch failed HTTP {response.status_code}",
            }
        )

    try:
        if updated > 0:
            refresh_comment_first_line_only()
    except Exception as exc:
        log(f"send-to-client process: comment refresh failed: {type(exc).__name__}: {exc}")

    return {
        "ok": failed == 0,
        "detail": "completed",
        "processed": len(target_rows),
        "matched": len(target_rows),
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
        "errors": errors[:50],
    }


def _send_email(to_email: str, subject: str, body_text: str) -> tuple[bool, str]:
    recipient = str(to_email or "").strip()
    if not _looks_like_email(recipient):
        return False, "invalid recipient email"
    sender = str(SMTP_SENDER or "").strip()
    if not SMTP_HOST or not sender:
        return False, "SMTP is not configured"

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"] = str(subject or "").strip() or "Напоминание по КП"
    msg.set_content(str(body_text or "").strip())

    try:
        use_ssl = SMTP_USE_SSL or SMTP_PORT == 465
        if use_ssl:
            server_ctx = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT_SECONDS)
        else:
            server_ctx = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT_SECONDS)

        with server_ctx as server:
            if SMTP_USE_TLS and not use_ssl:
                server.starttls()
            if SMTP_USERNAME:
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)
        return True, ""
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _execute_client_thinking_reminder_rule(
    rows: list[dict],
    docs_by_ref: dict[str, dict] | None,
    headers: dict,
) -> dict:
    """Runs the client_thinking_reminder_v1 rule (comment prefix + manager email)
    for rows currently at the trigger status. Not called automatically during
    refresh cycles — must be triggered explicitly (see /api/kp/process/client-thinking-reminder).
    """
    result = {"processed": 0, "matched": 0, "sent": 0, "skipped": 0, "failed": 0, "errors": []}

    with _comment_automation_rules_lock:
        payload = load_comment_automation_rules()
    rules = payload.get("rules") if isinstance(payload, dict) else []
    if not isinstance(rules, list) or not rules:
        return result

    rule = None
    for item in rules:
        if not isinstance(item, dict):
            continue
        if str(item.get("id") or "").strip() == "client_thinking_reminder_v1":
            rule = item
            break
    if not isinstance(rule, dict) or not bool(rule.get("enabled", True)):
        return result

    trigger_status = str(rule.get("triggerStatus") or "КЛИЕНТ ДУМАЕТ").strip()
    comment_tpl = str(rule.get("commentPrefixTemplate") or "").strip()
    subject_tpl = str(rule.get("emailSubjectTemplate") or "").strip()
    body_tpl = str(rule.get("emailBodyTemplate") or "").strip()
    if not trigger_status or not comment_tpl or not body_tpl:
        log("client-thinking rule skipped: invalid templates")
        return result

    with _status_rules_lock:
        status_rules = _parse_status_rules_text(load_status_rules_text())

    state = _load_comment_automation_state()
    if not isinstance(state, dict):
        state = {}
    state_items = state.get("items")
    if not isinstance(state_items, dict):
        state_items = {}

    active_refs: set[str] = set()
    sent_count = 0
    skipped_count = 0
    failed_count = 0
    errors: list[dict] = []

    for row in list(rows or []):
        ref_key = str(row.get("refKey") or "").strip()
        kp_number = str(row.get("number") or "").strip()
        manager_name = str(row.get("managerName") or "").strip() or UNKNOWN_MANAGER_NAME
        if not ref_key or not kp_number:
            continue

        computed_status = _compute_status_for_row(row, status_rules)
        if computed_status != trigger_status:
            continue
        active_refs.add(ref_key)
        result["matched"] += 1

        existing_entry = state_items.get(ref_key)
        if isinstance(existing_entry, dict) and str(existing_entry.get("status") or "") == trigger_status:
            skipped_count += 1
            continue

        dative_name = _to_dative_case(manager_name, rule.get("dativeOverrides"))
        values = {
            "managerName": manager_name,
            "managerNameDative": dative_name,
            "kpNumber": kp_number,
        }
        comment_line = _render_rule_template(comment_tpl, values)
        subject = _render_rule_template(subject_tpl, values)
        body = _render_rule_template(body_tpl, values)

        doc = (docs_by_ref or {}).get(ref_key) or {}
        if not doc:
            doc = _fetch_doc_by_ref(ref_key, headers, timeout=max(DOC_TIMEOUT_SECONDS, 8.0)) or {}
        existing_comment = str(doc.get("Комментарий") or "")
        patched = _patch_comment_prefix_line(ref_key, existing_comment, comment_line, headers)
        if not patched:
            log(f"client-thinking rule: comment patch failed for KP {kp_number}")
            failed_count += 1
            errors.append({"number": kp_number, "error": "comment patch failed"})
            continue

        manager_email = _resolve_manager_email_for_rule(manager_name, rule)
        if not manager_email:
            log(f"client-thinking rule: manager email is not resolved for KP {kp_number} ({manager_name})")
            failed_count += 1
            errors.append({"number": kp_number, "error": f"manager email is not resolved ({manager_name})"})
            continue

        sent, err = _send_email(manager_email, subject, body)
        if not sent:
            log(f"client-thinking rule: email send failed for KP {kp_number}: {err}")
            failed_count += 1
            errors.append({"number": kp_number, "error": f"email send failed: {err}"})
            continue

        sent_count += 1
        result["processed"] += 1
        state_items[ref_key] = {
            "status": trigger_status,
            "kpNumber": kp_number,
            "managerName": manager_name,
            "managerEmail": manager_email,
            "sentAt": datetime.now(timezone.utc).isoformat(),
            "ruleId": "client_thinking_reminder_v1",
        }

    stale_refs = [rk for rk, entry in state_items.items() if isinstance(entry, dict) and str(entry.get("status") or "") == trigger_status and rk not in active_refs]
    for rk in stale_refs:
        state_items.pop(rk, None)

    state["items"] = state_items
    state["updatedAt"] = datetime.now(timezone.utc).isoformat()
    _save_comment_automation_state(state)

    result["sent"] = sent_count
    result["skipped"] = skipped_count
    result["failed"] = failed_count
    result["errors"] = errors

    if sent_count or skipped_count or failed_count:
        log(
            "client-thinking rule run: "
            f"sent={sent_count}, skipped={skipped_count}, failed={failed_count}, active={len(active_refs)}"
        )

    return result


def _process_client_thinking_reminder_for_user(user: dict) -> dict:
    """Manual trigger for the client_thinking_reminder_v1 rule (button-only,
    not part of the automatic refresh pipeline)."""
    if not _cached_rows:
        return {
            "ok": False,
            "detail": "KP data is not available yet",
            "processed": 0,
            "matched": 0,
            "sent": 0,
            "skipped": 0,
            "failed": 0,
            "errors": [],
        }

    visible_rows = _filter_rows_for_user(_cached_rows, user)
    headers = _build_headers()
    result = _execute_client_thinking_reminder_rule(visible_rows, None, headers)
    return {"ok": True, **result}


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


def _find_user_key_by_login_or_name(headers: dict, value: str) -> tuple[str, str]:
    desired = str(value or "").strip()
    if not desired:
        return "", ""

    candidate_fields = ["ИмяПользователя", "Description", "Код"]
    select_full = "Ref_Key,Description,ИмяПользователя,Код"

    for field in candidate_fields:
        payload, error = _get_json_with_retry(
            f"{BASE}/Catalog_Пользователи",
            headers,
            params={
                "$select": select_full,
                "$filter": f"{field} eq '{_escape_odata_literal(desired)}'",
                "$top": "1",
            },
            timeout=6,
            retries=2,
        )
        if error or not isinstance(payload, dict):
            continue
        rows = payload.get("value", [])
        if not rows:
            continue
        first = rows[0] if isinstance(rows[0], dict) else {}
        ref_key = str(first.get("Ref_Key") or "").strip()
        if ref_key:
            name = str(first.get("Description") or first.get("ИмяПользователя") or desired).strip()
            return ref_key, name

    wanted_norm = _normalize_human_name(desired)
    best_ref = ""
    best_name = ""

    for skip in range(0, 2000, 200):
        payload, error = _get_json_with_retry(
            f"{BASE}/Catalog_Пользователи",
            headers,
            params={"$select": select_full, "$top": "200", "$skip": str(skip)},
            timeout=8,
            retries=2,
        )
        if (error or not isinstance(payload, dict)) and skip == 0:
            payload, error = _get_json_with_retry(
                f"{BASE}/Catalog_Пользователи",
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
            ref_key = str(item.get("Ref_Key") or "").strip()
            if not ref_key:
                continue

            names = [
                str(item.get("Description") or "").strip(),
                str(item.get("ИмяПользователя") or "").strip(),
                str(item.get("Код") or "").strip(),
            ]
            for name in names:
                if not name:
                    continue
                name_norm = _normalize_human_name(name)
                if name_norm == wanted_norm:
                    return ref_key, str(item.get("Description") or name).strip()
                if wanted_norm and (wanted_norm in name_norm or name_norm in wanted_norm):
                    if not best_ref:
                        best_ref = ref_key
                        best_name = str(item.get("Description") or name).strip()

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


def _resolve_manager_key(headers: dict, manager_name: str | None = None, manager_key: str | None = None) -> str:
    explicit_key = str(manager_key or "").strip()
    if explicit_key and explicit_key != ZERO_GUID:
        return explicit_key

    preferred_name = str(manager_name or "").strip() or UNKNOWN_MANAGER_NAME
    manager_catalogs = [
        "Catalog_Пользователи",
        "Catalog_Сотрудники",
        "Catalog_СотрудникиОрганизаций",
    ]
    for entity_name in manager_catalogs:
        ref_key, _ = _find_catalog_item_key_by_description(entity_name, preferred_name, headers)
        if ref_key:
            return ref_key

    ref_key, _ = _find_user_key_by_login_or_name(headers, preferred_name)
    if ref_key:
        return ref_key

    if preferred_name != CREATE_ODATA_USERNAME:
        ref_key, _ = _find_user_key_by_login_or_name(headers, CREATE_ODATA_USERNAME)
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
    headers = _build_create_headers()
    normalized_request_text = str(request_text).replace("\x00", "").strip()
    client_key, customer_key, resolved_customer_name, requested_customer_name, recognized = _resolve_customer_for_new_request(normalized_request_text, headers)
    manager_key = _resolve_manager_key(headers, CREATE_MANAGER_NAME, CREATE_MANAGER_KEY)
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
    row["managerFilled"] = _coerce_manager_filled(row)
    for key, default_value in STORAGE_DEFAULTS.items():
        if key not in row:
            row[key] = default_value
    return row


def apply_runtime_defaults(row: dict) -> dict:
    row["clientFilled"] = is_client_filled(row.get("customerName"))
    if not str(row.get("managerName") or "").strip():
        row["managerName"] = UNKNOWN_MANAGER_NAME
    row["managerFilled"] = _coerce_manager_filled(row)
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
        cached_name = str(_manager_name_cache.get(ref_key) or "").strip()
        # Do not trust cached UNKNOWN here: it can come from transient nav timeouts.
        if cached_name and cached_name != UNKNOWN_MANAGER_NAME:
            return cached_name

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
            manager_name = str(nav_obj.get("Description") or "").strip()
            if manager_name:
                _manager_name_cache[ref_key] = manager_name
                return manager_name
            return None
    except Exception:
        pass

    return None


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

    def _parse_price_value(raw_value: object) -> float:
        if isinstance(raw_value, (int, float)):
            return float(raw_value)

        text = str(raw_value or "").strip()
        if not text:
            return 0.0

        text = text.replace("\xa0", " ").replace(" ", "")
        text = re.sub(r"[^0-9,.-]", "", text)
        if not text:
            return 0.0

        if "," in text and "." in text:
            # Keep the last decimal separator, strip the other as thousands separator.
            if text.rfind(",") > text.rfind("."):
                text = text.replace(".", "").replace(",", ".")
            else:
                text = text.replace(",", "")
        elif "," in text:
            text = text.replace(",", ".")

        try:
            return float(text)
        except (ValueError, TypeError):
            return 0.0

    def _check_price(goods_row: dict) -> bool:
        price = _parse_price_value(goods_row.get("Цена"))
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
    started_at = time.time()
    max_pages = max(1, GROUP_SCAN_MAX_PAGES)
    max_seconds = max(10.0, GROUP_SCAN_MAX_SECONDS)
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

        if len(pages) >= max_pages:
            log(f"[{entity_name}] tail scan reached page limit {len(pages)}/{max_pages}")
            return pages, False
        if (time.time() - started_at) >= max_seconds:
            elapsed = time.time() - started_at
            log(f"[{entity_name}] tail scan reached time limit {elapsed:.1f}s/{max_seconds:.1f}s")
            return pages, False

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


def _extract_payment_candidate_order_refs(item: dict) -> set[str]:
    refs: set[str] = set()

    settlement_order = str(item.get("ОбъектРасчетов_Key") or item.get("ОбъектРасчетов") or "")
    if settlement_order:
        refs.add(settlement_order)

    direct_order = str(item.get("ЗаказКлиента") or "")
    direct_order_type = str(item.get("ЗаказКлиента_Type") or "")
    if direct_order and (not direct_order_type or direct_order_type.endswith("Document_ЗаказКлиента")):
        refs.add(direct_order)

    base_order = str(item.get("ДокументОснование") or "")
    base_order_type = str(item.get("ДокументОснование_Type") or "")
    if base_order and (not base_order_type or base_order_type.endswith("Document_ЗаказКлиента")):
        refs.add(base_order)

    refs.update(_extract_order_refs_from_payment_breakdown(item))
    return {ref for ref in refs if ref}


def _resolve_order_refs_to_target_kps(
    order_refs: set[str], headers: dict, kp_ref_set: set[str]
) -> dict[str, dict]:
    resolved: dict[str, dict] = {}
    target_order_refs = {str(ref or "").strip() for ref in order_refs if str(ref or "").strip()}
    if not target_order_refs or not kp_ref_set:
        return resolved

    def _fetch_one(order_ref: str) -> tuple[str, dict]:
        try:
            payload, error = _get_json_with_retry(
                f"{BASE}/Document_ЗаказКлиента(guid'{order_ref}')",
                headers,
                params={"$select": "Ref_Key,Number,ДокументОснование,ДокументОснование_Type"},
                timeout=max(8.0, GROUP_CHECK_TIMEOUT_SECONDS),
                retries=1,
            )
            if error or not isinstance(payload, dict):
                return order_ref, {}

            base_type = str(payload.get("ДокументОснование_Type") or "")
            base_ref = str(payload.get("ДокументОснование") or "")
            if not base_ref or not base_type.endswith("Document_КоммерческоеПредложениеКлиенту") or base_ref not in kp_ref_set:
                return order_ref, {}

            order_number = str(payload.get("Number") or "")
            digits_trim = "".join(ch for ch in order_number if ch.isdigit()).lstrip("0")
            compact_number = "".join(ch for ch in order_number.lower() if ch.isalnum())
            return order_ref, {
                "kp": base_ref,
                "num": digits_trim,
                "raw": order_number,
                "compact": compact_number,
            }
        except Exception:
            return order_ref, {}

    for order_ref in sorted(target_order_refs):
        _, payload = _fetch_one(order_ref)
        if payload:
            resolved[order_ref] = payload

    return resolved


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

    # Bounded tail scan: this fallback must not consume the entire refresh budget.
    try:
        count_resp = requests.get(
            f"{BASE}/Document_ЗаказКлиента/$count",
            headers=headers,
            timeout=max(20.0, GROUP_CHECK_TIMEOUT_SECONDS),
            verify=False,
        )
        if count_resp.status_code != 200:
            return result
        total_count = int(str(count_resp.text or "0").strip() or "0")
    except Exception:
        return result

    if total_count <= 0:
        return result

    page_size = max(5, ORDERS_HINT_SCAN_PAGE_SIZE)
    max_pages = max(1, ORDERS_HINT_SCAN_MAX_PAGES)
    tail_skip = ((total_count - 1) // page_size) * page_size
    select_expr = "Ref_Key,Date,Number,ДокументОснование,ДокументОснование_Type"
    pages_scanned_tail = 0
    pages_scanned_head = 0
    matched_kp_refs: set[str] = set()

    def _process_batch(batch: list) -> None:
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
                matched_kp_refs.add(base_ref)

    # Pass 1: scan from tail (recent orders).
    while pages_scanned_tail < max_pages:
        payload, error = _get_json_with_retry(
            f"{BASE}/Document_ЗаказКлиента",
            headers,
            params={"$select": select_expr, "$top": str(page_size), "$skip": str(tail_skip)},
            timeout=max(20.0, GROUP_CHECK_TIMEOUT_SECONDS),
            retries=2,
        )
        if error or not isinstance(payload, dict):
            break

        batch = payload.get("value", [])
        if not isinstance(batch, list) or not batch:
            break

        _process_batch(batch)

        if len(matched_kp_refs) >= len(kp_ref_set):
            break

        batch_dates = [_parse_odata_datetime(item.get("Date")) for item in batch if isinstance(item, dict)]
        batch_dates = [d for d in batch_dates if d is not None]
        if batch_dates and max(batch_dates) < TARGET_START:
            break

        pages_scanned_tail += 1
        if tail_skip == 0:
            break
        tail_skip = max(0, tail_skip - page_size)

    if pages_scanned_tail >= max_pages:
        log(
            f"[orders-lazy] hint scan page limit reached "
            f"({pages_scanned_tail}/{max_pages}), matched_kps={len(matched_kp_refs)}"
        )

    # Pass 2: if unresolved remains, scan from head too (older order numbers).
    unresolved_after_tail = len(matched_kp_refs) < len(kp_ref_set)
    if unresolved_after_tail:
        head_max_pages = max(0, ORDERS_HINT_HEAD_SCAN_MAX_PAGES)
        head_skip = 0
        while pages_scanned_head < head_max_pages:
            payload, error = _get_json_with_retry(
                f"{BASE}/Document_ЗаказКлиента",
                headers,
                params={"$select": select_expr, "$top": str(page_size), "$skip": str(head_skip)},
                timeout=max(20.0, GROUP_CHECK_TIMEOUT_SECONDS),
                retries=2,
            )
            if error or not isinstance(payload, dict):
                break

            batch = payload.get("value", [])
            if not isinstance(batch, list) or not batch:
                break

            _process_batch(batch)

            if len(matched_kp_refs) >= len(kp_ref_set):
                break

            pages_scanned_head += 1
            head_skip += page_size

        if pages_scanned_head:
            log(
                f"[orders-lazy] head scan pages={pages_scanned_head}/{head_max_pages}, "
                f"matched_kps={len(matched_kp_refs)}"
            )

    return result


def _enrich_group_flags_bulk(rows: list[dict], headers: dict, skip_invoice_scan: bool = False) -> dict:
    target_refs = [str(r.get("refKey") or "") for r in rows]
    target_refs = [r for r in target_refs if r]
    if not target_refs:
        return {
            "ordersScanComplete": False,
            "paymentsScanComplete": False,
            "paymentsRows": 0,
            "matchedKpCount": 0,
        }

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

    unresolved_kp_refs = {kp_ref for kp_ref in kp_ref_set if not kp_to_orders.get(kp_ref)}
    if unresolved_kp_refs:
        # Last-resort for unresolved KPs: use the same number hints as block3
        # payment-match table (from payment purpose text), then backfill order links.
        log(
            f"[orders-lazy] unresolved KPs before hint backfill: "
            f"{len(unresolved_kp_refs)} of {len(kp_ref_set)}"
        )
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
            lazy_orders = _fetch_orders_by_number_hints(purpose_number_hints, headers, unresolved_kp_refs)
            log(f"[orders-lazy] tail-page scan found {len(lazy_orders)} order→KP matches")
            for order_ref, kp_ref in lazy_orders.items():
                order_to_kp[order_ref] = kp_ref
                kp_to_orders[kp_ref].add(order_ref)
            target_order_refs = set(order_to_kp.keys())
            if target_order_refs:
                log(f"[orders-lazy] now have {len(target_order_refs)} target orders for {len(kp_ref_set)} KPs")
            unresolved_after = {kp_ref for kp_ref in kp_ref_set if not kp_to_orders.get(kp_ref)}
            if unresolved_after:
                log(f"[orders-lazy] unresolved KPs after hint backfill: {len(unresolved_after)}")

    invoice_order_refs: set[str] = set()
    invoices_complete = False
    if skip_invoice_scan:
        # Fast mode (used by "process 4/4"): payments matching does not need
        # the invoice scan (invoiceCreated is not part of the "Оплата
        # получена" / block-3 rule), so skip it entirely to save time.
        # Existing invoiceCreated values on rows are left untouched.
        log("[invoices] scan skipped (4/4 fast mode); keeping invoiceCreated as-is")
    elif not target_order_refs:
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
    payment_order_hits: set[str] = set()
    purpose_num_set: set[str] = set()
    payment_pages, payments_complete, _ = _collect_tail_pages_with_field_fallback(
        "Document_ПоступлениеБезналичныхДенежныхСредств",
        headers,
        PAYMENT_MATCH_SELECT_FIELD_CANDIDATES,
        timeout=max(120.0, GROUP_CHECK_TIMEOUT_SECONDS),
    )
    if not payments_complete and not payment_pages:
        # One extra probe before declaring scan unavailable.
        payment_pages_probe, payments_complete_probe, _ = _collect_tail_pages_with_field_fallback(
            "Document_ПоступлениеБезналичныхДенежныхСредств",
            headers,
            PAYMENT_MATCH_SELECT_FIELD_CANDIDATES,
            timeout=max(180.0, GROUP_CHECK_TIMEOUT_SECONDS),
        )
        if payment_pages_probe:
            payment_pages = payment_pages_probe
        payments_complete = bool(payments_complete or payments_complete_probe)
    if not payments_complete and not payment_pages:
        # Payment scan completely failed — no data to confirm any payment.
        # Continue to row update loop so stale cached True values are reset to False,
        # consistent with Block 3 showing "нет совпадений" when data unavailable.
        log("[payments] scan completely unavailable; treating all KPs as unpaid (no stale cache)")

    unresolved_payment_order_refs: set[str] = set()
    for batch in payment_pages:
        for item in batch:
            candidate_order_refs = _extract_payment_candidate_order_refs(item)
            for order_ref in candidate_order_refs:
                if order_ref in target_order_refs:
                    payment_order_hits.add(order_ref)
                elif order_ref not in order_to_kp:
                    unresolved_payment_order_refs.add(order_ref)

            purpose = str(item.get("НазначениеПлатежа") or "").lower()
            if not purpose:
                continue
            # Extract purpose numbers exactly as in admin block3 payment-match table.
            # Matches: "УТ-226", "ПСУТ-226" and also "№ 226", "№226" (e.g. "СЧЕТ НА ОПЛАТУ № 226").
            for m in re.finditer(r"(?:[а-яa-z]*ут[\s\-_/]*|№\s*)0*(\d+)", purpose):
                purpose_num = (m.group(1) or "").lstrip("0")
                if purpose_num:
                    purpose_num_set.add(purpose_num)

    if unresolved_payment_order_refs:
        resolved_orders = _resolve_order_refs_to_target_kps(unresolved_payment_order_refs, headers, kp_ref_set)
        if resolved_orders:
            for order_ref, payload in resolved_orders.items():
                kp_ref = str(payload.get("kp") or "")
                if not kp_ref:
                    continue
                order_to_kp[order_ref] = kp_ref
                kp_to_orders.setdefault(kp_ref, set()).add(order_ref)
                num = str(payload.get("num") or "")
                raw = str(payload.get("raw") or "")
                compact = str(payload.get("compact") or "")
                if num:
                    order_short_numbers[order_ref] = num
                if compact:
                    order_compact_numbers[order_ref] = compact
            target_order_refs = set(order_to_kp.keys())

            for batch in payment_pages:
                for item in batch:
                    for order_ref in _extract_payment_candidate_order_refs(item):
                        if order_ref in target_order_refs:
                            payment_order_hits.add(order_ref)

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

    for order_ref in payment_order_hits:
        kp_ref = order_to_kp.get(order_ref)
        if kp_ref:
            kp_payment_map[kp_ref] = True

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

    return {
        "ordersScanComplete": bool(orders_complete),
        "paymentsScanComplete": bool(payments_complete),
        "paymentsRows": sum(len(batch) for batch in payment_pages),
        "matchedKpCount": len(block3_ui_kp_hits),
    }


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


def _build_payment_match_table(headers: dict, target_rows: list[dict] | None = None) -> dict:
    """Scan all orders and payment docs, return table rows for the admin match UI."""
    _load_order_cache()

    # Target the latest 300 KPs from current runtime cache.
    target_kp_refs: set[str] = set()
    source_rows = target_rows if isinstance(target_rows, list) and target_rows else _cached_rows
    if source_rows:
        for row in source_rows[:300]:
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
    if source_rows:
        for row in source_rows:
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
        PAYMENT_MATCH_SELECT_FIELD_CANDIDATES,
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
                "orderRefs": sorted(_extract_payment_candidate_order_refs(item)),
            })

    unresolved_payment_order_refs = {
        order_ref
        for pay in pay_rows
        for order_ref in pay.get("orderRefs", [])
        if order_ref and order_ref not in order_info
    }
    if unresolved_payment_order_refs:
        resolved_orders = _resolve_order_refs_to_target_kps(unresolved_payment_order_refs, headers, target_kp_refs)
        for order_ref, payload in resolved_orders.items():
            kp_ref = str(payload.get("kp") or "")
            if not kp_ref:
                continue
            raw = str(payload.get("raw") or payload.get("num") or "")
            short = str(payload.get("num") or "")
            order_info[order_ref] = {"kp_ref": kp_ref, "raw": raw, "short": short}
            kp_to_orders.setdefault(kp_ref, []).append(order_ref)

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
            if oref in p.get("orderRefs", []) or (order_short and order_short in p["purposeNums"])
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


def _persist_payment_match_result_to_cache(table_rows: list[dict]) -> dict:
    """Write block3 match results (already scanned, no extra 1C call here) into
    the runtime cache so /admin/dashboard's "Оплата получена" reflects exactly
    what block3 shows, instead of only relying on separate 1/4-4/4 runs."""
    global _cached_rows, _cached_fp

    if not _cached_rows:
        return {"applied": False, "reason": "empty-cache"}
    matched_kp_numbers: set[str] = set()
    for item in table_rows or []:
        if not isinstance(item, dict) or str(item.get("match") or "") != "СОВПАДЕНИЕ":
            continue
        kp_num = _normalize_kp_number(str(item.get("kpNum") or ""))
        if kp_num:
            matched_kp_numbers.add(kp_num)

    if not matched_kp_numbers:
        return {"applied": True, "promoted": 0, "matchedKpCount": 0}

    with _refresh_coordination_lock:
        if _stage1_4_blocks_runtime_writer("block3-match-view"):
            log("block3 persist skipped: stage1/4 owns exclusive runtime cycle")
            return {"applied": False, "reason": "stage1-4-exclusive"}
        if not _refresh_run_lock.acquire(blocking=False):
            owner = str(_refresh_run_lock_state.get("owner") or "unknown")
            log(f"block3 persist skipped: main refresh cycle is running (owner={owner})")
            return {"applied": False, "reason": "another-refresh-running"}
    if not _partial_refresh_lock.acquire(blocking=False):
        _refresh_run_lock.release()
        owner = str(_partial_refresh_lock_state.get("owner") or "unknown")
        log(f"block3 persist skipped: another partial refresh is running (owner={owner})")
        return {"applied": False, "reason": "already-running"}

    try:
        refreshed = [dict(r) for r in _cached_rows]
        promoted = 0
        for row in refreshed:
            kp_num = _normalize_kp_number(str(row.get("number") or ""))
            if kp_num and kp_num in matched_kp_numbers and not bool(row.get("paymentReceived")):
                row["paymentReceived"] = True
                promoted += 1

        if promoted == 0:
            return {"applied": True, "promoted": 0, "matchedKpCount": len(matched_kp_numbers)}

        saved = save_rows(refreshed, write_source="block3-match-view")
        if saved:
            _cached_rows = refreshed
            _cached_fp = rows_fingerprint(refreshed)
            log(f"block3 persist: promoted paymentReceived for {promoted} KP(s) from block3 match view")
        return {"applied": bool(saved), "promoted": promoted, "matchedKpCount": len(matched_kp_numbers)}
    finally:
        _partial_refresh_lock.release()
        _refresh_run_lock.release()


def _promote_payment_received_from_match_table(rows: list[dict], headers: dict) -> dict:
    """Promote paymentReceived=True for rows that have admin-style payment matches.

    Uses the same matching logic as /api/admin/payment-match-table so a detected
    block2 match reaches the runtime flag persisted for dashboard cards.
    """
    if not rows:
        return {
            "promoted": 0,
            "matchedKpCount": 0,
            "ordersScanComplete": False,
            "paymentsScanComplete": False,
        }

    table = _build_payment_match_table(headers, target_rows=rows)
    table_rows = table.get("rows") if isinstance(table, dict) else []
    table_rows = table_rows if isinstance(table_rows, list) else []

    matched_kp_numbers: set[str] = set()
    for item in table_rows:
        if not isinstance(item, dict):
            continue
        if str(item.get("match") or "") != "СОВПАДЕНИЕ":
            continue
        kp_num = _normalize_kp_number(str(item.get("kpNum") or ""))
        if kp_num:
            matched_kp_numbers.add(kp_num)

    promoted = 0
    for row in rows:
        kp_num = _normalize_kp_number(str(row.get("number") or ""))
        if not kp_num or kp_num not in matched_kp_numbers:
            continue
        if not bool(row.get("paymentReceived")):
            row["paymentReceived"] = True
            promoted += 1

    return {
        "promoted": promoted,
        "matchedKpCount": len(matched_kp_numbers),
        "ordersScanComplete": bool(table.get("ordersScanComplete")) if isinstance(table, dict) else False,
        "paymentsScanComplete": bool(table.get("paymentsScanComplete")) if isinstance(table, dict) else False,
    }


def _count_payment_received_promotions(before_rows: list[dict], after_rows: list[dict]) -> int:
    """Count how many KP rows were promoted to paymentReceived=True during this pass."""
    if not before_rows or not after_rows:
        return 0

    before_by_ref: dict[str, bool] = {}
    for row in before_rows:
        ref = str(row.get("refKey") or "")
        if ref:
            before_by_ref[ref] = bool(row.get("paymentReceived"))

    promoted = 0
    for row in after_rows:
        ref = str(row.get("refKey") or "")
        if not ref:
            continue
        before_value = bool(before_by_ref.get(ref, False))
        after_value = bool(row.get("paymentReceived"))
        if (not before_value) and after_value:
            promoted += 1
    return promoted


def _build_payment_coverage_audit(max_rows: int = 300) -> dict:
    """Compare runtime payment flags with block3 payment-match scan for latest KP rows."""
    if not _cached_rows:
        return {
            "ok": False,
            "detail": "KP data is not available yet",
            "rowsChecked": 0,
            "mismatchCount": 0,
            "mismatches": [],
        }

    headers = _build_headers()
    table = _build_payment_match_table(headers)
    table_rows = table.get("rows") if isinstance(table, dict) else []
    table_rows = table_rows if isinstance(table_rows, list) else []
    orders_scan_complete = bool(table.get("ordersScanComplete")) if isinstance(table, dict) else False
    payments_scan_complete = bool(table.get("paymentsScanComplete")) if isinstance(table, dict) else False

    matched_kp_numbers: set[str] = set()
    for row in table_rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("match") or "") != "СОВПАДЕНИЕ":
            continue
        kp_num = _normalize_kp_number(str(row.get("kpNum") or ""))
        if kp_num:
            matched_kp_numbers.add(kp_num)

    rows_checked = list(_cached_rows[: max(1, max_rows)])
    mismatches: list[dict] = []
    for row in rows_checked:
        kp_num = _normalize_kp_number(str(row.get("number") or ""))
        if not kp_num:
            continue
        block3_has_payment = kp_num in matched_kp_numbers
        runtime_payment = bool(row.get("paymentReceived"))
        if block3_has_payment and not runtime_payment:
            mismatches.append(
                {
                    "number": str(row.get("number") or ""),
                    "refKey": str(row.get("refKey") or ""),
                    "customerName": str(row.get("customerName") or ""),
                    "managerName": str(row.get("managerName") or ""),
                    "paymentReceived": runtime_payment,
                    "block3HasPayment": block3_has_payment,
                }
            )

    if not payments_scan_complete:
        return {
            "ok": False,
            "detail": "payment scan incomplete; audit result is not reliable",
            "rowsChecked": len(rows_checked),
            "ordersScanComplete": orders_scan_complete,
            "paymentsScanComplete": payments_scan_complete,
            "block3MatchedKpCount": len(matched_kp_numbers),
            "mismatchCount": len(mismatches),
            "mismatches": mismatches,
        }

    return {
        "ok": True,
        "detail": "completed",
        "rowsChecked": len(rows_checked),
        "ordersScanComplete": orders_scan_complete,
        "paymentsScanComplete": payments_scan_complete,
        "block3MatchedKpCount": len(matched_kp_numbers),
        "mismatchCount": len(mismatches),
        "mismatches": mismatches,
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


def _load_json_with_sha_from_github_path(file_path: str) -> tuple[object | None, str]:
    if not GITHUB_REPO or not file_path:
        return None, ""

    gh_headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if GITHUB_TOKEN:
        gh_headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_path}"
    try:
        resp = requests.get(api_url, headers=gh_headers, params={"ref": _github_runtime_ref()}, timeout=20)
        if resp.status_code == 404:
            return None, ""
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
        response_payload = resp.json()
        content_b64 = str(response_payload.get("content") or "").replace("\n", "")
        decoded_payload = json.loads(base64.b64decode(content_b64.encode("ascii")).decode("utf-8"))
        return decoded_payload, str(response_payload.get("sha") or "")
    except Exception as exc:
        log(f"github json+sha API fetch failed ({file_path}): {exc}")
        raise


def _compare_and_swap_github_json(
    file_path: str,
    payload: object,
    message: str,
    expected_sha: str,
) -> str:
    if not GITHUB_TOKEN or not GITHUB_REPO or not file_path:
        raise RuntimeError("GitHub CAS requires GITHUB_TOKEN, GITHUB_REPO and file path")

    gh_headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_path}"
    body: dict = {
        "message": message,
        "content": base64.b64encode(
            json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        ).decode("ascii"),
        "branch": _github_runtime_ref(),
    }
    if expected_sha:
        body["sha"] = expected_sha

    resp = requests.put(api_url, headers=gh_headers, json=body, timeout=30)
    if resp.status_code in (200, 201):
        return "updated"
    if resp.status_code in (409, 422):
        return "conflict"
    raise RuntimeError(f"GitHub CAS failed ({file_path}): HTTP {resp.status_code}: {resp.text[:300]}")


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

    max_attempts = 4
    for attempt in range(1, max_attempts + 1):
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
            resp = requests.put(api_url, headers=gh_headers, json=body, timeout=30)
            if resp.status_code in (200, 201):
                return True

            # On conflicts/throttling/transient errors, retry a few times.
            log(f"GitHub push attempt {attempt} failed ({file_path}): HTTP {resp.status_code}: {resp.text[:300]}")
            if resp.status_code in (403, 409, 422, 429) or resp.status_code >= 500:
                if attempt < max_attempts:
                    retry_after_raw = str(resp.headers.get("Retry-After") or "").strip()
                    try:
                        retry_after = float(retry_after_raw) if retry_after_raw else 0.0
                    except Exception:
                        retry_after = 0.0
                    sleep_seconds = max(0.5 * attempt, retry_after)
                    time.sleep(sleep_seconds)
                    continue
            return False
        except Exception as exc:
            log(f"GitHub push error ({file_path}): {exc}")
            if attempt < max_attempts:
                time.sleep(0.5 * attempt)
                continue
            return False

    return False


def _build_runtime_snapshot_paths(snapshot_id: str) -> tuple[str, str]:
    normalized_id = str(snapshot_id or "").strip()
    if not normalized_id:
        raise RuntimeError("runtime snapshot has no snapshotId")
    cache_path = f"{GITHUB_RUNTIME_VERSIONS_DIR}/kp_runtime_cache_{normalized_id}.json"
    meta_path = f"{GITHUB_RUNTIME_VERSIONS_DIR}/kp_runtime_meta_{normalized_id}.json"
    return cache_path, meta_path


def _build_runtime_current_pointer(
    rows: list,
    meta: dict,
    *,
    version: int | None = None,
    cache_path: str | None = None,
    meta_path: str | None = None,
) -> dict:
    cycle_version = version or _to_int_or_none(meta.get("cycleVersion")) or 0
    if cycle_version <= 0:
        raise RuntimeError("confirmed runtime pointer has no valid version")
    snapshot_id = str(meta.get("snapshotId") or "").strip()
    if snapshot_id:
        default_cache_path, default_meta_path = _build_runtime_snapshot_paths(snapshot_id)
    else:
        suffix = f"v{cycle_version:06d}"
        default_cache_path = f"{GITHUB_RUNTIME_VERSIONS_DIR}/kp_runtime_cache_{suffix}.json"
        default_meta_path = f"{GITHUB_RUNTIME_VERSIONS_DIR}/kp_runtime_meta_{suffix}.json"
    return {
        "version": cycle_version,
        "status": "confirmed",
        "snapshotId": snapshot_id,
        "cachePath": cache_path or default_cache_path,
        "metaPath": meta_path or default_meta_path,
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


def _runtime_version_of(meta: dict | None, pointer: dict | None) -> int:
    return (
        _to_int_or_none((pointer or {}).get("version"))
        or _to_int_or_none((meta or {}).get("cycleVersion"))
        or 0
    )


def _runtime_generated_at_from_rows(rows: list) -> Optional[datetime]:
    latest: Optional[datetime] = None
    for row in list(rows or []):
        created_at = str((row or {}).get("createdAt") or "").strip()
        if not created_at:
            continue
        try:
            parsed_local = datetime.fromisoformat(created_at.replace(" ", "T"))
            if parsed_local.tzinfo is None:
                parsed = parsed_local.replace(tzinfo=_TZ_MSK).astimezone(timezone.utc)
            else:
                parsed = parsed_local.astimezone(timezone.utc)
        except Exception:
            continue
        if latest is None or parsed > latest:
            latest = parsed
    return latest


def _runtime_normalize_meta(
    meta: dict | None,
    rows: list,
    *,
    pointer: dict | None = None,
    fallback_source: str,
) -> dict:
    normalized = dict(meta or {})
    pointer = dict(pointer or {})

    row_count = len(list(rows or []))
    pointer_confirmed = str(pointer.get("status") or "") == "confirmed"
    version = _to_int_or_none(pointer.get("version")) if pointer_confirmed else None
    if version is None and str(normalized.get("status") or "") == "confirmed":
        version = _to_int_or_none(normalized.get("cycleVersion"))

    generated = _parse_iso_datetime_utc(normalized.get("generatedAt"))
    pointer_generated = _parse_iso_datetime_utc(pointer.get("generatedAt"))
    rows_generated = _runtime_generated_at_from_rows(rows)
    if generated is None:
        candidates = [dt for dt in (pointer_generated, rows_generated) if dt is not None]
        generated = max(candidates) if candidates else None
    else:
        candidates = [dt for dt in (generated, pointer_generated, rows_generated) if dt is not None]
        generated = max(candidates) if candidates else generated
    if generated is None:
        generated = datetime.now(timezone.utc)

    write_source = str(normalized.get("writeSource") or pointer.get("writeSource") or fallback_source)
    last_1c_version = _to_int_or_none(normalized.get("last1cLoadedVersion")) or version or 0
    last_1c_at = str(normalized.get("last1cLoadedAt") or normalized.get("generatedAt") or generated.isoformat())
    refresh_started_at = str(normalized.get("refreshStartedAt") or generated.isoformat())

    normalized.update(
        {
            "generatedAt": generated.isoformat(),
            "refreshStartedAt": refresh_started_at,
            "rowCount": row_count,
            "writeSource": write_source,
            "last1cLoadedVersion": last_1c_version,
            "last1cLoadedAt": last_1c_at,
        }
    )
    if version:
        normalized["status"] = "confirmed"
        normalized["cycleVersion"] = version
        normalized["snapshotId"] = str(pointer.get("snapshotId") or normalized.get("snapshotId") or "")
    else:
        normalized["status"] = "draft"
        normalized.pop("cycleVersion", None)
    return normalized


def _runtime_pointer_matches(rows: list, meta: dict, pointer: dict) -> bool:
    if str((pointer or {}).get("status") or "") != "confirmed":
        return False
    if str(meta.get("status") or "") != "confirmed":
        return False
    version = _runtime_version_of(meta, pointer)
    if version <= 0:
        return False
    pointer_version = _to_int_or_none((pointer or {}).get("version")) or 0
    if pointer_version != version:
        return False
    expected_fp = rows_fingerprint(rows)
    pointer_fp = str((pointer or {}).get("rowsFingerprint") or "")
    if not pointer_fp or pointer_fp != expected_fp:
        return False
    if str((pointer or {}).get("generatedAt") or "") != str(meta.get("generatedAt") or ""):
        return False
    if int((pointer or {}).get("rowCount") or 0) != len(rows):
        return False
    pointer_snapshot_id = str((pointer or {}).get("snapshotId") or "")
    meta_snapshot_id = str(meta.get("snapshotId") or "")
    if pointer_snapshot_id and pointer_snapshot_id != meta_snapshot_id:
        return False
    return True


def _runtime_load_local_consistent_state() -> tuple[list, dict, dict]:
    rows = load_rows_from_path(Path(RUNTIME_DATA_FILE))
    meta = _read_runtime_meta()
    pointer = _read_runtime_current_pointer()

    if not rows:
        return [], meta if isinstance(meta, dict) else {}, pointer if isinstance(pointer, dict) else {}

    if str((pointer or {}).get("status") or "") != "confirmed":
        return rows, meta if isinstance(meta, dict) else {}, pointer if isinstance(pointer, dict) else {}

    # The plain (non-versioned) RUNTIME_DATA_FILE/RUNTIME_META_FILE on disk are
    # only ever written locally (see _write_local_confirmed_runtime) — they are
    # NOT pushed to GitHub on every publish (only the versioned copies and the
    # pointer are). On an ephemeral filesystem (e.g. Render), a fresh deploy
    # restores whatever was last committed to git for these plain files, which
    # can be far older than the pointer. If the pointer already references a
    # specific fingerprinted snapshot and the on-disk rows don't match it, the
    # on-disk cache is stale garbage — do NOT relabel it with the pointer's
    # version/timestamp (that would disguise old data as current). Treat it as
    # untrustworthy so callers fall back to the GitHub-confirmed snapshot.
    pointer_fp = str((pointer or {}).get("rowsFingerprint") or "")
    if pointer_fp and pointer_fp != rows_fingerprint(rows):
        log(
            "runtime consistency: on-disk runtime cache does not match current pointer "
            f"(pointer version={pointer.get('version')}, pointer rows={pointer.get('rowCount')}, "
            f"on-disk rows={len(rows)}); ignoring stale on-disk cache"
        )
        return [], {}, {}

    normalized_meta = _runtime_normalize_meta(
        meta,
        rows,
        pointer=pointer,
        fallback_source="consistency-recovery:local",
    )

    if normalized_meta != (meta if isinstance(meta, dict) else {}):
        _write_runtime_snapshot_files(rows, normalized_meta)
        log(
            "runtime consistency: repaired local runtime meta "
            f"(version={normalized_meta.get('cycleVersion')}, rows={len(rows)})"
        )

    normalized_pointer = pointer if isinstance(pointer, dict) else {}
    if not _runtime_pointer_matches(rows, normalized_meta, normalized_pointer):
        return [], {}, {}

    return rows, normalized_meta, normalized_pointer


def _runtime_apply_local_state(rows: list, meta: dict, pointer: dict) -> None:
    _write_local_confirmed_runtime(rows, meta, pointer)


def _runtime_pick_authoritative_state(
    local_rows: list,
    local_meta: dict,
    local_pointer: dict,
    github_rows: list,
    github_meta: dict,
    github_pointer: dict,
) -> tuple[str, list, dict, dict]:
    local_version = _runtime_version_of(local_meta, local_pointer)
    github_version = _runtime_version_of(github_meta, github_pointer)
    local_generated_at = _parse_iso_datetime_utc(local_meta.get("generatedAt"))
    github_generated_at = _parse_iso_datetime_utc(github_meta.get("generatedAt"))

    local_ready = bool(local_rows and local_version > 0 and local_generated_at is not None)
    github_ready = bool(github_rows and github_version > 0 and github_generated_at is not None)

    if RUNTIME_STRICT_GITHUB_POINTER:
        if github_ready:
            return "github", github_rows, github_meta, github_pointer
        return "none", [], {}, {}

    if local_ready and github_ready:
        if local_version > github_version:
            return "local", local_rows, local_meta, local_pointer
        if github_version > local_version:
            return "github", github_rows, github_meta, github_pointer
        if github_generated_at and local_generated_at and github_generated_at > local_generated_at:
            return "github", github_rows, github_meta, github_pointer
        return "local", local_rows, local_meta, local_pointer

    if local_ready:
        return "local", local_rows, local_meta, local_pointer
    if github_ready:
        return "github", github_rows, github_meta, github_pointer

    if local_rows:
        return "local", local_rows, local_meta, local_pointer
    if github_rows:
        return "github", github_rows, github_meta, github_pointer
    return "none", [], {}, {}


def _load_confirmed_runtime_from_github() -> tuple[list, dict, dict]:
    pointer = _load_runtime_current_pointer_from_github()
    if pointer and str(pointer.get("status") or "") == "confirmed":
        cache_path = str(pointer.get("cachePath") or "").strip()
        meta_path = str(pointer.get("metaPath") or "").strip()
        rows = _load_runtime_rows_from_github_path(cache_path) if cache_path else []
        meta = _load_runtime_meta_from_github_path(meta_path) if meta_path else {}
        expected_version = _to_int_or_none(pointer.get("version")) or 0
        meta_version = _to_int_or_none(meta.get("cycleVersion")) or 0
        expected_fp = str(pointer.get("rowsFingerprint") or "")
        if rows and meta and expected_version and meta_version == expected_version:
            if not expected_fp or rows_fingerprint(rows) == expected_fp:
                normalized_meta = _runtime_normalize_meta(
                    meta,
                    rows,
                    pointer=pointer,
                    fallback_source="consistency-recovery:github-pointer",
                )
                normalized_pointer = pointer
                if not _runtime_pointer_matches(rows, normalized_meta, normalized_pointer):
                    normalized_pointer = _build_runtime_current_pointer(rows, normalized_meta)
                return rows, normalized_meta, normalized_pointer

        if rows and meta and expected_version and str(pointer.get("snapshotId") or ""):
            if expected_fp and rows_fingerprint(rows) == expected_fp:
                normalized_meta = _runtime_normalize_meta(
                    meta,
                    rows,
                    pointer=pointer,
                    fallback_source="consistency-recovery:github-pointer",
                )
                return rows, normalized_meta, pointer

    if RUNTIME_STRICT_GITHUB_POINTER:
        return [], {}, {}

    rows = _load_runtime_rows_from_github()
    meta = _load_runtime_meta_from_github()
    if rows and meta:
        normalized_meta = _runtime_normalize_meta(
            meta,
            rows,
            pointer={},
            fallback_source="consistency-recovery:github-legacy",
        )
        normalized_pointer = _build_runtime_current_pointer(rows, normalized_meta)
        return rows, normalized_meta, normalized_pointer
    return [], {}, {}


def _publish_confirmed_runtime_snapshot_or_raise(candidate_rows: list | None = None, candidate_meta: dict | None = None) -> tuple[list, dict, dict]:
    rows = list(candidate_rows or load_rows_from_path(Path(RUNTIME_DATA_FILE)))
    if not rows:
        raise RuntimeError("runtime snapshot is empty after 1C refresh")

    meta = _runtime_normalize_meta(
        dict(candidate_meta or _read_runtime_meta()),
        rows,
        pointer={},
        fallback_source="consistency-recovery:publish",
    )
    fingerprint = rows_fingerprint(rows)
    snapshot_id = str(meta.get("snapshotId") or "").strip()
    if not snapshot_id or str(meta.get("rowsFingerprint") or "") not in {"", fingerprint}:
        snapshot_id = str(uuid.uuid4())
    meta["status"] = "draft"
    meta["snapshotId"] = snapshot_id
    meta["rowsFingerprint"] = fingerprint
    meta.pop("cycleVersion", None)
    cache_path, meta_path = _build_runtime_snapshot_paths(snapshot_id)
    message_prefix = f"Runtime snapshot {snapshot_id}"

    # Push cache and meta in parallel to save time
    _push_cache_ok: list[bool] = [False]
    _push_meta_ok: list[bool] = [False]
    _push_cache_err: list = [None]
    _push_meta_err: list = [None]

    def _do_push_cache() -> None:
        try:
            ok = _push_json_to_github_path(cache_path, rows, f"{message_prefix} cache [skip ci]")
            _push_cache_ok[0] = ok
            if not ok and _push_cache_err[0] is None:
                _push_cache_err[0] = (
                    f"push returned False for {cache_path}; "
                    f"check previous 'GitHub push attempt ... failed' logs"
                )
        except Exception as e:
            _push_cache_err[0] = e

    def _do_push_meta() -> None:
        try:
            ok = _push_json_to_github_path(meta_path, meta, f"{message_prefix} meta [skip ci]")
            _push_meta_ok[0] = ok
            if not ok and _push_meta_err[0] is None:
                _push_meta_err[0] = (
                    f"push returned False for {meta_path}; "
                    f"check previous 'GitHub push attempt ... failed' logs"
                )
        except Exception as e:
            _push_meta_err[0] = e

    # Keep publish strictly sequential so one snapshot writer does not race
    # another writer updating the same GitHub branch/files.
    _do_push_cache()
    _do_push_meta()

    if not _push_cache_ok[0]:
        raise RuntimeError(f"GitHub cache version push failed: {_push_cache_err[0]}")
    if not _push_meta_ok[0]:
        raise RuntimeError(f"GitHub meta version push failed: {_push_meta_err[0]}")

    # Skip readback of versioned cache/meta files (trust 200/201 push response).
    # Use in-memory rows and meta — they are exactly what was pushed.
    # Local fingerprint sanity check (no network call needed):
    if rows_fingerprint(rows) != str(meta.get("rowsFingerprint") or ""):
        raise RuntimeError("publish: in-memory fingerprint mismatch (should never happen)")

    log(f"publish: draft snapshot {snapshot_id} pushed ({len(rows)} rows), promoting pointer with CAS")

    confirmed_pointer: dict = {}
    for attempt in range(1, 5):
        current_payload, current_sha = _load_json_with_sha_from_github_path(GITHUB_RUNTIME_CURRENT_PATH)
        current_pointer = current_payload if isinstance(current_payload, dict) else {}
        current_version = _to_int_or_none(current_pointer.get("version")) or 0

        if (
            str(current_pointer.get("status") or "") == "confirmed"
            and str(current_pointer.get("snapshotId") or "") == snapshot_id
            and str(current_pointer.get("rowsFingerprint") or "") == fingerprint
        ):
            confirmed_pointer = current_pointer
            break

        version = current_version + 1
        pointer_meta = dict(meta)
        pointer_meta["cycleVersion"] = version
        pointer_meta["status"] = "confirmed"
        pointer = _build_runtime_current_pointer(
            rows,
            pointer_meta,
            version=version,
            cache_path=cache_path,
            meta_path=meta_path,
        )
        cas_result = _compare_and_swap_github_json(
            GITHUB_RUNTIME_CURRENT_PATH,
            pointer,
            f"Promote runtime current v{version} [skip ci]",
            current_sha,
        )
        if cas_result == "updated":
            confirmed_pointer = pointer
            break
        log(f"publish: pointer CAS conflict on attempt {attempt}; retrying with fresh GitHub pointer")

    if not confirmed_pointer:
        raise RuntimeError("GitHub current pointer CAS failed after 4 conflicts")

    readback_pointer = _load_runtime_current_pointer_from_github()
    if (
        str(readback_pointer.get("status") or "") != "confirmed"
        or str(readback_pointer.get("snapshotId") or "") != snapshot_id
        or str(readback_pointer.get("rowsFingerprint") or "") != fingerprint
        or (_to_int_or_none(readback_pointer.get("version")) or 0)
        != (_to_int_or_none(confirmed_pointer.get("version")) or 0)
    ):
        raise RuntimeError("GitHub current pointer CAS readback mismatch")

    confirmed_meta = dict(meta)
    confirmed_meta["status"] = "confirmed"
    confirmed_meta["cycleVersion"] = int(readback_pointer["version"])
    confirmed_meta["last1cLoadedVersion"] = int(readback_pointer["version"])
    _write_local_confirmed_runtime(rows, confirmed_meta, readback_pointer)
    return rows, confirmed_meta, readback_pointer


def _sync_confirmed_runtime_cache_from_github_if_needed(reason: str, force: bool = False) -> bool:
    global _cached_rows, _cached_fp, _last_refresh_error, _last_confirmed_runtime_sync_check

    if _stage1_4_blocks_runtime_writer(f"github-sync:{reason}"):
        log(f"runtime consistency sync skipped: stage1/4 owns exclusive runtime cycle (reason={reason})")
        return False

    now = time.time()
    if not force and _cached_rows and (now - _last_confirmed_runtime_sync_check) < CONFIRMED_RUNTIME_SYNC_TTL_SECONDS:
        return True

    with _confirmed_runtime_sync_lock:
        now = time.time()
        if not force and _cached_rows and (now - _last_confirmed_runtime_sync_check) < CONFIRMED_RUNTIME_SYNC_TTL_SECONDS:
            return True

        local_rows, local_meta, local_pointer = _runtime_load_local_consistent_state()
        github_rows, github_meta, github_pointer = _load_confirmed_runtime_from_github()

        source, rows, meta, pointer = _runtime_pick_authoritative_state(
            local_rows,
            local_meta,
            local_pointer,
            github_rows,
            github_meta,
            github_pointer,
        )

        if source == "none" or not rows:
            return False

        # Keep local files strictly aligned with the selected authoritative snapshot.
        if source == "github":
            _runtime_apply_local_state(rows, meta, pointer)
            log(
                "runtime consistency: selected GitHub confirmed snapshot "
                f"(reason={reason}, version={_runtime_version_of(meta, pointer)}, rows={len(rows)})"
            )
        else:
            _runtime_apply_local_state(rows, meta, pointer)
            log(
                "runtime consistency: selected local snapshot "
                f"(reason={reason}, version={_runtime_version_of(meta, pointer)}, rows={len(rows)})"
            )

        _cached_rows = list(rows)
        _cached_fp = rows_fingerprint(_cached_rows)
        _last_refresh_error = None
        _last_confirmed_runtime_sync_check = now
        return True


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


def _should_skip_runtime_save(started_at: datetime, current_generated_at: datetime | None) -> bool:
    if not current_generated_at:
        return False
    if current_generated_at > started_at + timedelta(minutes=10):
        return False
    return current_generated_at > started_at


def save_rows(
    rows: list,
    *,
    refresh_started_at: Optional[datetime] = None,
    write_source: str = "runtime-refresh",
    push_to_github: bool = True,
) -> bool:
    for row in rows:
        apply_storage_defaults(row)

    if _stage1_4_blocks_runtime_writer(write_source):
        log(f"save_rows skipped: stage1/4 owns exclusive runtime cycle (source={write_source})")
        return False

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
        now_utc = datetime.now(timezone.utc)
        if current_generated_at and current_generated_at > now_utc + timedelta(minutes=10):
            log(
                "save_rows: runtime meta generatedAt looks invalid (far future), "
                f"ignoring guard value {current_generated_at.isoformat()}"
            )
            current_generated_at = None
        if _should_skip_runtime_save(started_at, current_generated_at):
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
        
        prev_last_1c = int(prev_meta.get("last1cLoadedVersion") or 0)
        prev_last_1c_at = str(prev_meta.get("last1cLoadedAt") or prev_meta.get("generatedAt") or "")

        is_live_1c_write = not str(write_source or "").startswith("github-recovery:")
        snapshot_id = str(uuid.uuid4())
        meta_payload = {
            "status": "draft",
            "snapshotId": snapshot_id,
            "generatedAt": generated_at.isoformat(),
            "refreshStartedAt": started_at.isoformat(),
            "rowCount": len(rows),
            "rowsFingerprint": rows_fingerprint(rows),
            "writeSource": write_source,
            "last1cLoadedVersion": prev_last_1c,
            "last1cLoadedAt": generated_at.isoformat() if is_live_1c_write else prev_last_1c_at,
        }

        with runtime_path.open("w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        with runtime_meta_path.open("w", encoding="utf-8") as f:
            json.dump(meta_payload, f, ensure_ascii=False, indent=2)
        _write_runtime_current_pointer(
            {
                "status": "draft",
                "snapshotId": snapshot_id,
                "rowCount": len(rows),
                "rowsFingerprint": meta_payload["rowsFingerprint"],
                "generatedAt": meta_payload["generatedAt"],
                "writeSource": write_source,
            }
        )

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
            if not number:
                continue

            existing_row = known.get(number)
            if not existing_row:
                known[number] = source_row
                continue

            existing_manager = _row_manager_name(existing_row)
            candidate_manager = _row_manager_name(source_row)
            if _manager_name_is_known(candidate_manager) and not _manager_name_is_known(existing_manager):
                merged_row = dict(existing_row)
                merged_row["managerName"] = candidate_manager
                if source_row.get("managerFilled") is not None:
                    merged_row["managerFilled"] = source_row.get("managerFilled")
                known[number] = merged_row

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


def _in_target_window(dt: datetime) -> bool:
    if dt < TARGET_START:
        return False
    if TARGET_END is not None and dt > TARGET_END:
        return False
    return True


def _build_date_filter() -> str:
    """Build OData $filter for TARGET_START..TARGET_END (if bounded)."""
    start_str = TARGET_START.strftime("%Y-%m-%dT%H:%M:%S")
    if TARGET_END is None:
        return f"Date ge datetime'{start_str}'"
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


def _fetch_latest_kp_base_batch(headers: dict, page_size: int = 0) -> tuple[int, int, list]:
    select_expr = "Ref_Key,Number,Date,Статус,СуммаДокумента"

    total_count = get_total_count(headers)
    if total_count <= 0:
        return total_count, 0, []

    # page_size controls request chunk size only; do not use it as a top-N cap.
    chunk_size = min(50, max(1, page_size)) if page_size > 0 else 50

    skip = max(0, total_count - chunk_size)
    initial_skip = skip
    collected: list = []

    # 1C OData-specific stable strategy:
    # read from the tail in small pages and move backwards.
    while True:
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
        next_skip = max(0, skip - chunk_size)
        if next_skip == skip:
            break
        skip = next_skip

        if len(batch) < top:
            break

    return total_count, initial_skip, collected


def _refresh_subprocess_worker(include_stage6: bool, page_size: int, log_queue, result_queue) -> None:
    """Entry point for the child process. Redirects this process's own copy of
    log() to log_queue so progress is still visible to the parent, then runs
    the heavy fetch pipeline and reports the outcome back via result_queue.

    Runs in a separate OS process (not a thread) so that CPU-heavy JSON
    parsing / many parallel HTTP fetches cannot starve the main process's
    asyncio event loop of GIL time, which is what caused /healthz timeouts.
    """
    def _queued_log(message: str) -> None:
        try:
            log_queue.put_nowait(str(message))
        except Exception:
            pass

    globals()["log"] = _queued_log
    try:
        rows = fetch_rows_from_odata(include_stage6=include_stage6, page_size=page_size)
        result_queue.put(("ok", rows))
    except Exception as exc:
        result_queue.put(("error", f"{type(exc).__name__}: {exc}"))


def _fetch_rows_from_odata_subprocess(
    include_stage6: bool = True,
    page_size: int = 0,
    timeout_seconds: float | None = None,
) -> list:
    """Runs fetch_rows_from_odata() in a separate OS process instead of the
    calling thread, so the main event loop (and /healthz) stays responsive
    during heavy parallel 1C fetch work.

    If the child process does not report a result within timeout_seconds,
    it is forcibly terminated (SIGTERM, then SIGKILL if needed) so the
    _refresh_run_lock held by the caller is released promptly instead of
    staying held by an orphaned process indefinitely.
    """
    if timeout_seconds is None:
        timeout_seconds = REFRESH_SUBPROCESS_TIMEOUT_SECONDS

    try:
        ctx = multiprocessing.get_context("fork")
    except ValueError:
        ctx = multiprocessing.get_context()

    log_queue = ctx.Queue()
    result_queue = ctx.Queue()
    proc = ctx.Process(
        target=_refresh_subprocess_worker,
        args=(include_stage6, page_size, log_queue, result_queue),
        daemon=True,
    )
    proc.start()
    log(f"refresh subprocess started (pid={proc.pid})")

    stop_draining = threading.Event()

    def _drain_logs() -> None:
        while not stop_draining.is_set():
            try:
                message = log_queue.get(timeout=0.5)
            except Exception:
                continue
            log(message)

    drain_thread = threading.Thread(target=_drain_logs, daemon=True)
    drain_thread.start()

    # IMPORTANT: the result_queue must be drained CONCURRENTLY with proc.join(),
    # not after it. multiprocessing docs warn that a child process cannot fully
    # exit until everything it has put() on a queue has been flushed into the
    # underlying pipe, which requires a reader on the other end. Calling
    # proc.join() before reading result_queue can deadlock/hang for a long time
    # once the payload (300 rows) exceeds the OS pipe buffer.
    result_holder: dict = {}

    def _collect_result() -> None:
        try:
            status, payload = result_queue.get()
            result_holder["status"] = status
            result_holder["payload"] = payload
        except Exception as exc:
            result_holder["status"] = "error"
            result_holder["payload"] = f"result queue read failed: {exc}"

    result_thread = threading.Thread(target=_collect_result, daemon=True)
    result_thread.start()

    try:
        result_thread.join(timeout=timeout_seconds)
        if result_thread.is_alive():
            log(
                f"refresh subprocess exceeded {timeout_seconds:.0f}s "
                f"(pid={proc.pid}) — forcing termination"
            )
            proc.terminate()
            proc.join(timeout=5)
            if proc.is_alive():
                log(f"refresh subprocess (pid={proc.pid}) ignored terminate — sending kill")
                proc.kill()
                proc.join(timeout=5)
            stop_draining.set()
            drain_thread.join(timeout=2)
            raise TimeoutError(
                f"refresh subprocess exceeded {timeout_seconds:.0f}s and was killed (pid={proc.pid})"
            )
        proc.join(timeout=10)
    finally:
        stop_draining.set()
        drain_thread.join(timeout=2)
        while True:
            try:
                message = log_queue.get_nowait()
            except Exception:
                break
            log(message)

    if "status" not in result_holder:
        raise RuntimeError(f"refresh subprocess exited (code={proc.exitcode}) without a result")

    status = result_holder["status"]
    payload = result_holder["payload"]
    if status == "error":
        raise RuntimeError(payload)
    log(f"refresh subprocess finished (pid={proc.pid}, rows={len(payload)})")
    return payload


def fetch_rows_from_odata(include_stage6: bool = True, page_size: int = 0) -> list:
    """Staged refresh pipeline.

    Old legacy path (multi-page backward scan with large skip loop) is removed.
    """
    headers = _build_headers()
    known_rows = build_known_rows_lookup()
    rows: list[dict] = []
    total_count = 0
    skip = 0
    docs_by_ref: dict[str, dict] = {}

    checkpoint = _load_refresh_checkpoint()
    checkpoint_stage = None
    if checkpoint.get("inProgress") and isinstance(checkpoint.get("rows"), list):
        checkpoint_stage = str(checkpoint.get("stage") or "")
        rows = [dict(r) for r in checkpoint.get("rows") if isinstance(r, dict)]
        for row in rows:
            apply_storage_defaults(row)
        if STAGE1_ROW_LIMIT > 0 and len(rows) > STAGE1_ROW_LIMIT:
            rows.sort(key=lambda r: r.get("createdAt", ""), reverse=True)
            dropped = len(rows) - STAGE1_ROW_LIMIT
            rows = rows[:STAGE1_ROW_LIMIT]
            log(f"resume from checkpoint: trimmed to latest {STAGE1_ROW_LIMIT} rows (dropped {dropped})")
        log(f"resume from checkpoint: stage={checkpoint_stage}, rows={len(rows)}")

    if not _stage_completed("stage1_base", checkpoint_stage):
        base_batch: list = []
        stage1_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                total_count, skip, base_batch = _fetch_latest_kp_base_batch(headers, page_size=page_size)
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

        stage1_patch: list[dict] = []
        rows = []
        for item in base_batch:
            ref_key = str(item.get("Ref_Key") or "")
            number = str(item.get("Number") or "")
            dt_raw = item.get("Date") or ""
            status = str(item.get("Статус") or "")

            dt = _parse_odata_datetime(str(dt_raw))
            if dt is None:
                continue
            if not _in_target_window(dt):
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

        if STAGE1_ROW_LIMIT > 0 and len(rows) > STAGE1_ROW_LIMIT:
            rows.sort(key=lambda r: r.get("createdAt", ""), reverse=True)
            dropped = len(rows) - STAGE1_ROW_LIMIT
            rows = rows[:STAGE1_ROW_LIMIT]
            kept_refs = {r.get("refKey") for r in rows}
            stage1_patch = [p for p in stage1_patch if p.get("refKey") in kept_refs]
            log(f"stage1_base: trimmed to latest {STAGE1_ROW_LIMIT} rows (dropped {dropped})")

        _save_stage_patch("stage1_base", stage1_patch)
        _save_refresh_checkpoint("stage1_base", rows, include_stage6, page_size)
    else:
        log(f"stage1_base skipped by checkpoint stage={checkpoint_stage}")

    # Stage 2.5: fetch docs in parallel for per-doc stages.
    need_doc_stages = not _stage_completed("stage5_product_price", checkpoint_stage)
    if need_doc_stages:
        def _fetch_one(ref_key: str) -> tuple[str, dict]:
            if not ref_key:
                return ref_key, {}
            return ref_key, _fetch_doc_by_ref(ref_key, headers, timeout=max(DOC_TIMEOUT_SECONDS, 6.0))

        ref_keys = [str(row.get("refKey") or "") for row in rows]
        ref_key_to_number = {str(row.get("refKey") or ""): str(row.get("number") or "") for row in rows}
        doc_ok = 0
        doc_fail = 0
        failed_refs: list[str] = []
        with ThreadPoolExecutor(max_workers=max(1, STAGE25_WORKERS)) as pool:
            futures = {pool.submit(_fetch_one, rk): rk for rk in ref_keys}
            for future in as_completed(futures):
                rk, doc = future.result()
                docs_by_ref[rk] = doc
                if doc:
                    doc_ok += 1
                else:
                    doc_fail += 1
                    failed_refs.append(rk)
        log(f"stage2.5: fetched {doc_ok} ok, {doc_fail} failed/timeout out of {len(ref_keys)} docs")

        # Second pass for failed refs only: slower but much smaller batch,
        # so we can recover comments for transiently slow documents.
        if failed_refs:
            retry_targets = failed_refs[: max(0, STAGE25_RETRY_MAX_DOCS)]

            def _fetch_retry(ref_key: str) -> tuple[str, dict]:
                if not ref_key:
                    return ref_key, {}
                return ref_key, _fetch_doc_by_ref(
                    ref_key,
                    headers,
                    timeout=max(STAGE25_RETRY_TIMEOUT_SECONDS, DOC_TIMEOUT_SECONDS, 6.0),
                )

            recovered = 0
            still_failed: list[str] = []
            with ThreadPoolExecutor(max_workers=max(1, STAGE25_RETRY_WORKERS)) as retry_pool:
                retry_futures = {retry_pool.submit(_fetch_retry, rk): rk for rk in retry_targets}
                for future in as_completed(retry_futures):
                    rk, doc = future.result()
                    if doc:
                        docs_by_ref[rk] = doc
                        recovered += 1
                    else:
                        still_failed.append(rk)

            doc_ok += recovered
            doc_fail = max(0, doc_fail - recovered)
            failed_refs = still_failed + failed_refs[len(retry_targets) :]
            log(
                "stage2.5 retry: attempted "
                f"{len(retry_targets)}, recovered {recovered}, still failed {len(failed_refs)}"
            )

        if failed_refs and STAGE25_PROBE_ENABLED:
            failed_nums = [ref_key_to_number.get(rk, rk) for rk in failed_refs]
            log(f"stage2.5: failed docs (comments won't update): {', '.join(failed_nums)}")

            # Diagnostic-only third pass: a handful of docs (usually <10) keep
            # failing every cycle. Probe them once with a much longer timeout
            # to learn WHY (genuine timeout vs HTTP error vs 1C document lock),
            # and recover the data if it turns out to just be marginally slow.
            # This does not change behavior for the other ~290 docs.
            probe_targets = failed_refs[:15]

            def _probe_one(ref_key: str) -> tuple[str, dict, str]:
                attempts = max(1, STAGE25_PROBE_ATTEMPTS)
                last_reason = "unknown"
                for attempt in range(attempts):
                    try:
                        resp = requests.get(
                            f"{BASE}/{ENTITY}(guid'{ref_key}')",
                            headers=headers,
                            timeout=STAGE25_PROBE_TIMEOUT_SECONDS,
                            verify=False,
                        )
                        if resp.status_code == 200:
                            doc = resp.json()
                            return ref_key, (doc if isinstance(doc, dict) else {}), "ok"
                        last_reason = f"HTTP {resp.status_code}: {resp.text[:150]}"
                    except requests.exceptions.Timeout:
                        last_reason = f"timeout>{STAGE25_PROBE_TIMEOUT_SECONDS:.0f}s"
                    except Exception as exc:
                        last_reason = f"{type(exc).__name__}: {exc}"

                    if attempt + 1 < attempts:
                        time.sleep(max(0.0, STAGE25_PROBE_BACKOFF_SECONDS) * (attempt + 1))

                return ref_key, {}, last_reason

            probe_recovered = 0
            reasons: list[str] = []
            probe_workers = max(1, min(STAGE25_PROBE_WORKERS, len(probe_targets)))
            with ThreadPoolExecutor(max_workers=probe_workers) as probe_pool:
                probe_futures = {probe_pool.submit(_probe_one, rk): rk for rk in probe_targets}
                for future in as_completed(probe_futures):
                    rk, doc, reason = future.result()
                    number = ref_key_to_number.get(rk, rk)
                    if doc:
                        docs_by_ref[rk] = doc
                        failed_refs = [x for x in failed_refs if x != rk]
                        probe_recovered += 1
                        reasons.append(f"{number}: recovered on slow probe")
                    else:
                        reasons.append(f"{number}: {reason}")

            log("stage2.5 probe (diagnostic): " + "; ".join(reasons))
            if probe_recovered:
                doc_ok += probe_recovered
                doc_fail = max(0, doc_fail - probe_recovered)
        elif failed_refs:
            log(
                "stage2.5 probe skipped (STAGE25_PROBE_ENABLED=false); "
                f"failed docs count={len(failed_refs)}"
            )
    else:
        log(f"stage2.5 skipped by checkpoint stage={checkpoint_stage}")

    # Stage 2: quick flags from full comment payload.
    if not _stage_completed("stage2_comment_flags", checkpoint_stage):
        _t2 = time.time()
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
                "kpSent": any("КП ОТПРАВЛЕНО" in line for line in comment_top) if comment_raw else row.get("kpSent", False),
                "receiptConfirmed": any("КЛИЕНТ КП УВИДЕЛ" in line for line in comment_top) if comment_raw else row.get("receiptConfirmed", False),
                "edoSent": ("В ЭДО ОТПРАВЛЕНО" in comment_clean) if comment_raw else row.get("edoSent", False),
                "rejected": ("ОТКАЗ" in comment_clean) if comment_raw else row.get("rejected", False),
                "problem": ("ПРОБЛЕМА" in comment_clean) if comment_raw else row.get("problem", False),
                "shipmentPending": ("ОТГРУЗИТЬ" in comment_clean) if comment_raw else row.get("shipmentPending", False),
                "additionalInfoFirstLine": first_line(comment_raw) or row.get("additionalInfoFirstLine") or "",
            }
            if payment_by_comment:
                patch["paymentReceived"] = True
            row.update(patch)
            stage2_patch.append(patch)
        _save_stage_patch("stage2_comment_flags", stage2_patch)
        _save_refresh_checkpoint("stage2_comment_flags", rows, include_stage6, page_size)
        log(f"stage2: done {len(rows)} rows in {time.time()-_t2:.1f}s")
    else:
        log(f"stage2 skipped by checkpoint stage={checkpoint_stage}")

    # Stage 3: customer — parallel nav-link resolution.
    if not _stage_completed("stage3_customer", checkpoint_stage):
        _t3 = time.time()
        stage1_fast_mode = not include_stage6

        def _resolve_customer(row: dict) -> dict:
            ref_key = str(row.get("refKey") or "")
            # For 1/4 runs we already seed rows from the current snapshot.
            # Reuse existing non-empty customer to avoid 300 full nav-resolve
            # calls every launch; only unresolved/new rows hit 1C.
            if stage1_fast_mode:
                existing_customer = str(row.get("customerName") or "").strip()
                if existing_customer:
                    return {
                        "refKey": ref_key,
                        "customerName": existing_customer,
                    }

            doc = docs_by_ref.get(ref_key) or {}
            customer_name = ""
            if doc:
                customer_name = resolve_customer_name_for_ref(
                    ref_key,
                    headers,
                    doc=doc,
                    use_cache=stage1_fast_mode,
                ) or ""
            return {
                "refKey": ref_key,
                "customerName": customer_name or row.get("customerName") or "",
            }

        stage3_results: dict[str, dict] = {}
        with ThreadPoolExecutor(max_workers=max(1, STAGE34_WORKERS)) as s3_pool:
            s3_futures = {s3_pool.submit(_resolve_customer, row): row for row in rows}
            for future in as_completed(s3_futures):
                result = future.result()
                stage3_results[result["refKey"]] = result

        stage3_patch: list[dict] = []
        for row in rows:
            ref_key = str(row.get("refKey") or "")
            resolved = stage3_results.get(ref_key, {})
            if resolved.get("customerName"):
                row["customerName"] = resolved["customerName"]
            patch = {
                "refKey": ref_key,
                "customerName": row.get("customerName") or "",
                "clientFilled": is_client_filled(row.get("customerName") or ""),
            }
            row.update(patch)
            stage3_patch.append(patch)
        _save_stage_patch("stage3_customer", stage3_patch)
        _save_refresh_checkpoint("stage3_customer", rows, include_stage6, page_size)
        log(f"stage3: done {len(rows)} rows in {time.time()-_t3:.1f}s")
    else:
        log(f"stage3 skipped by checkpoint stage={checkpoint_stage}")

    # Stage 4: manager — parallel nav-link resolution.
    if not _stage_completed("stage4_manager", checkpoint_stage):
        _t4 = time.time()
        stage1_fast_mode = not include_stage6

        def _resolve_manager(row: dict) -> dict:
            ref_key = str(row.get("refKey") or "")
            # Same optimization as stage3: keep already-known manager fields in
            # 1/4 mode and resolve only missing values.
            if stage1_fast_mode:
                existing_name = str(row.get("managerName") or "").strip()
                existing_filled = row.get("managerFilled")
                if existing_name and existing_name != UNKNOWN_MANAGER_NAME and existing_filled is not None:
                    return {
                        "refKey": ref_key,
                        "managerName": existing_name,
                        "managerFilled": bool(existing_filled),
                    }

            doc = docs_by_ref.get(ref_key) or {}
            result: dict = {"refKey": ref_key}
            if doc:
                manager_filled = resolve_manager_filled_for_ref(
                    ref_key,
                    headers,
                    doc=doc,
                    use_cache=stage1_fast_mode,
                )
                if manager_filled is not None:
                    result["managerFilled"] = manager_filled
                    manager_name = resolve_manager_name_for_ref(
                        ref_key,
                        headers,
                        doc=doc,
                        use_cache=stage1_fast_mode,
                    )
                    if manager_name:
                        result["managerName"] = manager_name
            return result

        stage4_results: dict[str, dict] = {}
        with ThreadPoolExecutor(max_workers=max(1, STAGE34_WORKERS)) as s4_pool:
            s4_futures = {s4_pool.submit(_resolve_manager, row): row for row in rows}
            for future in as_completed(s4_futures):
                result = future.result()
                stage4_results[result["refKey"]] = result

        stage4_patch: list[dict] = []
        for row in rows:
            ref_key = str(row.get("refKey") or "")
            resolved = stage4_results.get(ref_key, {})
            if resolved.get("managerFilled") is not None:
                row["managerFilled"] = resolved["managerFilled"]
            if resolved.get("managerName"):
                row["managerName"] = resolved["managerName"]
            row["managerFilled"] = _coerce_manager_filled(row)
            patch = {
                "refKey": ref_key,
                "managerName": row.get("managerName") or UNKNOWN_MANAGER_NAME,
                "managerFilled": row.get("managerFilled"),
            }
            row.update(patch)
            stage4_patch.append(patch)
        _save_stage_patch("stage4_manager", stage4_patch)
        _save_refresh_checkpoint("stage4_manager", rows, include_stage6, page_size)
        log(f"stage4: done {len(rows)} rows in {time.time()-_t4:.1f}s")
    else:
        log(f"stage4 skipped by checkpoint stage={checkpoint_stage}")

    # Stage 5: goods/price.
    if not _stage_completed("stage5_product_price", checkpoint_stage):
        _t5 = time.time()
        stage5_patch: list[dict] = []
        for row in rows:
            ref_key = str(row.get("refKey") or "")
            doc = docs_by_ref.get(ref_key) or {}
            if doc:
                product_specified = resolve_product_specified_for_ref(ref_key, headers, doc=doc, use_cache=True)
                price_filled = resolve_price_filled_for_ref(ref_key, headers, doc=doc, use_cache=True)
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
        _save_refresh_checkpoint("stage5_product_price", rows, include_stage6, page_size)
        log(f"stage5: done {len(rows)} rows in {time.time()-_t5:.1f}s")
    else:
        log(f"stage5 skipped by checkpoint stage={checkpoint_stage}")

    # Stage 6: heavy group flags (orders/invoices/payments).
    if include_stage6:
        if not _stage_completed("stage6_group_flags", checkpoint_stage):
            _t6 = time.time()
            stage6_patch: list[dict] = []
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
            _save_refresh_checkpoint("stage6_group_flags", rows, include_stage6, page_size)
            log(f"stage6: done in {time.time()-_t6:.1f}s")
        else:
            log(f"stage6 skipped by checkpoint stage={checkpoint_stage}")
    else:
        _save_stage_patch("stage6_group_flags", [])
        _save_refresh_checkpoint("stage6_group_flags", rows, include_stage6, page_size)
        log("stage6_group_flags skipped (fast mode)")

    for row in rows:
        apply_runtime_defaults(row)

    rows.sort(key=lambda x: x.get("createdAt", ""), reverse=True)
    _clear_refresh_checkpoint()
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
    page_size: int = 0,
    use_known_cache: bool = True,
    push_to_github: bool = True,
    update_live_cache: bool = True,
    cycle_owner: str = "manual-refresh",
) -> bool:
    """Returns True if refresh actually ran, False if skipped (another cycle holds the lock)."""
    global _cached_rows, _cached_fp, _last_refresh, _last_refresh_error
    refresh_started_at = datetime.now(timezone.utc)

    with _refresh_coordination_lock:
        if _stage1_4_blocks_runtime_writer(cycle_owner):
            log(f"refresh skipped: stage1/4 owns exclusive runtime cycle (owner={cycle_owner})")
            return False
        if not _refresh_run_lock.acquire(blocking=False):
            owner = str(_refresh_run_lock_state.get("owner") or "unknown")
            log(f"refresh skipped: another refresh cycle is running (owner={owner})")
            return False
    if not _refresh_lock.acquire(blocking=False):
        _clear_lock_owner(_refresh_run_lock_state)
        _refresh_run_lock.release()
        owner = str(_refresh_run_lock_state.get("owner") or "unknown")
        log(f"refresh skipped: previous full cycle is still running (owner={owner})")
        return False

    _set_lock_owner(_refresh_run_lock_state, cycle_owner)
    try:
        try:
            fetched = _fetch_rows_from_odata_subprocess(include_stage6=include_stage6, page_size=page_size)
            if fetched:
                _apply_seed_payment_promotions_for_all_rows(fetched)
                write_source = "stage1-4-full-refresh" if cycle_owner.startswith("manual-refresh-1of4:") else "full-refresh"
                saved = save_rows(
                    fetched,
                    refresh_started_at=refresh_started_at,
                    write_source=write_source,
                    push_to_github=push_to_github,
                )
                if not saved:
                    latest_rows = load_fresh_runtime_rows()
                    if update_live_cache and latest_rows:
                        _cached_rows = latest_rows
                        _cached_fp = rows_fingerprint(latest_rows)
                    if latest_rows:
                        _last_refresh_error = None
                        _last_refresh = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")
                        log("full refresh skipped: newer runtime snapshot already exists; using current snapshot")
                        return True
                    _last_refresh_error = "full refresh skipped: newer runtime snapshot already exists"
                    log(_last_refresh_error)
                    return False
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
                            if _in_target_window(dt):
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
                        _apply_seed_payment_promotions_for_all_rows(partial_rows)
                        write_source = (
                            "stage1-4-partial-fallback"
                            if cycle_owner.startswith("manual-refresh-1of4:")
                            else "full-refresh-partial-fallback"
                        )
                        saved = save_rows(
                            partial_rows,
                            refresh_started_at=refresh_started_at,
                            write_source=write_source,
                            push_to_github=push_to_github,
                        )
                        if not saved:
                            latest_rows = load_fresh_runtime_rows()
                            if update_live_cache and latest_rows:
                                _cached_rows = latest_rows
                                _cached_fp = rows_fingerprint(latest_rows)
                            if latest_rows:
                                _last_refresh_error = None
                                _last_refresh = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")
                                log("partial fallback skipped: newer runtime snapshot already exists; using current snapshot")
                                return True
                            _last_refresh_error = "partial fallback skipped: newer runtime snapshot already exists"
                            log(_last_refresh_error)
                            return False
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
        _clear_lock_owner(_refresh_run_lock_state)
        _refresh_run_lock.release()
    return True


def refresh_cached_rows_only() -> dict:
    global _cached_rows, _cached_fp, _last_refresh, _last_refresh_error, _partial_refresh_cursor
    global _last_comment_refresh, _last_comment_refresh_error

    if not _cached_rows:
        return {"ok": False, "skipped": "empty-cache"}
    with _refresh_coordination_lock:
        if _stage1_4_blocks_runtime_writer("fast-partial-refresh"):
            return {"ok": False, "skipped": "stage1-4-exclusive"}
        if not _refresh_run_lock.acquire(blocking=False):
            owner = str(_refresh_run_lock_state.get("owner") or "unknown")
            log(f"fast partial refresh skipped: another refresh cycle is running (owner={owner})")
            return {"ok": False, "skipped": "another-refresh-running"}
    if not _partial_refresh_lock.acquire(blocking=False):
        owner = str(_partial_refresh_lock_state.get("owner") or "unknown")
        _refresh_run_lock.release()
        log(f"fast partial refresh skipped: already running (owner={owner})")
        return {"ok": False, "skipped": "already-running"}

    try:
        _set_lock_owner(_partial_refresh_lock_state, "fast-partial-refresh")
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
        _clear_lock_owner(_partial_refresh_lock_state)
        _partial_refresh_lock.release()
        _refresh_run_lock.release()


def refresh_comment_first_line_only(cycle_owner: str = "comment-first-line-refresh") -> dict:
    global _cached_rows, _cached_fp, _last_refresh, _last_refresh_error

    if not _cached_rows:
        return {"ok": False, "error": "empty-cache"}
    if _stage1_4_blocks_runtime_writer(cycle_owner):
        return {"ok": False, "skipped": "stage1-4-exclusive", "owner": "stage1/4"}
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
        write_source=(
            "stage1-4-comment-first-line-refresh"
            if cycle_owner.startswith("manual-refresh-1of4:")
            else "comment-first-line-refresh"
        ),
    )
    if not saved:
        latest_rows = load_fresh_runtime_rows()
        if latest_rows:
            _cached_rows = latest_rows
            _cached_fp = rows_fingerprint(latest_rows)
            _last_refresh_error = None
            _last_refresh = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")
            log("comment refresh skipped: newer runtime snapshot already exists; using current snapshot")
            return {"ok": True, "touched": 0, "rows": len(latest_rows), "skipped": "newer-runtime-snapshot"}
        _last_refresh_error = "comment refresh skipped: newer runtime snapshot already exists"
        log(_last_refresh_error)
        return {"ok": False, "error": _last_refresh_error}
    _cached_rows = refreshed
    _cached_fp = rows_fingerprint(refreshed)
    _last_refresh = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")
    _last_refresh_error = None
    log(f"comment-first-line refresh success: touched={touched}, rows={len(refreshed)}")
    return {"ok": True, "touched": touched, "rows": len(refreshed)}


def refresh_payments_only_for_cached_rows(
    cycle_owner: str = "payments-only-refresh",
    allow_when_refresh_busy: bool = False,
    skip_invoice_scan: bool = False,
) -> dict:
    global _cached_rows, _cached_fp, _last_refresh, _last_refresh_error

    acquired_run_lock = False
    forced_mode = False

    with _refresh_coordination_lock:
        if _stage1_4_blocks_runtime_writer(cycle_owner):
            log(f"payments-only refresh skipped: stage1/4 owns exclusive runtime cycle (owner={cycle_owner})")
            return {"ok": False, "skipped": "stage1-4-exclusive", "owner": "stage1/4"}
        if _refresh_run_lock.acquire(blocking=False):
            acquired_run_lock = True
        else:
            owner = str(_refresh_run_lock_state.get("owner") or "unknown")
            log(f"payments-only refresh skipped: another refresh cycle is running (owner={owner})")
            return {"ok": False, "skipped": "another-refresh-running", "owner": owner}

    if not _partial_refresh_lock.acquire(blocking=False):
        owner = str(_partial_refresh_lock_state.get("owner") or "unknown")
        if acquired_run_lock:
            _refresh_run_lock.release()
        log(f"payments-only refresh skipped: already running (owner={owner})")
        return {"ok": False, "skipped": "already-running", "owner": owner}

    try:
        if acquired_run_lock:
            _set_lock_owner(_refresh_run_lock_state, cycle_owner)
        _set_lock_owner(_partial_refresh_lock_state, "payments-only-refresh")

        source_rows = load_fresh_runtime_rows() or []
        if not source_rows:
            source_rows = list(_cached_rows) if _cached_rows else []
        if not source_rows:
            return {"ok": False, "error": "empty-cache"}

        refresh_started_at = datetime.now(timezone.utc)
        headers = _build_headers()
        refreshed: list[dict] = [dict(r) for r in source_rows]
        before_refresh_rows: list[dict] = [dict(r) for r in source_rows]

        # Run only stage6 enrichment (orders/invoices/payments matching).
        stage6_diag = _enrich_group_flags_bulk(refreshed, headers, skip_invoice_scan=skip_invoice_scan)
        if not bool(stage6_diag.get("paymentsScanComplete")):
            _last_refresh_error = (
                "payments-only refresh aborted: payment scan from 1C is unavailable "
                f"(paymentsRows={int(stage6_diag.get('paymentsRows') or 0)})"
            )
            log(_last_refresh_error)
            return {
                "ok": False,
                "error": _last_refresh_error,
                "ordersScanComplete": bool(stage6_diag.get("ordersScanComplete")),
                "paymentsScanComplete": bool(stage6_diag.get("paymentsScanComplete")),
                "paymentsRows": int(stage6_diag.get("paymentsRows") or 0),
            }

        # Preserve block3-compatible payment source from seed data across
        # payments-only runs, not only for explicitly queued KPs.
        _apply_seed_payment_promotions_for_all_rows(refreshed)

        # Important: do not rescan orders/payments again here. 4/4 already ran
        # the full stage6 matching once above, and we must keep one data source
        # per cycle to avoid mismatched "what user sees" vs "what was persisted".
        promoted_in_stage6 = _count_payment_received_promotions(before_refresh_rows, refreshed)
        if promoted_in_stage6 > 0:
            log(
                "payments-only refresh: promoted paymentReceived in stage6 "
                f"promoted={promoted_in_stage6}, matchedKpCount={int(stage6_diag.get('matchedKpCount') or 0)}, "
                f"ordersScanComplete={bool(stage6_diag.get('ordersScanComplete'))}, "
                f"paymentsScanComplete={bool(stage6_diag.get('paymentsScanComplete'))}"
            )

        # Apply any queued single-KP seed promotions requested while this
        # payments-only cycle was already running.
        queued_targets = _take_single_kp_seed_queue()
        queued_promotions = _apply_seed_payment_promotions(refreshed, queued_targets)
        if queued_targets:
            log(
                "payments-only refresh: applied queued single-kp promotions "
                f"targets={sorted(queued_targets)}, promoted={queued_promotions.get('promotedTargets', [])}"
            )

        refreshed.sort(key=lambda x: x.get("createdAt", ""), reverse=True)

        saved = save_rows(
            refreshed,
            refresh_started_at=refresh_started_at,
            write_source="payments-only-refresh",
        )
        if not saved:
            latest_rows = load_fresh_runtime_rows()
            if latest_rows:
                _cached_rows = latest_rows
                _cached_fp = rows_fingerprint(latest_rows)
            _last_refresh_error = "payments-only refresh skipped: newer runtime snapshot already exists"
            log(_last_refresh_error)
            return {"ok": False, "error": _last_refresh_error}

        _cached_rows = refreshed
        _cached_fp = rows_fingerprint(refreshed)
        _last_refresh = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")
        _last_refresh_error = None

        payment_received_count = sum(1 for row in refreshed if bool(row.get("paymentReceived")))
        invoice_created_count = sum(1 for row in refreshed if bool(row.get("invoiceCreated")))
        log(
            "payments-only refresh success: "
            f"rows={len(refreshed)}, paymentReceived={payment_received_count}, invoiceCreated={invoice_created_count}"
        )
        return {
            "ok": True,
            "rows": len(refreshed),
            "paymentReceivedCount": payment_received_count,
            "invoiceCreatedCount": invoice_created_count,
            "forcedMode": forced_mode,
            "queuedSeedTargets": sorted(queued_targets),
            "queuedSeedPromoted": queued_promotions.get("promotedTargets", []),
        }
    except Exception as exc:
        _last_refresh_error = str(exc)
        log(f"payments-only refresh failed: {type(exc).__name__}: {exc}")
        return {"ok": False, "error": str(exc)}
    finally:
        _clear_lock_owner(_partial_refresh_lock_state)
        _partial_refresh_lock.release()
        if acquired_run_lock:
            _clear_lock_owner(_refresh_run_lock_state)
            _refresh_run_lock.release()


def refresh_payments_for_single_kp_from_seed(
    kp_number: str,
    cycle_owner: str = "payments-only-kp-seed",
) -> dict:
    """Fast block3-compatible payment refresh for one KP using local seed caches only.

    This path intentionally avoids long 1C scans and applies the same idea as block3:
    paymentReceived=True when any order number linked to the KP appears in payment purpose numbers.
    Here both links come from local seed caches.
    """
    global _cached_rows, _cached_fp, _last_refresh, _last_refresh_error

    normalized_target = _normalize_kp_number(kp_number)
    if not normalized_target:
        return {"ok": False, "error": "invalid-kp-number"}

    with _payments_only_state_lock:
        payments_only_running = bool(_payments_only_state.get("running"))
    if payments_only_running:
        # Recover from stale running state: if partial lock is already free,
        # no active payments-only worker can be mutating rows now.
        probe_lock_acquired = _partial_refresh_lock.acquire(blocking=False)
        if probe_lock_acquired:
            _partial_refresh_lock.release()
            _set_payments_only_state(
                running=False,
                finishedAt=datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
                lastOk=False,
                lastError="stale-running-state-recovered",
            )
            payments_only_running = False
            log("payments-only stale running state recovered before single-kp seed refresh")

    if payments_only_running:
        _queue_single_kp_seed_promotion(normalized_target)
        return {
            "ok": True,
            "queued": True,
            "owner": "payments-only-refresh",
            "message": "queued for running payments-only refresh",
        }

    with _refresh_coordination_lock:
        if _stage1_4_blocks_runtime_writer(cycle_owner):
            return {"ok": False, "skipped": "stage1-4-exclusive", "owner": "stage1/4"}
        if not _refresh_run_lock.acquire(blocking=False):
            owner = str(_refresh_run_lock_state.get("owner") or "unknown")
            return {"ok": False, "skipped": "another-refresh-running", "owner": owner}

    if not _partial_refresh_lock.acquire(blocking=False):
        owner = str(_partial_refresh_lock_state.get("owner") or "unknown")
        _refresh_run_lock.release()
        if owner == "payments-only-refresh":
            _queue_single_kp_seed_promotion(normalized_target)
            return {
                "ok": True,
                "queued": True,
                "owner": owner,
                "message": "queued for running payments-only refresh",
            }
        return {"ok": False, "skipped": "already-running", "owner": owner}

    try:
        _set_lock_owner(_partial_refresh_lock_state, cycle_owner)

        source_rows = load_fresh_runtime_rows() or []
        if not source_rows:
            source_rows = list(_cached_rows) if _cached_rows else []
        if not source_rows:
            return {"ok": False, "error": "empty-cache"}

        refreshed: list[dict] = [dict(r) for r in source_rows]
        target_index = -1
        for i, row in enumerate(refreshed):
            if _normalize_kp_number(row.get("number") or "") == normalized_target:
                target_index = i
                break

        if target_index < 0:
            return {"ok": False, "error": "kp-not-found"}

        target_row = refreshed[target_index]
        kp_ref = str(target_row.get("refKey") or "").strip()
        if not kp_ref:
            return {"ok": False, "error": "kp-ref-missing"}

        prev_payment_received = bool(target_row.get("paymentReceived"))
        seed_result = _apply_seed_payment_promotions(refreshed, {normalized_target})
        matched_nums = list(seed_result.get("matchedOrderNumsByTarget", {}).get(normalized_target, []))
        kp_order_nums = list(seed_result.get("seedOrderNumsByTarget", {}).get(normalized_target, []))
        purpose_nums = list(seed_result.get("seedPurposeNums", []))

        apply_runtime_defaults(target_row)
        refreshed.sort(key=lambda x: x.get("createdAt", ""), reverse=True)

        refresh_started_at = datetime.now(timezone.utc)
        saved = save_rows(
            refreshed,
            refresh_started_at=refresh_started_at,
            write_source=f"payments-only-kp-seed:{normalized_target}",
        )
        if not saved:
            latest_rows = load_fresh_runtime_rows()
            if latest_rows:
                _cached_rows = latest_rows
                _cached_fp = rows_fingerprint(latest_rows)
            _last_refresh_error = "payments-only single-kp seed refresh skipped: newer runtime snapshot already exists"
            return {"ok": False, "error": _last_refresh_error}

        _cached_rows = refreshed
        _cached_fp = rows_fingerprint(refreshed)
        _last_refresh = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")
        _last_refresh_error = None

        return {
            "ok": True,
            "kpNumber": target_row.get("number"),
            "kpRef": kp_ref,
            "seedOrderNums": sorted(kp_order_nums, key=lambda x: int(x) if x.isdigit() else x),
            "seedPurposeNums": sorted(purpose_nums, key=lambda x: int(x) if x.isdigit() else x),
            "matchedOrderNums": matched_nums,
            "paymentReceivedBefore": prev_payment_received,
            "paymentReceivedAfter": bool(target_row.get("paymentReceived")),
            "promoted": (not prev_payment_received) and bool(target_row.get("paymentReceived")),
        }
    except Exception as exc:
        _last_refresh_error = str(exc)
        log(f"payments-only single-kp seed refresh failed: {type(exc).__name__}: {exc}")
        return {"ok": False, "error": str(exc)}
    finally:
        _clear_lock_owner(_partial_refresh_lock_state)
        _partial_refresh_lock.release()
        _refresh_run_lock.release()


def _apply_seed_payment_promotions(rows: list[dict], normalized_targets: set[str]) -> dict:
    targets = {str(t or "").strip() for t in set(normalized_targets or set()) if str(t or "").strip()}
    if not rows or not targets:
        return {
            "seedPurposeNums": [],
            "seedOrderNumsByTarget": {},
            "matchedOrderNumsByTarget": {},
            "promotedTargets": [],
        }

    _load_order_cache()
    _load_payment_seed()

    purpose_nums: set[str] = set()
    for seed_entry in _payment_seed:
        for num in seed_entry.get("purposeNums", []):
            n = str(num or "").strip().lstrip("0")
            if n:
                purpose_nums.add(n)

    target_rows: dict[str, dict] = {}
    target_refs: dict[str, str] = {}
    for row in rows:
        n = _normalize_kp_number(row.get("number") or "")
        if n in targets:
            target_rows[n] = row
            target_refs[n] = str(row.get("refKey") or "").strip()

    seed_order_nums_by_target: dict[str, list[str]] = {}
    matched_by_target: dict[str, list[str]] = {}
    promoted_targets: list[str] = []

    # Build kp_ref -> order numbers map once from persistent order cache.
    kp_ref_to_order_nums: dict[str, set[str]] = {}
    with _order_cache_lock:
        for _, entry in _order_to_kp_cache.items():
            kp_ref = str(entry.get("kp") or "").strip()
            if not kp_ref:
                continue
            order_num = str(entry.get("num") or "").strip().lstrip("0")
            if not order_num:
                continue
            kp_ref_to_order_nums.setdefault(kp_ref, set()).add(order_num)

    for target in targets:
        row = target_rows.get(target)
        kp_ref = target_refs.get(target, "")
        if not row or not kp_ref:
            seed_order_nums_by_target[target] = []
            matched_by_target[target] = []
            continue

        order_nums = sorted(kp_ref_to_order_nums.get(kp_ref, set()), key=lambda x: int(x) if x.isdigit() else x)
        matched_nums = sorted(set(order_nums).intersection(purpose_nums), key=lambda x: int(x) if x.isdigit() else x)

        seed_order_nums_by_target[target] = order_nums
        matched_by_target[target] = matched_nums

        prev = bool(row.get("paymentReceived"))
        if matched_nums:
            row["paymentReceived"] = True
        if (not prev) and bool(row.get("paymentReceived")):
            promoted_targets.append(target)

    return {
        "seedPurposeNums": sorted(purpose_nums, key=lambda x: int(x) if x.isdigit() else x),
        "seedOrderNumsByTarget": seed_order_nums_by_target,
        "matchedOrderNumsByTarget": matched_by_target,
        "promotedTargets": promoted_targets,
    }


def _apply_seed_payment_promotions_for_all_rows(rows: list[dict]) -> dict:
    """Apply block3-compatible seed promotions for all rows before persisting."""
    targets: set[str] = set()
    for row in rows or []:
        n = _normalize_kp_number(row.get("number") or "")
        if n:
            targets.add(n)
    return _apply_seed_payment_promotions(rows, targets)


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
    if _is_refresh_paused():
        return
    if not cache_is_stale():
        return
    task = getattr(app.state, "on_demand_refresh_task", None)
    if task and not task.done():
        return
    app.state.on_demand_refresh_task = asyncio.create_task(
        asyncio.to_thread(
            refresh_cache_and_file,
            True,
            True,
            0,
            True,
            True,
            True,
            "on-demand-refresh",
        )
    )


async def refresh_loop() -> None:
    # Wait before first refresh to allow Render health-check to pass.
    # Render health-check timeout is ~30s, so we delay well after that.
    await asyncio.sleep(35)
    while True:
        started_at = time.time()
        try:
            if _is_refresh_paused():
                await asyncio.sleep(min(REFRESH_SECONDS, 5))
                continue
            await asyncio.to_thread(
                refresh_cache_and_file,
                True,
                True,
                0,
                True,
                True,
                True,
                "background-refresh-loop",
            )
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

    # In strict mode a restarted process may only serve the snapshot selected
    # by the confirmed GitHub pointer. Local files are replicas, never authority.
    if RUNTIME_STRICT_GITHUB_POINTER:
        _cached_rows = []
    else:
        local_rows, _, _ = _runtime_load_local_consistent_state()
        _cached_rows = list(local_rows)
        if not _cached_rows:
            _cached_rows = load_seed_rows()
    _cached_fp = rows_fingerprint(_cached_rows)
    _last_refresh = None

    # All blocking network operations (GitHub sync, 1C enrich) run in a background task
    # so startup completes immediately and Render health check passes without delay.
    async def _post_startup_sync():
        global _cached_rows, _cached_fp, _resume_interrupted_manual_refresh
        try:
            await asyncio.to_thread(_sync_confirmed_runtime_cache_from_github_if_needed, "startup", True)
        except Exception as exc:
            log(f"[startup] GitHub sync failed (non-blocking): {type(exc).__name__}: {exc}")

        need_recovery_refresh = _resume_interrupted_manual_refresh
        recovery_reason = "interrupted-manual-refresh"
        if not need_recovery_refresh and _has_pending_refresh_checkpoint():
            need_recovery_refresh = True
            recovery_reason = "pending-refresh-checkpoint"

        if need_recovery_refresh:
            log(f"[startup] running recovery refresh ({recovery_reason})")
            try:
                await asyncio.to_thread(
                    refresh_cache_and_file,
                    True,
                    True,
                    MANUAL_REFRESH_PAGE_SIZE,
                    True,
                    False,
                    True,
                    "startup-recovery",
                )
                if _last_refresh_error:
                    log(f"[startup] recovery refresh finished with error: {_last_refresh_error}")
                else:
                    log("[startup] recovery refresh completed")
            except Exception as exc:
                log(f"[startup] recovery refresh failed: {type(exc).__name__}: {exc}")
            finally:
                _resume_interrupted_manual_refresh = False

        if STARTUP_ENRICH_ENABLED:
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
        else:
            log("[startup] group flags enrichment skipped (STARTUP_ENRICH_ENABLED=false)")

        if REQUIRE_LIVE_REFRESH_AFTER_STARTUP:
            _set_startup_live_refresh_state(
                running=True,
                completed=False,
                ok=None,
                startedAt=datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
                finishedAt=None,
                lastError=None,
            )
            try:
                ran = await asyncio.to_thread(
                    refresh_cache_and_file,
                    False,  # no partial fallback: require live 1C refresh
                    True,
                    MANUAL_REFRESH_PAGE_SIZE,
                    True,
                    False,
                    True,
                )
                if ran and not _last_refresh_error:
                    _set_startup_live_refresh_state(
                        running=False,
                        completed=True,
                        ok=True,
                        finishedAt=datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
                        lastError=None,
                    )
                    log("[startup] mandatory live refresh completed successfully")
                else:
                    _set_startup_live_refresh_state(
                        running=False,
                        completed=True,
                        ok=False,
                        finishedAt=datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
                        lastError=str(_last_refresh_error or "startup live refresh did not produce fresh data"),
                    )
                    log("[startup] mandatory live refresh did not complete successfully")
            except Exception as exc:
                _set_startup_live_refresh_state(
                    running=False,
                    completed=True,
                    ok=False,
                    finishedAt=datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
                    lastError=str(exc),
                )
                log(f"[startup] mandatory live refresh failed: {type(exc).__name__}: {exc}")

    app.state.post_startup_task = asyncio.create_task(_post_startup_sync())

    # Do NOT await refresh here: blocking startup prevents health-check from reaching the app.
    # Background refresh loops are optional and disabled by default.
    if ENABLE_BACKGROUND_REFRESH:
        app.state.refresh_task = asyncio.create_task(refresh_loop())
        app.state.fast_partial_refresh_task = None
        log("background refresh loop enabled (full refresh only)")
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


def _require_admin_dashboard_access(request: Request) -> dict:
    admin_username = _get_admin_username(request)
    if admin_username:
        return {"username": admin_username, "role": "admin", "allowedManagers": "*"}
    user = _get_user_from_request(request)
    role = str(user.get("role") or "manager").lower()
    if role != "admin":
        raise HTTPException(status_code=403, detail="Admin role required")
    return user


@app.get("/admin/dashboard/block1")
async def admin_dashboard_block1(request: Request):
    try:
        _require_admin_dashboard_access(request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    return FileResponse(
        "admin_block1_kp_orders.html",
        media_type="text/html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/admin/dashboard/block2")
async def admin_dashboard_block2(request: Request):
    try:
        _require_admin_dashboard_access(request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    return FileResponse(
        "admin_block2_payments_invoices.html",
        media_type="text/html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/admin/dashboard/block3")
async def admin_dashboard_block3(request: Request):
    try:
        _require_admin_dashboard_access(request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    return FileResponse(
        "admin_block3_match.html",
        media_type="text/html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/admin/rights")
async def admin_rights():
    return FileResponse("admin_rights.html", media_type="text/html")


@app.api_route("/healthz", methods=["GET", "HEAD"])
async def healthz():
    return {
        "ok": True,
        "rows": len(_cached_rows),
        "backgroundRefreshEnabled": ENABLE_BACKGROUND_REFRESH,
        "startupLiveRefresh": _startup_live_refresh_snapshot(),
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

    with _refresh_coordination_lock, _manual_refresh_state_lock:
        if _stage1_4_blocks_runtime_writer(f"manual-refresh:{username}"):
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "error": "stage1/4 owns exclusive runtime cycle",
                    "blockers": ["stage1/4"],
                },
            )
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
                "lastOk": None,
                "lastError": None,
                "confirmedVersion": None,
            }
        )
        try:
            path = Path(MANUAL_REFRESH_STATE_FILE)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as f:
                json.dump(_manual_refresh_state, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            log(f"manual refresh state save failed: {exc}")

    async def _run_manual_refresh() -> None:
        global _cached_rows, _cached_fp, _last_refresh, _last_refresh_error, _last_confirmed_runtime_sync_check

        _set_manual_refresh_state(startedAt=datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"))
        log(f"manual refresh requested by {username} from {client_host}")

        # refresh_cache_and_file may legitimately run longer than
        # MANUAL_REFRESH_TIMEOUT_SECONDS when stage2.5 enters the slow probe pass
        # for chronically failing docs. Keep an additional margin so we don't
        # cut the cycle a few minutes before it naturally completes.
        refresh_wait_timeout = max(60, MANUAL_REFRESH_TIMEOUT_SECONDS) + 180
        # Hard deadline still protects from truly stuck cycles, but should remain
        # above the wait timeout to leave room for GitHub publish/readback.
        TOTAL_HARD_DEADLINE = refresh_wait_timeout + 180
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
                asyncio.to_thread(
                    refresh_cache_and_file,
                    True,
                    MANUAL_REFRESH_INCLUDE_STAGE6,
                    MANUAL_REFRESH_PAGE_SIZE,
                    True,
                    False,
                    False,
                    f"manual-refresh:{username}",
                ),
                timeout=refresh_wait_timeout,
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

            if _last_refresh_error:
                # refresh_cache_and_file caught its own exception internally
                # (e.g. subprocess timeout/kill) and returns True regardless
                # (True only means "was not skipped due to a busy lock").
                # Do NOT publish whatever happens to be on disk in that case —
                # it may be a stale/unrelated snapshot never touched by this
                # cycle's save_rows(). Treat this the same as a hard failure.
                raise RuntimeError(f"full refresh cycle did not produce a fresh snapshot: {_last_refresh_error}")

            candidate_rows = load_rows_from_path(Path(RUNTIME_DATA_FILE))
            candidate_meta = _read_runtime_meta()
            _publish_t0 = time.time()
            log(f"[refresh] starting github publish ({len(candidate_rows)} rows)")
            publish_source = "github-current"
            try:
                github_rows, _, github_pointer = await asyncio.wait_for(
                    asyncio.to_thread(
                        _publish_confirmed_runtime_snapshot_or_raise,
                        candidate_rows,
                        candidate_meta,
                    ),
                    timeout=120,
                )
                log(f"[refresh] github publish done in {time.time()-_publish_t0:.1f}s")
            except Exception as publish_exc:
                if RUNTIME_STRICT_GITHUB_POINTER:
                    log(
                        "[refresh] versioned github publish failed in strict mode: "
                        f"{type(publish_exc).__name__}: {publish_exc}"
                    )
                    raise RuntimeError(
                        f"strict runtime publish failed: {type(publish_exc).__name__}: {publish_exc}"
                    ) from publish_exc

                log(
                    f"[refresh] versioned github publish failed: {type(publish_exc).__name__}: {publish_exc}; "
                    "using local runtime snapshot instead"
                )
                github_rows = list(candidate_rows)
                github_meta = _runtime_normalize_meta(
                    dict(candidate_meta or {}),
                    github_rows,
                    pointer=previous_pointer,
                    fallback_source="consistency-recovery:manual-refresh-fallback",
                )
                try:
                    github_pointer = _build_runtime_current_pointer(github_rows, github_meta)
                except Exception:
                    github_pointer = {}
                if github_pointer:
                    _write_local_confirmed_runtime(github_rows, github_meta, github_pointer)
                else:
                    _write_runtime_snapshot_files(github_rows, github_meta)
                publish_source = "local-runtime"
                log(f"[refresh] local runtime fallback done in {time.time()-_publish_t0:.1f}s")

            _cached_rows = list(github_rows)
            _cached_fp = rows_fingerprint(_cached_rows)
            _last_confirmed_runtime_sync_check = time.time()
            _last_refresh_error = None
            _last_refresh = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")

            confirmed_version = github_pointer.get("version") if github_pointer else None
            _set_manual_refresh_state(confirmedVersion=confirmed_version, lastOk=True, lastError=None)
            log(
                "manual refresh finished: "
                f"rows={len(_cached_rows)}, lastRefresh={_last_refresh}, confirmedVersion={confirmed_version}, source={publish_source}, user={username}, host={client_host}"
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


@app.post("/api/kp/refresh/force")
async def manual_refresh_force(request: Request):
    # Explicit endpoint for forced/manual refresh from external automations.
    return await manual_refresh(request)


@app.post("/api/kp/refresh/stage1_4")
async def manual_refresh_stage1_4(request: Request):
    """Проект 1/4 (не связан с кнопкой "Обновить"): базовый скан КП +
    карточка документа (товары/услуги, комментарий, клиент, менеджер, цена
    товара) — stage1_base+stage2.5+stage2+stage3+stage4+stage5, БЕЗ stage6
    (заказы/накладные/оплаты). Публикует результат в GitHub/UI сразу после
    завершения, независимо от полного цикла refresh."""
    username = "anonymous"
    try:
        user = _get_user_from_request(request)
        username = str(user.get("username") or "anonymous")
    except HTTPException:
        pass

    client_host = request.client.host if request.client else "unknown"

    with _refresh_coordination_lock:
        with _stage1_4_refresh_state_lock:
            if _stage1_4_refresh_state.get("running"):
                state = dict(_stage1_4_refresh_state)
                return JSONResponse(
                    status_code=202,
                    content={
                        "ok": True,
                        "message": "stage1/4 refresh is already running",
                        **state,
                        "rows": len(_cached_rows),
                        "lastRefresh": _last_refresh,
                        "lastRefreshError": _last_refresh_error,
                    },
                )

            with _manual_refresh_state_lock:
                manual_running = bool(_manual_refresh_state.get("running"))
            with _payments_only_state_lock:
                payments_running = bool(_payments_only_state.get("running"))
            with _stage4_4_refresh_state_lock:
                stage4_running = bool(_stage4_4_refresh_state.get("running"))
            with _stage4_4_local_queue_lock:
                local_stage4_running = bool(_stage4_4_local_queue_state.get("running"))
            refresh_owner = str(_refresh_run_lock_state.get("owner") or "")
            partial_owner = str(_partial_refresh_lock_state.get("owner") or "")
            blockers = [
                name
                for name, active in (
                    ("manual-refresh", manual_running),
                    ("payments-only", payments_running),
                    ("stage4/4", stage4_running),
                    ("local-stage4/4", local_stage4_running),
                    (refresh_owner or "refresh-lock", bool(refresh_owner) or _refresh_run_lock.locked()),
                    (partial_owner or "partial-refresh-lock", bool(partial_owner) or _partial_refresh_lock.locked()),
                )
                if active
            ]
            if blockers:
                return JSONResponse(
                    status_code=409,
                    content={
                        "ok": False,
                        "error": "another runtime process is active",
                        "blockers": blockers,
                    },
                )

            _stage1_4_refresh_state.update(
                {
                    "running": True,
                    "requestedAt": datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
                    "requestedBy": username,
                    "requestedFrom": client_host,
                    "startedAt": None,
                    "finishedAt": None,
                    "lastOk": None,
                    "lastError": None,
                    "confirmedVersion": None,
                }
            )

    async def _run_stage1_4_refresh() -> None:
        global _cached_rows, _cached_fp, _last_refresh, _last_refresh_error, _last_confirmed_runtime_sync_check

        _set_stage1_4_refresh_state(startedAt=datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"))
        log(f"stage1/4 refresh requested by {username} from {client_host}")

        refresh_wait_timeout = max(60, MANUAL_REFRESH_TIMEOUT_SECONDS) + 180
        TOTAL_HARD_DEADLINE = refresh_wait_timeout + 180
        deadline_task: asyncio.Task | None = None

        async def _deadline_killer():
            await asyncio.sleep(TOTAL_HARD_DEADLINE)
            log(f"[refresh-1of4] hard deadline {TOTAL_HARD_DEADLINE}s reached — forcing running=False")
            _set_stage1_4_refresh_state(
                running=False,
                finishedAt=datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
                lastOk=False,
                lastError="stage1/4 refresh hard deadline exceeded",
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
                asyncio.to_thread(
                    refresh_cache_and_file,
                    True,
                    False,  # include_stage6=False — this is exactly what makes this "1/4"
                    MANUAL_REFRESH_PAGE_SIZE,
                    True,
                    False,
                    False,
                    f"manual-refresh-1of4:{username}",
                ),
                timeout=refresh_wait_timeout,
            )

            if not ran:
                log("[refresh-1of4] cycle was skipped — waiting for running cycle to complete")
                wait_deadline = time.time() + max(120, MANUAL_REFRESH_TIMEOUT_SECONDS)
                while time.time() < wait_deadline:
                    await asyncio.sleep(5)
                    lock_free = _refresh_run_lock.acquire(blocking=False)
                    if lock_free:
                        _refresh_run_lock.release()
                        break
                log("[refresh-1of4] running cycle completed; proceeding to publish from disk")

            if _last_refresh_error:
                raise RuntimeError(f"stage1/4 refresh cycle did not produce a fresh snapshot: {_last_refresh_error}")

            try:
                refreshed_comment_rows = refresh_comment_first_line_only(f"manual-refresh-1of4:{username}")
                log(
                    "stage1/4 refresh: comment-first-line refresh completed: "
                    f"ok={refreshed_comment_rows.get('ok')}, touched={refreshed_comment_rows.get('touched')}"
                )
            except Exception as comment_exc:
                log(f"stage1/4 refresh: comment-first-line refresh failed: {type(comment_exc).__name__}: {comment_exc}")

            candidate_rows = load_rows_from_path(Path(RUNTIME_DATA_FILE))
            candidate_meta = _read_runtime_meta()
            _publish_t0 = time.time()
            log(f"[refresh-1of4] starting github publish ({len(candidate_rows)} rows)")
            publish_source = "github-current"
            try:
                github_rows, _, github_pointer = await asyncio.wait_for(
                    asyncio.to_thread(
                        _publish_confirmed_runtime_snapshot_or_raise,
                        candidate_rows,
                        candidate_meta,
                    ),
                    timeout=120,
                )
                log(f"[refresh-1of4] github publish done in {time.time()-_publish_t0:.1f}s")
            except Exception as publish_exc:
                if RUNTIME_STRICT_GITHUB_POINTER:
                    log(
                        "[refresh-1of4] versioned github publish failed in strict mode: "
                        f"{type(publish_exc).__name__}: {publish_exc}"
                    )
                    raise RuntimeError(
                        f"strict runtime publish failed: {type(publish_exc).__name__}: {publish_exc}"
                    ) from publish_exc

                log(
                    f"[refresh-1of4] versioned github publish failed: {type(publish_exc).__name__}: {publish_exc}; "
                    "using local runtime snapshot instead"
                )
                github_rows = list(candidate_rows)
                github_meta = _runtime_normalize_meta(
                    dict(candidate_meta or {}),
                    github_rows,
                    pointer=previous_pointer,
                    fallback_source="consistency-recovery:stage1_4-refresh-fallback",
                )
                try:
                    github_pointer = _build_runtime_current_pointer(github_rows, github_meta)
                except Exception:
                    github_pointer = {}
                if github_pointer:
                    _write_local_confirmed_runtime(github_rows, github_meta, github_pointer)
                else:
                    _write_runtime_snapshot_files(github_rows, github_meta)
                publish_source = "local-runtime"
                log(f"[refresh-1of4] local runtime fallback done in {time.time()-_publish_t0:.1f}s")

            _cached_rows = list(github_rows)
            _cached_fp = rows_fingerprint(_cached_rows)
            _last_confirmed_runtime_sync_check = time.time()
            _last_refresh_error = None
            _last_refresh = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")

            confirmed_version = github_pointer.get("version") if github_pointer else None
            _set_stage1_4_refresh_state(confirmedVersion=confirmed_version, lastOk=True, lastError=None)
            log(
                "stage1/4 refresh finished: "
                f"rows={len(_cached_rows)}, lastRefresh={_last_refresh}, confirmedVersion={confirmed_version}, "
                f"source={publish_source}, user={username}, host={client_host}"
            )
        except Exception as exc:
            _cached_rows = list(previous_rows)
            _cached_fp = rows_fingerprint(_cached_rows)
            _write_local_confirmed_runtime(_cached_rows, previous_meta, previous_pointer)
            _last_refresh = previous_last_refresh
            _last_refresh_error = previous_last_refresh_error
            _last_confirmed_runtime_sync_check = previous_last_confirmed_sync_check

            if isinstance(exc, asyncio.TimeoutError):
                _set_stage1_4_refresh_state(
                    confirmedVersion=previous_pointer.get("version") if previous_pointer else None,
                    lastOk=False,
                    lastError="stage1/4 refresh timed out",
                )
                log("stage1/4 refresh timed out")
            else:
                _set_stage1_4_refresh_state(
                    confirmedVersion=previous_pointer.get("version") if previous_pointer else None,
                    lastOk=False,
                    lastError=str(exc),
                )
                log(f"stage1/4 refresh crashed and kept previous confirmed snapshot: {type(exc).__name__}: {exc}")
        finally:
            if deadline_task and not deadline_task.done():
                deadline_task.cancel()
            _set_stage1_4_refresh_state(
                running=False,
                finishedAt=datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
            )

    asyncio.create_task(_run_stage1_4_refresh())
    return JSONResponse(
        status_code=202,
        content={
            "ok": True,
            "message": "stage1/4 refresh started",
            **_stage1_4_refresh_snapshot(),
        },
    )


@app.get("/api/kp/refresh/stage1_4/status")
async def manual_refresh_stage1_4_status():
    return _stage1_4_refresh_snapshot()


@app.post("/api/kp/refresh/payments-only")
async def payments_only_refresh(request: Request):
    username = "anonymous"
    try:
        user = _get_user_from_request(request)
        username = str(user.get("username") or "anonymous")
    except HTTPException:
        # Allow endpoint usage from automations without auth cookie.
        pass

    with _refresh_coordination_lock, _payments_only_state_lock:
        if _stage1_4_blocks_runtime_writer(f"payments-only-refresh:{username}"):
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "error": "stage1/4 owns exclusive runtime cycle",
                    "blockers": ["stage1/4"],
                },
            )
        if _payments_only_state.get("running"):
            running_state = dict(_payments_only_state)
            running_state["rows"] = len(_cached_rows)
            running_state["lastRefresh"] = _last_refresh
            running_state["lastRefreshError"] = _last_refresh_error
            return JSONResponse(
                status_code=202,
                content={
                    "ok": True,
                    "message": "payments-only refresh is already running",
                    **running_state,
                },
            )

        _payments_only_state.update(
            {
                "running": True,
                "requestedAt": datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
                "requestedBy": username,
                "startedAt": None,
                "finishedAt": None,
                "lastOk": None,
                "lastError": None,
                "waitedSeconds": None,
                "paymentReceivedCount": None,
                "invoiceCreatedCount": None,
                "confirmedVersion": None,
            }
        )

    async def _run_payments_only_refresh() -> None:
        pause_reason = "payments-only-refresh"
        wait_started = time.time()
        _set_payments_only_state(startedAt=datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"))

        _set_refresh_pause(PAYMENTS_ONLY_PAUSE_SECONDS, pause_reason, username)
        try:
            # Wait for the main refresh lock (stage1/4 etc.) to be free instead of
            # forcing through in isolated mode — avoids two cycles writing rows at once.
            wait_deadline = wait_started + max(10, PAYMENTS_ONLY_HARD_DEADLINE_SECONDS)
            while time.time() < wait_deadline:
                lock_free = _refresh_run_lock.acquire(blocking=False)
                if lock_free:
                    _refresh_run_lock.release()
                    break
                await asyncio.sleep(2)
            else:
                log("payments-only refresh: main refresh lock still busy after full wait; skipping this cycle")

            remaining_budget = max(30, PAYMENTS_ONLY_HARD_DEADLINE_SECONDS - (time.time() - wait_started))
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    refresh_payments_only_for_cached_rows,
                    f"payments-only-refresh:{username}",
                    False,
                ),
                timeout=remaining_budget,
            )

            confirmed_version = None
            if result.get("ok"):
                # Mirror the 4/4 local watchdog: publish the confirmed runtime
                # pointer so the fingerprint-consistency check does not treat
                # this refresh's on-disk write as stale and roll it back.
                try:
                    candidate_rows = await asyncio.to_thread(load_rows_from_path, Path(RUNTIME_DATA_FILE))
                    candidate_meta = await asyncio.to_thread(_read_runtime_meta)
                    _, _, confirmed_pointer = await asyncio.to_thread(
                        _publish_confirmed_runtime_snapshot_or_raise,
                        candidate_rows,
                        candidate_meta,
                    )
                    confirmed_version = confirmed_pointer.get("version")
                    log(f"payments-only refresh: published confirmed runtime snapshot v{confirmed_version}")
                except Exception as publish_exc:
                    result["ok"] = False
                    result["error"] = f"publish-confirmed-pointer failed: {type(publish_exc).__name__}: {publish_exc}"
                    log(f"payments-only refresh: {result['error']}")

            _set_payments_only_state(
                lastOk=bool(result.get("ok")),
                lastError=result.get("error") or (None if result.get("ok") else result.get("skipped")),
                waitedSeconds=int(max(0, time.time() - wait_started)),
                paymentReceivedCount=result.get("paymentReceivedCount"),
                invoiceCreatedCount=result.get("invoiceCreatedCount"),
                confirmedVersion=confirmed_version,
            )
        except asyncio.TimeoutError:
            _set_payments_only_state(
                lastOk=False,
                lastError=(
                    f"payments-only refresh hard deadline {PAYMENTS_ONLY_HARD_DEADLINE_SECONDS}s exceeded "
                    "(worker thread may still be running in background; lock stays held until it exits)"
                ),
                waitedSeconds=int(max(0, time.time() - wait_started)),
            )
            log(f"payments-only refresh hard deadline {PAYMENTS_ONLY_HARD_DEADLINE_SECONDS}s exceeded")
        except Exception as exc:
            _set_payments_only_state(
                lastOk=False,
                lastError=str(exc),
                waitedSeconds=int(max(0, time.time() - wait_started)),
            )
            log(f"payments-only refresh task crashed: {type(exc).__name__}: {exc}")
        finally:
            _clear_refresh_pause(pause_reason)
            _set_payments_only_state(
                running=False,
                finishedAt=datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
            )

    app.state.payments_only_task = asyncio.create_task(_run_payments_only_refresh())
    return JSONResponse(
        status_code=202,
        content={
            "ok": True,
            "message": "payments-only refresh started",
            **_payments_only_snapshot(),
        },
    )


@app.get("/api/kp/refresh/payments-only/status")
async def payments_only_refresh_status():
    return _payments_only_snapshot()


@app.post("/api/kp/refresh/payments-only/kp/{kp_number}")
async def payments_only_refresh_single_kp(kp_number: str, request: Request):
    username = "anonymous"
    try:
        user = _get_user_from_request(request)
        username = str(user.get("username") or "anonymous")
    except HTTPException:
        # Allow endpoint usage from automations without auth cookie.
        pass

    result = await asyncio.to_thread(
        refresh_payments_for_single_kp_from_seed,
        kp_number,
        f"payments-only-kp-seed:{username}",
    )
    status_code = 200 if result.get("ok") else 202
    return JSONResponse(status_code=status_code, content=result)


@app.post("/api/kp/refresh/stage4_4/local-queue")
async def enqueue_local_stage4_4_refresh(request: Request):
    """Queue a 4/4 task for a local watchdog agent (Windows machine).

    This endpoint DOES NOT run 1C scan on Render. It only records a queue task
    that a trusted local agent can claim and execute locally.
    """
    user = _get_user_from_request(request)
    username = str(user.get("username") or "anonymous")
    client_host = request.client.host if request.client else "unknown"

    with _refresh_coordination_lock, _stage4_4_local_queue_lock:
        if _stage1_4_blocks_runtime_writer(f"local-stage4-queue:{username}"):
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "error": "stage1/4 owns exclusive runtime cycle",
                    "blockers": ["stage1/4"],
                },
            )
        if _stage4_4_local_queue_state.get("running"):
            state = dict(_stage4_4_local_queue_state)
            state["rows"] = len(_cached_rows)
            state["lastRefresh"] = _last_refresh
            state["lastRefreshError"] = _last_refresh_error
            return JSONResponse(
                status_code=202,
                content={
                    "ok": True,
                    "message": "stage4/4 local task is already queued or running",
                    **state,
                },
            )

        task_id = uuid.uuid4().hex
        _stage4_4_local_queue_state.update(
            {
                "running": True,
                "phase": "queued",
                "taskId": task_id,
                "requestedAt": datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
                "requestedBy": username,
                "requestedFrom": client_host,
                "claimedAt": None,
                "claimedBy": None,
                "startedAt": None,
                "finishedAt": None,
                "lastOk": None,
                "lastError": None,
                "waitedSeconds": None,
                "paymentReceivedCount": None,
                "invoiceCreatedCount": None,
                "confirmedVersion": None,
                "resultSource": None,
            }
        )

    log(f"stage4/4 local queue: task {task_id} queued by {username} from {client_host}")
    return JSONResponse(
        status_code=202,
        content={
            "ok": True,
            "message": "stage4/4 local task queued",
            **_stage4_4_local_queue_snapshot(),
        },
    )


@app.get("/api/kp/refresh/stage4_4/local-queue/status")
async def local_stage4_4_queue_status(request: Request):
    _get_user_from_request(request)
    return _stage4_4_local_queue_snapshot()


@app.get("/api/kp/local-agent/stage4_4/state")
async def local_stage4_4_state(request: Request):
    """Read-only queue state for trusted local agents.

    Unlike /claim, this endpoint does not mutate queue state and therefore
    cannot accidentally steal a queued task during diagnostics.
    """
    _require_local_stage4_agent_auth(request)
    return {"ok": True, "state": _stage4_4_local_queue_snapshot()}


@app.post("/api/kp/local-agent/stage4_4/claim")
async def local_stage4_4_claim(request: Request):
    runner_id = _require_local_stage4_agent_auth(request)

    with _refresh_coordination_lock, _stage4_4_local_queue_lock:
        if _stage1_4_blocks_runtime_writer(f"local-stage4-agent:{runner_id}"):
            state = dict(_stage4_4_local_queue_state)
            state["rows"] = len(_cached_rows)
            state["lastRefresh"] = _last_refresh
            state["lastRefreshError"] = _last_refresh_error
            return {
                "ok": True,
                "claimed": False,
                "blockedBy": "stage1/4",
                "state": state,
            }
        running = bool(_stage4_4_local_queue_state.get("running"))
        phase = str(_stage4_4_local_queue_state.get("phase") or "")
        task_id = str(_stage4_4_local_queue_state.get("taskId") or "")
        requested_at = _stage4_4_local_queue_state.get("requestedAt")

        if not running or phase != "queued" or not task_id:
            state = dict(_stage4_4_local_queue_state)
            state["rows"] = len(_cached_rows)
            state["lastRefresh"] = _last_refresh
            state["lastRefreshError"] = _last_refresh_error
            return {"ok": True, "claimed": False, "state": state}

        _stage4_4_local_queue_state.update(
            {
                "phase": "running",
                "claimedAt": datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
                "claimedBy": runner_id,
                "startedAt": datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    log(f"stage4/4 local queue: task {task_id} claimed by {runner_id}")
    return {
        "ok": True,
        "claimed": True,
        "taskId": task_id,
        "requestedAt": requested_at,
    }


@app.post("/api/kp/local-agent/stage4_4/report")
async def local_stage4_4_report(payload: LocalStage44Report, request: Request):
    runner_id = _require_local_stage4_agent_auth(request)

    now_text = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")

    with _stage4_4_local_queue_lock:
        current_task_id = str(_stage4_4_local_queue_state.get("taskId") or "")
        current_running = bool(_stage4_4_local_queue_state.get("running"))
        requested_at_text = str(_stage4_4_local_queue_state.get("requestedAt") or "")

        if not current_task_id or payload.taskId != current_task_id:
            raise HTTPException(status_code=409, detail="Task mismatch or no active local queue task")
        if not current_running:
            raise HTTPException(status_code=409, detail="Local queue task is not running")

        waited_seconds = None
        try:
            if requested_at_text:
                requested_at = datetime.strptime(requested_at_text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=_TZ_MSK)
                waited_seconds = int(max(0, (datetime.now(_TZ_MSK) - requested_at).total_seconds()))
        except Exception:
            waited_seconds = None

        _stage4_4_local_queue_state.update(
            {
                "running": False,
                "phase": "success" if payload.ok else "error",
                "finishedAt": now_text,
                "lastOk": bool(payload.ok),
                "lastError": None if payload.ok else str(payload.error or "local-agent reported failure"),
                "waitedSeconds": waited_seconds,
                "paymentReceivedCount": payload.paymentReceivedCount,
                "invoiceCreatedCount": payload.invoiceCreatedCount,
                "confirmedVersion": payload.confirmedVersion,
                "resultSource": payload.source or "local-agent",
                "claimedBy": runner_id,
            }
        )

    log(
        "stage4/4 local queue: task "
        f"{payload.taskId} reported by {runner_id}, ok={payload.ok}, "
        f"confirmedVersion={payload.confirmedVersion}, source={payload.source or 'local-agent'}"
    )
    return {"ok": True, "state": _stage4_4_local_queue_snapshot()}


@app.post("/api/kp/refresh/stage4_4")
async def manual_refresh_stage4_4(request: Request):
    """Проект 4/4 (не связан с кнопкой "Обновить" и с payments-only): поиск
    оплат (Document_ПоступлениеБезналичныхДенежныхСредств) и запись
    результата в runtime-кэш, откуда его читает блок 3 на /admin/dashboard.
    Это обёртка над уже существующей refresh_payments_only_for_cached_rows,
    но со skip_invoice_scan=True — скан накладных (Document_РеализацияТова
    ровУслуг) пропускается, т.к. он не влияет на "Оплата получена"/блок 3
    (влияет только на invoiceCreated), а скан заказов остаётся обязательным,
    т.к. без него не из чего брать номера заказов для сопоставления с
    назначением платежа. Отдельное состояние, чтобы не смешиваться со
    статусом кнопки "Оплаты" и обычного refresh."""
    username = "anonymous"
    try:
        user = _get_user_from_request(request)
        username = str(user.get("username") or "anonymous")
    except HTTPException:
        pass

    client_host = request.client.host if request.client else "unknown"

    with _refresh_coordination_lock, _stage4_4_refresh_state_lock:
        if _stage1_4_blocks_runtime_writer(f"manual-refresh-4of4:{username}"):
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "error": "stage1/4 owns exclusive runtime cycle",
                    "blockers": ["stage1/4"],
                },
            )
        if _stage4_4_refresh_state.get("running"):
            state = dict(_stage4_4_refresh_state)
            return JSONResponse(
                status_code=202,
                content={
                    "ok": True,
                    "message": "stage4/4 refresh is already running",
                    **state,
                    "rows": len(_cached_rows),
                    "lastRefresh": _last_refresh,
                    "lastRefreshError": _last_refresh_error,
                },
            )

        _stage4_4_refresh_state.update(
            {
                "running": True,
                "requestedAt": datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
                "requestedBy": username,
                "requestedFrom": client_host,
                "startedAt": None,
                "finishedAt": None,
                "lastOk": None,
                "lastError": None,
                "waitedSeconds": None,
                "paymentReceivedCount": None,
                "invoiceCreatedCount": None,
            }
        )

    async def _run_stage4_4_refresh() -> None:
        global _cached_rows, _cached_fp, _last_confirmed_runtime_sync_check

        pause_reason = "stage4-4-refresh"
        wait_started = time.time()
        _set_stage4_4_refresh_state(startedAt=datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"))
        log(f"stage4/4 refresh requested by {username} from {client_host}")

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

        _set_refresh_pause(PAYMENTS_ONLY_PAUSE_SECONDS, pause_reason, username)
        try:
            # Wait for the main refresh lock (stage1/4 etc.) to be free instead of
            # forcing through in isolated mode — avoids two cycles writing rows at once.
            wait_deadline = wait_started + max(10, PAYMENTS_ONLY_HARD_DEADLINE_SECONDS)
            while time.time() < wait_deadline:
                lock_free = _refresh_run_lock.acquire(blocking=False)
                if lock_free:
                    _refresh_run_lock.release()
                    break
                await asyncio.sleep(2)
            else:
                log("stage4/4 refresh: main refresh lock still busy after full wait; skipping this cycle")

            remaining_budget = max(30, PAYMENTS_ONLY_HARD_DEADLINE_SECONDS - (time.time() - wait_started))
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    refresh_payments_only_for_cached_rows,
                    f"manual-refresh-4of4:{username}",
                    False,
                    True,  # skip_invoice_scan=True — this is exactly what makes this "4/4" faster
                ),
                timeout=remaining_budget,
            )

            # Публикуем полный версионированный снапшот в GitHub/UI (тот же механизм,
            # что и у 1/4) — refresh_payments_only_for_cached_rows сам по себе делает
            # только legacy-запись, чего недостаточно для доставки на дашборд.
            confirmed_version = None
            publish_source = "github-current"
            if result.get("ok"):
                try:
                    candidate_rows = load_rows_from_path(Path(RUNTIME_DATA_FILE))
                    candidate_meta = _read_runtime_meta()
                    _publish_t0 = time.time()
                    log(f"[refresh-4of4] starting github publish ({len(candidate_rows)} rows)")
                    try:
                        github_rows, _, github_pointer = await asyncio.wait_for(
                            asyncio.to_thread(
                                _publish_confirmed_runtime_snapshot_or_raise,
                                candidate_rows,
                                candidate_meta,
                            ),
                            timeout=120,
                        )
                        log(f"[refresh-4of4] github publish done in {time.time()-_publish_t0:.1f}s")
                        _cached_rows = list(github_rows)
                        _cached_fp = rows_fingerprint(_cached_rows)
                        _last_confirmed_runtime_sync_check = time.time()
                        confirmed_version = github_pointer.get("version") if github_pointer else None
                        publish_source = "github-current"
                    except Exception as publish_exc:
                        if RUNTIME_STRICT_GITHUB_POINTER:
                            log(
                                "[refresh-4of4] versioned github publish failed in strict mode: "
                                f"{type(publish_exc).__name__}: {publish_exc}"
                            )
                            raise RuntimeError(
                                f"strict runtime publish failed: {type(publish_exc).__name__}: {publish_exc}"
                            ) from publish_exc
                        log(
                            "[refresh-4of4] versioned github publish failed: "
                            f"{type(publish_exc).__name__}: {publish_exc}; using local runtime fallback instead"
                        )
                        github_rows = list(candidate_rows)
                        github_meta = _runtime_normalize_meta(
                            dict(candidate_meta or {}),
                            github_rows,
                            pointer=previous_pointer,
                            fallback_source="consistency-recovery:stage4_4-refresh-fallback",
                        )
                        try:
                            github_pointer = _build_runtime_current_pointer(github_rows, github_meta)
                        except Exception:
                            github_pointer = {}
                        if github_pointer:
                            _write_local_confirmed_runtime(github_rows, github_meta, github_pointer)
                        else:
                            _write_runtime_snapshot_files(github_rows, github_meta)
                        _cached_rows = list(github_rows)
                        _cached_fp = rows_fingerprint(_cached_rows)
                        _last_confirmed_runtime_sync_check = time.time()
                        confirmed_version = github_pointer.get("version") if github_pointer else None
                        publish_source = "local-runtime"
                        log(f"[refresh-4of4] local runtime fallback done in {time.time()-_publish_t0:.1f}s")
                except Exception as publish_outer_exc:
                    result["ok"] = False
                    result["error"] = f"github publish failed: {type(publish_outer_exc).__name__}: {publish_outer_exc}"
                    log(f"[refresh-4of4] publish step failed: {type(publish_outer_exc).__name__}: {publish_outer_exc}")

            _set_stage4_4_refresh_state(
                lastOk=bool(result.get("ok")),
                lastError=result.get("error") or (None if result.get("ok") else result.get("skipped")),
                waitedSeconds=int(max(0, time.time() - wait_started)),
                paymentReceivedCount=result.get("paymentReceivedCount"),
                invoiceCreatedCount=result.get("invoiceCreatedCount"),
                confirmedVersion=confirmed_version,
            )
            log(
                "stage4/4 refresh finished: "
                f"ok={result.get('ok')}, paymentReceivedCount={result.get('paymentReceivedCount')}, "
                f"confirmedVersion={confirmed_version}, source={publish_source}, user={username}, host={client_host}"
            )
        except asyncio.TimeoutError:
            _set_stage4_4_refresh_state(
                lastOk=False,
                lastError=(
                    f"stage4/4 refresh hard deadline {PAYMENTS_ONLY_HARD_DEADLINE_SECONDS}s exceeded "
                    "(worker thread may still be running in background; lock stays held until it exits)"
                ),
                waitedSeconds=int(max(0, time.time() - wait_started)),
            )
            log(f"stage4/4 refresh hard deadline {PAYMENTS_ONLY_HARD_DEADLINE_SECONDS}s exceeded")
        except Exception as exc:
            _set_stage4_4_refresh_state(
                lastOk=False,
                lastError=str(exc),
                waitedSeconds=int(max(0, time.time() - wait_started)),
            )
            log(f"stage4/4 refresh task crashed: {type(exc).__name__}: {exc}")
        finally:
            _clear_refresh_pause(pause_reason)
            _set_stage4_4_refresh_state(
                running=False,
                finishedAt=datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
            )

    asyncio.create_task(_run_stage4_4_refresh())
    return JSONResponse(
        status_code=202,
        content={
            "ok": True,
            "message": "stage4/4 refresh started",
            **_stage4_4_refresh_snapshot(),
        },
    )


@app.get("/api/kp/refresh/stage4_4/status")
async def manual_refresh_stage4_4_status():
    return _stage4_4_refresh_snapshot()


async def _run_system_checkpoint_recovery(trigger: str) -> None:
    """Best-effort auto recovery for pending refresh checkpoints."""
    global _cached_rows, _cached_fp, _last_refresh, _last_refresh_error, _last_confirmed_runtime_sync_check

    if _is_refresh_paused():
        log(f"[checkpoint-recovery] skipped due to refresh pause (trigger={trigger})")
        return

    now_msk = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")
    _set_manual_refresh_state(
        running=True,
        requestedAt=now_msk,
        requestedBy="system:checkpoint-recovery",
        requestedFrom=trigger,
        startedAt=now_msk,
        finishedAt=None,
        lastError=None,
        confirmedVersion=None,
    )
    log(f"[checkpoint-recovery] started (trigger={trigger})")
    try:
        ran = False
        # The full refresh lock can be held briefly by another cycle.
        # Retry instead of failing recovery immediately.
        for attempt in range(1, 7):
            ran = await asyncio.to_thread(
                refresh_cache_and_file,
                True,
                MANUAL_REFRESH_INCLUDE_STAGE6,
                MANUAL_REFRESH_PAGE_SIZE,
                True,
                False,
                False,
                "checkpoint-recovery",
            )
            if ran:
                break
            log(f"[checkpoint-recovery] refresh lock busy, retry {attempt}/6")
            await asyncio.sleep(5)

        if ran and not _last_refresh_error:
            # Recovery must publish the confirmed pointer too; otherwise strict
            # mode keeps serving an older GitHub snapshot forever.
            candidate_rows = load_rows_from_path(Path(RUNTIME_DATA_FILE))
            candidate_meta = _read_runtime_meta()
            if not candidate_rows:
                raise RuntimeError("checkpoint recovery produced empty runtime snapshot")

            publish_source = "github-current"
            try:
                github_rows, _, github_pointer = await asyncio.wait_for(
                    asyncio.to_thread(
                        _publish_confirmed_runtime_snapshot_or_raise,
                        candidate_rows,
                        candidate_meta,
                    ),
                    timeout=180,
                )
            except Exception as publish_exc:
                if RUNTIME_STRICT_GITHUB_POINTER:
                    raise RuntimeError(
                        "checkpoint recovery strict publish failed: "
                        f"{type(publish_exc).__name__}: {publish_exc}"
                    ) from publish_exc

                github_rows = list(candidate_rows)
                github_meta = _runtime_normalize_meta(
                    dict(candidate_meta or {}),
                    github_rows,
                    pointer=_read_runtime_current_pointer(),
                    fallback_source="consistency-recovery:checkpoint-fallback",
                )
                github_pointer = _build_runtime_current_pointer(github_rows, github_meta)
                _write_local_confirmed_runtime(github_rows, github_meta, github_pointer)
                publish_source = "local-runtime"
                log(
                    "[checkpoint-recovery] github publish failed in non-strict mode; "
                    "using local runtime snapshot"
                )

            _cached_rows = list(github_rows)
            _cached_fp = rows_fingerprint(_cached_rows)
            _last_confirmed_runtime_sync_check = time.time()
            _last_refresh_error = None
            _last_refresh = datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S")

            confirmed_version = github_pointer.get("version") if github_pointer else None
            _set_manual_refresh_state(lastOk=True, lastError=None, confirmedVersion=confirmed_version)
            log(
                "[checkpoint-recovery] completed successfully "
                f"(rows={len(_cached_rows)}, confirmedVersion={confirmed_version}, source={publish_source})"
            )
        elif ran and _last_refresh_error:
            error_text = str(_last_refresh_error)
            _set_manual_refresh_state(lastOk=False, lastError=error_text)
            log(f"[checkpoint-recovery] ended with refresh error: {error_text}")
        else:
            # Recovery could not acquire the refresh lock in time.
            # Keep state neutral to avoid surfacing a misleading manual-refresh error.
            _set_manual_refresh_state(lastOk=None, lastError=None)
            log("[checkpoint-recovery] skipped (refresh lock busy); keeping manual refresh state neutral")
    except Exception as exc:
        _set_manual_refresh_state(lastOk=False, lastError=str(exc))
        log(f"[checkpoint-recovery] crashed: {type(exc).__name__}: {exc}")
    finally:
        _set_manual_refresh_state(
            running=False,
            finishedAt=datetime.now(_TZ_MSK).strftime("%Y-%m-%d %H:%M:%S"),
        )


@app.get("/api/kp/refresh/status")
async def manual_refresh_status():
    with _manual_refresh_state_lock:
        running = bool(_manual_refresh_state.get("running"))
    with _stage1_4_refresh_state_lock:
        stage1_running = bool(_stage1_4_refresh_state.get("running"))

    if not running and not stage1_running and not _is_refresh_paused() and _has_pending_refresh_checkpoint():
        task = getattr(app.state, "checkpoint_recovery_task", None)
        if task is None or task.done():
            app.state.checkpoint_recovery_task = asyncio.create_task(
                _run_system_checkpoint_recovery("status-endpoint")
            )
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
        "refreshLockState": _lock_state_snapshot(_refresh_run_lock_state),
        "partialRefreshLockState": _lock_state_snapshot(_partial_refresh_lock_state),
        "runtimeStrictGithubOnly": RUNTIME_STRICT_GITHUB_POINTER,
        "githubRuntimeSync": bool(GITHUB_TOKEN and GITHUB_REPO),
        "manualRefreshState": dict(_manual_refresh_state),
        "startupLiveRefresh": _startup_live_refresh_snapshot(),
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
                    if _in_target_window(dt):
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

    # Apply block3-compatible seed payment overlay on response rows so
    # payment status stays consistent even when a background sync reloads an
    # older runtime snapshot.
    overlay_rows: list[dict] = [dict(r) for r in list(rows or [])]
    _apply_seed_payment_promotions_for_all_rows(overlay_rows)

    output = []
    for row in overlay_rows:
        formatted = format_row_for_client(row)
        formatted["statusKpComputed"] = _compute_status_for_row(formatted, rules)
        output.append(formatted)
    return output


@app.get("/api/kp/all")
async def get_all_kp(request: Request):
    user = _get_user_from_request(request)
    if not _startup_live_refresh_gate_open():
        raise HTTPException(status_code=503, detail=_startup_live_refresh_gate_detail())
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
    try:
        persist_outcome = await asyncio.to_thread(
            _persist_payment_match_result_to_cache, result.get("rows") or []
        )
        if persist_outcome.get("promoted"):
            log(f"block3 view persisted paymentReceived promotions: {persist_outcome}")
    except Exception as exc:
        log(f"block3 persist failed: {type(exc).__name__}: {exc}")
    return {"ok": True, **result}


@app.get("/api/admin/payment-coverage-audit")
async def admin_payment_coverage_audit(request: Request):
    # Same auth model as payment match table: admin cookie OR user role=admin.
    is_admin = _get_admin_username(request)
    if not is_admin:
        user = _get_user_from_request(request)
        role = str(user.get("role") or "").strip().lower()
        if role != "admin":
            raise HTTPException(status_code=403, detail="Admin access required")

    result = await asyncio.to_thread(_build_payment_coverage_audit, 300)
    if result.get("ok") is False and result.get("detail") == "KP data is not available yet":
        raise HTTPException(status_code=503, detail=result.get("detail"))
    return result


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
    return result


@app.post("/api/kp/process/send-to-client")
async def process_send_to_client_status(request: Request):
    user = _get_user_from_request(request)
    result = await asyncio.to_thread(_process_send_to_client_status_for_user, user)
    if result.get("ok") is False and result.get("detail") == "KP data is not available yet":
        raise HTTPException(status_code=503, detail=result.get("detail"))
    return result


@app.post("/api/kp/process/client-thinking-reminder")
async def process_client_thinking_reminder_status(request: Request):
    user = _get_user_from_request(request)
    result = await asyncio.to_thread(_process_client_thinking_reminder_for_user, user)
    if result.get("ok") is False and result.get("detail") == "KP data is not available yet":
        raise HTTPException(status_code=503, detail=result.get("detail"))
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
            if not _startup_live_refresh_gate_open():
                await asyncio.sleep(2)
                continue
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


@app.middleware("http")
async def _add_no_cache_for_assets(request: Request, call_next):
    response = await call_next(request)
    path = request.url.path
    if path.endswith(".js") or path.endswith(".css"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


app.mount("/", StaticFiles(directory=".", html=True), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
