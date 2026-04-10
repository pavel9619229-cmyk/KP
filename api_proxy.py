#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import base64
import json
import os
import re
import time
from datetime import datetime
from html import unescape
from pathlib import Path

import requests
import urllib3
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

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
DATA_FILE = os.getenv("DATA_FILE", "kp_2026_march_april.json")
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "60"))
ENRICH_PER_REFRESH = int(os.getenv("ENRICH_PER_REFRESH", "8"))
DOC_TIMEOUT_SECONDS = float(os.getenv("DOC_TIMEOUT_SECONDS", "2.0"))
NAV_TIMEOUT_SECONDS = float(os.getenv("NAV_TIMEOUT_SECONDS", "1.2"))
NAV_LINK_LIMIT = int(os.getenv("NAV_LINK_LIMIT", "4"))

TARGET_START = datetime(2026, 3, 1, 0, 0, 0)
TARGET_END = datetime(2026, 4, 30, 23, 59, 59)

LIGHT_SELECT_FIELDS = [
    "Number",
    "Date",
    "Статус",
]

_cached_rows = []
_cached_fp = ""
_last_refresh = None
_customer_name_cache = {}
_additional_info_cache = {}


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def _build_headers() -> dict:
    creds = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode("utf-8")).decode("ascii")
    return {
        "Authorization": f"Basic {creds}",
        "Accept": "application/json",
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
    try:
        doc_resp = requests.get(
            f"{BASE}/{ENTITY}(guid'{ref_key}')",
            headers=headers,
            timeout=timeout,
            verify=False,
        )
        if doc_resp.status_code != 200:
            return {}
        doc = doc_resp.json()
        return doc if isinstance(doc, dict) else {}
    except Exception:
        return {}


def resolve_customer_name_for_ref(ref_key: str, headers: dict, doc: dict | None = None) -> str:
    if not ref_key:
        return ""
    if ref_key in _customer_name_cache:
        return _customer_name_cache[ref_key]

    row = doc or _fetch_doc_by_ref(ref_key, headers, timeout=DOC_TIMEOUT_SECONDS)
    if not row:
        _customer_name_cache[ref_key] = ""
        return ""

    nav_links = [v for k, v in row.items() if k.endswith("@navigationLinkUrl")]

    best_description = ""
    best_score = 0

    for rel in nav_links[:NAV_LINK_LIMIT]:
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

            candidate_score = score_customer_candidate(nav_obj)
            if candidate_score > best_score:
                best_score = candidate_score
                best_description = str(nav_obj.get("Description") or "").strip()
        except Exception:
            continue

    _customer_name_cache[ref_key] = best_description
    return best_description


def resolve_additional_info_for_ref(ref_key: str, headers: dict, doc: dict | None = None) -> str:
    if not ref_key:
        return ""
    if ref_key in _additional_info_cache:
        return _additional_info_cache[ref_key]

    row = doc or _fetch_doc_by_ref(ref_key, headers, timeout=DOC_TIMEOUT_SECONDS)
    if not row:
        _additional_info_cache[ref_key] = ""
        return ""

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

    _additional_info_cache[ref_key] = best_line
    return best_line


def load_rows_from_file() -> list:
    path = Path(DATA_FILE)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    for row in data:
        if "customerName" not in row:
            row["customerName"] = ""
    data.sort(key=lambda x: x.get("createdAt", ""), reverse=True)
    return data


def save_rows(rows: list) -> None:
    for row in rows:
        if "customerName" not in row:
            row["customerName"] = ""
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def build_known_rows_lookup() -> dict:
    known = {}
    for source_row in load_rows_from_file() + list(_cached_rows):
        number = source_row.get("number")
        if number and number not in known:
            known[number] = source_row
    return known


def get_total_count(headers: dict) -> int:
    resp = requests.get(
        f"{BASE}/{ENTITY}/$count",
        headers=headers,
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

    total_count = get_total_count(headers)
    if total_count <= 0:
        return []

    skip = ((total_count - 1) // page_size) * page_size

    while True:
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
                    timeout=45,
                    verify=False,
                )
                if resp.status_code == 200:
                    break
            except requests.RequestException:
                resp = None
            time.sleep(1)

        if resp is None or resp.status_code != 200:
            break

        batch = resp.json().get("value", [])
        if not batch:
            break

        batch_dates = []

        for item in batch:
            ref_key = item.get("Ref_Key") or ""
            values = [item.get(f) or "" for f in LIGHT_SELECT_FIELDS]
            number, dt_raw, status = values[0], values[1], values[2]
            try:
                dt = datetime.fromisoformat(str(dt_raw).replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                continue

            batch_dates.append(dt)

            if TARGET_START <= dt <= TARGET_END:
                known_row = known_rows.get(number, {})
                rows.append(
                    {
                        "refKey": str(ref_key),
                        "number": number,
                        "createdAt": dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "customerName": known_row.get("customerName", ""),
                        "status": status,
                        "additionalInfoFirstLine": known_row.get("additionalInfoFirstLine", ""),
                    }
                )

        if batch_dates and max(batch_dates) < TARGET_START:
            break

        if skip == 0:
            break

        skip = max(0, skip - page_size)

    rows.sort(key=lambda x: x["createdAt"], reverse=True)

    enriched = 0
    for row in rows:
        if enriched >= ENRICH_PER_REFRESH:
            break

        ref_key = row.get("refKey", "")
        if not ref_key:
            continue

        need_customer = not (row.get("customerName") or "").strip()
        need_info = not (row.get("additionalInfoFirstLine") or "").strip()
        if not need_customer and not need_info:
            continue

        doc = _fetch_doc_by_ref(ref_key, headers, timeout=DOC_TIMEOUT_SECONDS)
        if not doc:
            continue

        if need_info:
            line = resolve_additional_info_for_ref(ref_key, headers, doc=doc)
            if line:
                row["additionalInfoFirstLine"] = line

        if need_customer:
            customer = resolve_customer_name_for_ref(ref_key, headers, doc=doc)
            if customer:
                row["customerName"] = customer

        enriched += 1

    for row in rows:
        row.pop("refKey", None)

    return rows


def refresh_cache_and_file() -> None:
    global _cached_rows, _cached_fp, _last_refresh

    try:
        fetched = fetch_rows_from_odata()
        if fetched:
            save_rows(fetched)
            _cached_rows = fetched
            _cached_fp = rows_fingerprint(fetched)
            _last_refresh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log(f"refresh success: {len(fetched)} rows")
            return

        log("refresh returned 0 rows, using file fallback")
    except Exception as exc:
        log(f"refresh failed: {exc}")

    fallback = load_rows_from_file()
    _cached_rows = fallback
    _cached_fp = rows_fingerprint(fallback)
    _last_refresh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


async def refresh_loop() -> None:
    while True:
        try:
            await asyncio.to_thread(refresh_cache_and_file)
        except Exception:
            pass
        await asyncio.sleep(REFRESH_SECONDS)


@app.on_event("startup")
async def on_startup() -> None:
    global _cached_rows, _cached_fp, _last_refresh
    _cached_rows = load_rows_from_file()
    _cached_fp = rows_fingerprint(_cached_rows)
    _last_refresh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log(f"startup cache loaded: {len(_cached_rows)} rows")
    app.state.refresh_task = asyncio.create_task(refresh_loop())


@app.on_event("shutdown")
async def on_shutdown() -> None:
    task = getattr(app.state, "refresh_task", None)
    if task:
        task.cancel()


@app.get("/")
async def root():
    return FileResponse("index.html", media_type="text/html")


@app.get("/healthz")
async def healthz():
    return {
        "ok": True,
        "rows": len(_cached_rows),
        "lastRefresh": _last_refresh,
    }


@app.get("/api/kp/all")
async def get_all_kp():
    return _cached_rows


@app.websocket("/ws/kp")
async def ws_kp(websocket: WebSocket):
    await websocket.accept()
    previous_fp = ""

    try:
        while True:
            current_fp = _cached_fp
            if current_fp != previous_fp:
                previous_fp = current_fp
                await websocket.send_json(
                    {
                        "type": "rows",
                        "updatedAt": _last_refresh,
                        "rows": _cached_rows,
                    }
                )
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        return


app.mount("/", StaticFiles(directory=".", html=True), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
