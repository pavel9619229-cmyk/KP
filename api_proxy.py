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

TARGET_START = datetime(2026, 3, 1, 0, 0, 0)
TARGET_END = datetime(2026, 4, 30, 23, 59, 59)

SELECT_FIELDS = [
    "Number",
    "Date",
    "Статус",
    "ПрочаяДополнительнаяИнформацияТекст",
    "Комментарий",
]

_cached_rows = []
_cached_fp = ""
_last_refresh = None
_customer_name_cache = {}


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


def resolve_customer_name_for_ref(ref_key: str, headers: dict) -> str:
    if not ref_key:
        return ""
    if ref_key in _customer_name_cache:
        return _customer_name_cache[ref_key]

    try:
        doc_resp = requests.get(
            f"{BASE}/{ENTITY}(guid'{ref_key}')",
            headers=headers,
            timeout=60,
            verify=False,
        )
        if doc_resp.status_code != 200:
            _customer_name_cache[ref_key] = ""
            return ""

        row = doc_resp.json()
        nav_links = [v for k, v in row.items() if k.endswith("@navigationLinkUrl")]

        best_description = ""
        best_score = 0

        for rel in nav_links:
            try:
                nav_resp = requests.get(
                    f"{BASE}/{rel}",
                    headers=headers,
                    timeout=60,
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
    except Exception:
        _customer_name_cache[ref_key] = ""
        return ""


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


def fetch_rows_from_odata() -> list:
    headers = _build_headers()

    rows = []
    skip = 0
    page_size = 500

    while True:
        params = {
            "$select": "Ref_Key," + ",".join(SELECT_FIELDS),
            "$top": str(page_size),
            "$skip": str(skip),
        }

        resp = None
        for _ in range(3):
            resp = requests.get(
                f"{BASE}/{ENTITY}",
                headers=headers,
                params=params,
                timeout=90,
                verify=False,
            )
            if resp.status_code == 200:
                break
            time.sleep(1)

        if resp is None or resp.status_code != 200:
            break

        batch = resp.json().get("value", [])
        if not batch:
            break

        for item in batch:
            ref_key = item.get("Ref_Key") or ""
            values = [item.get(f) or "" for f in SELECT_FIELDS]
            number, dt_raw, status, info_text, comment = values[0], values[1], values[2], values[3], values[4]
            customer_name = resolve_customer_name_for_ref(str(ref_key), headers)
            try:
                dt = datetime.fromisoformat(str(dt_raw).replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                continue

            if TARGET_START <= dt <= TARGET_END:
                rows.append(
                    {
                        "number": number,
                        "createdAt": dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "customerName": customer_name,
                        "status": status,
                        "additionalInfoFirstLine": first_line(info_text, comment),
                    }
                )

        skip += page_size

    rows.sort(key=lambda x: x["createdAt"], reverse=True)
    return rows


def refresh_cache_and_file() -> None:
    global _cached_rows, _cached_fp, _last_refresh

    fetched = fetch_rows_from_odata()
    if fetched:
        save_rows(fetched)
        _cached_rows = fetched
        _cached_fp = rows_fingerprint(fetched)
        _last_refresh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return

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
