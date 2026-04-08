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


def load_rows_from_file() -> list:
    path = Path(DATA_FILE)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    data.sort(key=lambda x: x.get("createdAt", ""), reverse=True)
    return data


def save_rows(rows: list) -> None:
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def fetch_rows_from_odata() -> list:
    headers = _build_headers()
    rows = []
    skip = 0
    page_size = 500

    while True:
        params = {
            "$select": ",".join(SELECT_FIELDS),
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
            values = [item.get(f) or "" for f in SELECT_FIELDS]
            number, dt_raw, status, info_text, comment = values[0], values[1], values[2], values[3], values[4]
            try:
                dt = datetime.fromisoformat(str(dt_raw).replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                continue

            if TARGET_START <= dt <= TARGET_END:
                rows.append(
                    {
                        "number": number,
                        "createdAt": dt.strftime("%Y-%m-%d %H:%M:%S"),
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
    await asyncio.to_thread(refresh_cache_and_file)
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
