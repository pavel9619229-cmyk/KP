import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from threading import Lock

import requests

import api_proxy as core
import kp_max_live_rows as live_rows
import kp_max_navigation as nav

ZERO_GUID = "00000000-0000-0000-0000-000000000000"
_CREATE_LOCK = Lock()
_RECENT: dict[str, dict] = {}
DUPLICATE_GUARD_SECONDS = 8


def _base() -> str:
    return str(core.BASE).strip().strip('"').strip("'").rstrip("/")


def _norm_number(value: str) -> str:
    m = re.search(r"(\d+)$", str(value or ""))
    return str(int(m.group(1))) if m else str(value or "").strip()


def _now_1c() -> str:
    tz = getattr(core, "_TZ_MSK", None)
    current = datetime.now(tz) if tz is not None else datetime.now()
    return current.replace(tzinfo=None, microsecond=0).isoformat()


def _created_at(value: str) -> str:
    try:
        dt = core._parse_odata_datetime(str(value or ""))
        if dt is not None:
            return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    return str(value or "").replace("T", " ")[:19]


def _fetch_created(ref_key: str) -> dict:
    response = requests.get(
        f"{_base()}/{core.ENTITY}(guid'{ref_key}')",
        headers=core._build_headers(),
        params={"$select": "Ref_Key,Number,Date,Статус,Клиент_Key,Контрагент_Key,Комментарий"},
        timeout=30,
    )
    if response.status_code != 200:
        raise RuntimeError(f"1C created KP read HTTP {response.status_code}")
    data = response.json() if response.content else {}
    return data if isinstance(data, dict) else {}


def _inject_created(data: dict) -> dict:
    ref_key = str(data.get("Ref_Key") or data.get("refKey") or "").strip()
    number_raw = str(data.get("Number") or data.get("number") or "").strip()
    number = _norm_number(number_raw)
    if not ref_key or not number:
        raise RuntimeError("1C did not return Ref_Key/Number for new KP")
    row = {
        "refKey": ref_key,
        "number": number,
        "createdAt": _created_at(data.get("Date") or ""),
        "customerName": "",
        "status": str(data.get("Статус") or ""),
        "Клиент_Key": str(data.get("Клиент_Key") or ""),
        "Контрагент_Key": str(data.get("Контрагент_Key") or ""),
        "additionalInfoFirstLine": "",
    }
    core.apply_storage_defaults(row)
    return live_rows.inject_into_core(row)


def _audit(user_id: str, role: str, row: dict) -> None:
    path = Path(os.getenv("KP_MAX_ACCESS_FILE", "/opt/kp-api/data/kp_max_access.json")).parent / "kp_max_create_audit.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts": int(time.time()),
        "userId": str(user_id),
        "role": str(role),
        "kp": str(row.get("number") or ""),
        "refKey": str(row.get("refKey") or ""),
    }
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass


def create_empty(user_id: str, role: str) -> dict:
    now = time.time()
    with _CREATE_LOCK:
        recent = _RECENT.get(str(user_id))
        if recent and now - float(recent.get("ts") or 0) < DUPLICATE_GUARD_SECONDS:
            row = dict(recent.get("row") or {})
            if row:
                return row

        payload = {
            "Date": _now_1c(),
            "DeletionMark": False,
            "Posted": False,
            "Клиент_Key": ZERO_GUID,
            "Контрагент_Key": ZERO_GUID,
            "ЦенаВключаетНДС": True,
            "Комментарий": "",
            "Товары": [],
        }
        headers = {**core._build_headers(), "Content-Type": "application/json; charset=utf-8"}
        response = requests.post(
            f"{_base()}/{core.ENTITY}",
            headers=headers,
            json=payload,
            timeout=35,
        )
        if response.status_code not in (200, 201):
            raise RuntimeError(f"1C create KP HTTP {response.status_code}: {response.text[:300]}")
        created = response.json() if response.content else {}
        if not isinstance(created, dict):
            created = {}
        ref_key = str(created.get("Ref_Key") or "").strip()
        if ref_key and not str(created.get("Number") or "").strip():
            created = _fetch_created(ref_key)
        row = _inject_created(created)
        _RECENT[str(user_id)] = {"ts": time.time(), "row": dict(row)}
        _audit(user_id, role, row)
        return dict(row)


def create_and_menu(user_id: str, role: str) -> tuple[dict, dict]:
    row = create_empty(user_id, role)
    number = str(row.get("number") or "")
    menu = nav.kp_level3(number, 0, 0)
    menu["text"] = f"НОВОЕ КП СОЗДАНО\n\n" + menu["text"]
    return menu, row
