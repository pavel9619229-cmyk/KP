import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import requests

import api_proxy as core

TTL_SECONDS = 120
MAX_ROWS = 300
PAGE_SIZE = 50
_LOCK = Lock()
_ROWS: list[dict] = []
_ROWS_AT = 0.0


def _base() -> str:
    return str(core.BASE).strip().strip(chr(34)).strip(chr(39)).rstrip("/")


def _number(value) -> str:
    raw = str(value or "").strip()
    match = re.search(r"(\d+)$", raw)
    return str(int(match.group(1))) if match else raw.lstrip("0")


def _get(url: str, *, params: dict | None = None, timeout: int = 30):
    response = requests.get(
        url,
        headers=core._build_headers(),
        params=params,
        timeout=timeout,
    )
    response.raise_for_status()
    return response


def _latest_base_rows() -> list[dict]:
    count_resp = _get(f"{_base()}/{core.ENTITY}/$count", timeout=60)
    total = int(count_resp.text.strip())
    start = max(0, total - MAX_ROWS)
    collected: list[dict] = []
    select = "Ref_Key,Number,Date,Статус,СуммаДокумента,Клиент_Key,Контрагент_Key,Комментарий"
    for skip in range(start, total, PAGE_SIZE):
        top = min(PAGE_SIZE, total - skip)
        resp = _get(
            f"{_base()}/{core.ENTITY}",
            params={"$select": select, "$top": str(top), "$skip": str(skip)},
            timeout=60,
        )
        payload = resp.json()
        batch = payload.get("value", []) if isinstance(payload, dict) else []
        collected.extend(item for item in batch if isinstance(item, dict))
    collected.sort(
        key=lambda item: str(item.get("Date") or ""),
        reverse=True,
    )
    return collected[:MAX_ROWS]


def _known_maps() -> tuple[dict[str, dict], dict[str, dict]]:
    by_ref: dict[str, dict] = {}
    by_number: dict[str, dict] = {}
    for row in list(core._cached_rows):
        if not isinstance(row, dict):
            continue
        ref_key = str(row.get("refKey") or row.get("Ref_Key") or "").strip()
        number = _number(row.get("number"))
        if ref_key:
            by_ref[ref_key] = row
        if number:
            by_number[number] = row
    return by_ref, by_number


def _lookup_name(entity: str, ref_key: str) -> str:
    if not ref_key or ref_key == "00000000-0000-0000-0000-000000000000":
        return ""
    try:
        resp = _get(
            f"{_base()}/{entity}(guid'{ref_key}')",
            params={"$select": "Description"},
            timeout=20,
        )
        payload = resp.json()
        return str(payload.get("Description") or "").strip() if isinstance(payload, dict) else ""
    except Exception:
        return ""


def _resolve_customer_name(item: dict, known: dict | None) -> str:
    if known:
        existing = str(known.get("customerName") or "").strip()
        if existing:
            return existing
    cp_key = str(item.get("Контрагент_Key") or "").strip()
    partner_key = str(item.get("Клиент_Key") or "").strip()
    name = _lookup_name("Catalog_Контрагенты", cp_key)
    return name or _lookup_name("Catalog_Партнеры", partner_key)


def _build_rows() -> list[dict]:
    base_rows = _latest_base_rows()
    known_by_ref, known_by_number = _known_maps()
    result: list[dict] = []
    unresolved: list[tuple[int, dict, dict | None]] = []
    for item in base_rows:
        ref_key = str(item.get("Ref_Key") or "").strip()
        number_raw = str(item.get("Number") or "").strip()
        number = _number(number_raw)
        known = known_by_ref.get(ref_key) or known_by_number.get(number)
        row = dict(known or {})
        dt = core._parse_odata_datetime(str(item.get("Date") or ""))
        row.update({
            "refKey": ref_key,
            "number": number,
            "createdAt": dt.strftime("%Y-%m-%d %H:%M:%S") if dt else str(item.get("Date") or ""),
            "status": str(item.get("Статус") or row.get("status") or ""),
            "Клиент_Key": str(item.get("Клиент_Key") or ""),
            "Контрагент_Key": str(item.get("Контрагент_Key") or ""),
        })
        core.apply_storage_defaults(row)
        raw_comment = str(item.get("Комментарий") or "")
        if raw_comment:
            comment_clean = core.strip_html(raw_comment).replace("\r\n", "\n").replace("\r", "\n").upper()
            comment_top = comment_clean.split("\n")[:5]
            row["additionalInfoFirstLine"] = core.first_line(raw_comment) or ""
            row["kpSent"] = any("КП ОТПРАВЛЕНО" in line for line in comment_top)
            row["receiptConfirmed"] = any("КЛИЕНТ КП УВИДЕЛ" in line for line in comment_top)
            row["edoSent"] = "В ЭДО ОТПРАВЛЕНО" in comment_clean
            row["rejected"] = "ОТКАЗ" in comment_clean
            row["problem"] = "ПРОБЛЕМА" in comment_clean
            row["shipmentPending"] = "ОТГРУЗИТЬ" in comment_clean
            if any("ОПЛАТА ПРИШЛА" in line for line in comment_top):
                row["paymentReceived"] = True
        result.append(row)
        if not str(row.get("customerName") or "").strip():
            unresolved.append((len(result) - 1, item, known))

    if unresolved:
        with ThreadPoolExecutor(max_workers=12) as pool:
            futures = {
                pool.submit(_resolve_customer_name, item, known): idx
                for idx, item, known in unresolved
            }
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    name = str(future.result() or "").strip()
                except Exception:
                    name = ""
                if name:
                    result[idx]["customerName"] = name
    result.sort(key=lambda row: str(row.get("createdAt") or ""), reverse=True)
    return result[:MAX_ROWS]


def load(force: bool = False) -> list[dict]:
    global _ROWS, _ROWS_AT
    now = time.time()
    with _LOCK:
        if not force and _ROWS and now - _ROWS_AT < TTL_SECONDS:
            return [dict(row) for row in _ROWS]
        rows = _build_rows()
        if rows:
            _ROWS = rows
            _ROWS_AT = time.time()
        return [dict(row) for row in (_ROWS or rows)]


def inject_into_core(row: dict) -> dict:
    candidate = dict(row)
    ref_key = str(candidate.get("refKey") or "").strip()
    number = _number(candidate.get("number"))
    for existing in core._cached_rows:
        if str(existing.get("refKey") or "").strip() == ref_key or _number(existing.get("number")) == number:
            existing.update({
                "refKey": candidate.get("refKey"),
                "number": candidate.get("number"),
                "createdAt": candidate.get("createdAt"),
                "customerName": candidate.get("customerName") or existing.get("customerName"),
                "Клиент_Key": candidate.get("Клиент_Key"),
                "Контрагент_Key": candidate.get("Контрагент_Key"),
            })
            return existing
    core._cached_rows.append(candidate)
    return candidate
