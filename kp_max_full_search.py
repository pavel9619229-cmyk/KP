import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock, Thread

import requests

import api_proxy as core
import kp_max_live_rows as live_rows

INDEX_TTL_SECONDS = 24 * 60 * 60
REQUEST_TOP = 500
WORKERS = 6
_INDEX_LOCK = Lock()
_INDEX_ROWS: list[dict] = []
_INDEX_AT = 0.0
_INDEX_REFRESHING = False
_INDEX_FILE = Path("/opt/kp-api/data/kp_full_client_search_index.json")


def _base() -> str:
    return str(core.BASE).strip().strip(chr(34)).strip(chr(39)).rstrip("/")


def _norm(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().casefold().replace("ё", "е")


def _number(value) -> str:
    raw = str(value or "").strip()
    match = re.search(r"(\d+)$", raw)
    if match:
        return str(int(match.group(1)))
    stripped = raw.lstrip("0")
    return stripped or "0"


def _get(entity: str, params: dict | None = None, timeout: int = 60) -> requests.Response:
    response = requests.get(
        f"{_base()}/{entity}",
        headers=core._build_headers(),
        params=params,
        timeout=timeout,
    )
    if response.status_code != 200:
        raise RuntimeError(f"1C {entity} HTTP {response.status_code}")
    return response


def _load_file() -> tuple[list[dict], float]:
    try:
        data = json.loads(_INDEX_FILE.read_text(encoding="utf-8"))
        rows = data.get("rows", []) if isinstance(data, dict) else []
        stamp = float(data.get("updatedAt") or 0) if isinstance(data, dict) else 0.0
        return [x for x in rows if isinstance(x, dict)], stamp
    except Exception:
        return [], 0.0


def _save_file(rows: list[dict], stamp: float) -> None:
    try:
        _INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
        temp = _INDEX_FILE.with_suffix(".tmp")
        temp.write_text(
            json.dumps({"updatedAt": stamp, "rows": rows}, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        temp.replace(_INDEX_FILE)
    except Exception as exc:
        core.log(f"KP full search index save failed: {type(exc).__name__}: {exc}")


def _fetch_page(skip: int, top: int) -> list[dict]:
    select = "Ref_Key,Number,Date,Статус,СуммаДокумента,Клиент_Key,Контрагент_Key,Менеджер_Key,Комментарий"
    response = _get(core.ENTITY, {
        "$select": select,
        "$top": str(top),
        "$skip": str(skip),
    })
    payload = response.json() if response.content else {}
    rows = payload.get("value", []) if isinstance(payload, dict) else []
    return [x for x in rows if isinstance(x, dict)]


def _build_index() -> list[dict]:
    count_response = _get(f"{core.ENTITY}/$count")
    total = int(count_response.text.strip())
    if total <= 0:
        return []
    skips = list(range(0, total, REQUEST_TOP))
    pages: dict[int, list[dict]] = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(_fetch_page, skip, min(REQUEST_TOP, total - skip)): skip for skip in skips}
        for future in as_completed(futures):
            skip = futures[future]
            pages[skip] = future.result()
    rows: list[dict] = []
    for skip in sorted(pages):
        for item in pages[skip]:
            dt = core._parse_odata_datetime(str(item.get("Date") or ""))
            rows.append({
                "refKey": str(item.get("Ref_Key") or ""),
                "number": _number(item.get("Number")),
                "createdAt": dt.strftime("%Y-%m-%d %H:%M:%S") if dt else str(item.get("Date") or ""),
                "status": str(item.get("Статус") or ""),
                "Клиент_Key": str(item.get("Клиент_Key") or ""),
                "Контрагент_Key": str(item.get("Контрагент_Key") or ""),
                "Менеджер_Key": str(item.get("Менеджер_Key") or ""),
                "sum": float(item.get("СуммаДокумента") or 0),
            })
    rows.sort(key=lambda x: str(x.get("createdAt") or ""), reverse=True)
    return rows


def rebuild() -> int:
    global _INDEX_ROWS, _INDEX_AT
    rows = _build_index()
    stamp = time.time()
    with _INDEX_LOCK:
        _INDEX_ROWS = rows
        _INDEX_AT = stamp
    if rows:
        _save_file(rows, stamp)
    return len(rows)


def _refresh_worker() -> None:
    global _INDEX_REFRESHING
    try:
        rebuild()
    except Exception as exc:
        core.log(f"KP full search refresh failed: {type(exc).__name__}: {exc}")
    finally:
        with _INDEX_LOCK:
            _INDEX_REFRESHING = False


def refresh_async(force: bool = False) -> bool:
    global _INDEX_REFRESHING, _INDEX_ROWS, _INDEX_AT
    with _INDEX_LOCK:
        if _INDEX_REFRESHING:
            return False
        if not force and _INDEX_ROWS and time.time() - _INDEX_AT < INDEX_TTL_SECONDS:
            return False
        _INDEX_REFRESHING = True
    Thread(target=_refresh_worker, name="kp-full-search-refresh", daemon=True).start()
    return True


def load() -> list[dict]:
    global _INDEX_ROWS, _INDEX_AT
    with _INDEX_LOCK:
        if _INDEX_ROWS:
            rows = [dict(x) for x in _INDEX_ROWS]
            fresh = time.time() - _INDEX_AT < INDEX_TTL_SECONDS
        else:
            rows = []
            fresh = False
    if not rows:
        disk, stamp = _load_file()
        if disk:
            with _INDEX_LOCK:
                _INDEX_ROWS = disk
                _INDEX_AT = stamp or time.time()
            rows = [dict(x) for x in disk]
            fresh = bool(stamp and time.time() - stamp < INDEX_TTL_SECONDS)
    if rows:
        if not fresh:
            refresh_async()
        return rows
    return []


def _escape(value: str) -> str:
    return str(value or "").replace("'", "''")


def _catalog_matches(entity: str, query: str) -> list[dict]:
    q = _norm(query)
    tokens = [x for x in re.findall(r"[0-9a-zа-я]+", q) if len(x) >= 2 or x.isdigit()]
    if not tokens:
        return []
    by_key: dict[str, dict] = {}
    for token in tokens:
        response = _get(entity, {
            "$select": "Ref_Key,Description,Партнер_Key,DeletionMark",
            "$filter": f"substringof('{_escape(token)}',Description) eq true",
            "$top": "500",
        }, timeout=30)
        payload = response.json() if response.content else {}
        rows = payload.get("value", []) if isinstance(payload, dict) else []
        if not by_key:
            for row in rows:
                if isinstance(row, dict) and not bool(row.get("DeletionMark")):
                    key = str(row.get("Ref_Key") or "").strip()
                    if key:
                        by_key[key] = row
        else:
            allowed = {str(row.get("Ref_Key") or "").strip() for row in rows if isinstance(row, dict)}
            by_key = {k: v for k, v in by_key.items() if k in allowed}
    return [x for x in by_key.values() if all(t in _norm(x.get("Description") or "") for t in tokens)]


def _tokens(query: str) -> list[str]:
    return [x for x in re.findall(r"[0-9a-zа-я]+", _norm(query)) if len(x) >= 2 or x.isdigit()]


def _decorate_comment(row: dict, raw_comment: str) -> dict:
    result = dict(row)
    clean = core.strip_html(str(raw_comment or "")).replace("\r\n", "\n").replace("\r", "\n")
    upper = clean.upper()
    top = upper.split("\n")[:5]
    result["additionalInfoFirstLine"] = core.first_line(raw_comment) or ""
    result["kpSent"] = any("КП ОТПРАВЛЕНО" in line for line in top)
    result["receiptConfirmed"] = any("КЛИЕНТ КП УВИДЕЛ" in line for line in top)
    result["edoSent"] = "В ЭДО ОТПРАВЛЕНО" in upper
    result["rejected"] = "ОТКАЗ" in upper
    result["problem"] = "ПРОБЛЕМА" in upper
    result["shipmentPending"] = "ОТГРУЗИТЬ" in upper
    if any("ОПЛАТА ПРИШЛА" in line for line in top):
        result["paymentReceived"] = True
    return result


def search_client(query: str) -> list[dict]:
    tokens = _tokens(query)
    if not tokens:
        return []
    matches = _catalog_matches("Catalog_Контрагенты", query)
    cp_names = {str(x.get("Ref_Key") or ""): str(x.get("Description") or "").strip() for x in matches}
    partner_names = {str(x.get("Партнер_Key") or ""): str(x.get("Description") or "").strip() for x in matches if x.get("Партнер_Key")}
    found: dict[str, dict] = {}
    for row in load():
        cp_key = str(row.get("Контрагент_Key") or "")
        partner_key = str(row.get("Клиент_Key") or "")
        name = cp_names.get(cp_key) or partner_names.get(partner_key) or ""
        if not name:
            continue
        item = dict(row)
        item["customerName"] = name
        found[str(item.get("refKey") or item.get("number") or "")] = item
    try:
        for row in live_rows.load():
            name = str(row.get("customerName") or "").strip()
            if not name or not all(token in _norm(name) for token in tokens):
                continue
            item = dict(row)
            found[str(item.get("refKey") or item.get("number") or "")] = item
    except Exception:
        pass
    rows = list(found.values())
    rows.sort(key=lambda x: str(x.get("createdAt") or ""), reverse=True)
    return rows


def find_number(number: str) -> dict | None:
    wanted = _number(number)
    try:
        for row in live_rows.load():
            if _number(row.get("number")) == wanted:
                return dict(row)
    except Exception:
        pass
    for row in load():
        if _number(row.get("number")) == wanted:
            return dict(row)
    return None


def enrich_row(row: dict) -> dict:
    ref_key = str(row.get("refKey") or "").strip()
    if not ref_key:
        return dict(row)
    response = _get(
        f"{core.ENTITY}(guid'{ref_key}')",
        {"$select": "Ref_Key,Number,Date,Статус,СуммаДокумента,Клиент_Key,Контрагент_Key,Менеджер_Key,Комментарий"},
        timeout=30,
    )
    data = response.json() if response.content else {}
    if not isinstance(data, dict):
        return dict(row)
    result = dict(row)
    result.update({
        "refKey": str(data.get("Ref_Key") or ref_key),
        "number": _number(data.get("Number") or row.get("number")),
        "status": str(data.get("Статус") or row.get("status") or ""),
        "Клиент_Key": str(data.get("Клиент_Key") or row.get("Клиент_Key") or ""),
        "Контрагент_Key": str(data.get("Контрагент_Key") or row.get("Контрагент_Key") or ""),
        "Менеджер_Key": str(data.get("Менеджер_Key") or row.get("Менеджер_Key") or ""),
    })
    return _decorate_comment(result, str(data.get("Комментарий") or ""))
