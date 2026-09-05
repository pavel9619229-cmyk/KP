import json
import os
import re
import time
from pathlib import Path
from threading import Lock

import requests

import api_proxy as core
import kp_max_live_rows as live_rows
import kp_max_navigation as nav

SESSION_TTL_SECONDS = 30 * 60
MAX_RESULTS = 12
ZERO_GUID = "00000000-0000-0000-0000-000000000000"
_LOCK = Lock()
_SESSIONS: dict[str, dict] = {}
_PARTNER_CACHE: list[dict] = []
_PARTNER_CACHE_AT = 0
_PARTNER_CACHE_TTL = 10 * 60
_COUNTERPARTY_CACHE: list[dict] = []
_COUNTERPARTY_CACHE_AT = 0
_COUNTERPARTY_CACHE_TTL = 10 * 60


def _base() -> str:
    return str(core.BASE).strip().strip('"').strip("'").rstrip("/")


def _norm(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().casefold().replace("ё", "е")


def _escape(value: str) -> str:
    return str(value or "").replace("'", "''")


def _cb(text: str, payload: str) -> dict:
    return {"type": "callback", "text": str(text), "payload": str(payload)}


def _keyboard(rows: list[list[dict]]) -> list[dict]:
    return [{"type": "inline_keyboard", "payload": {"buttons": rows}}]


def _find_kp(number: str) -> tuple[dict, str]:
    normalized = core._normalize_kp_number(number)
    sources = []
    try:
        sources.append(live_rows.load())
    except Exception:
        pass
    sources.append(list(core._cached_rows))
    for rows in sources:
        for row in rows:
            row_number = core._normalize_kp_number(row.get("number") or "")
            if row_number == normalized:
                ref_key = str(row.get("refKey") or row.get("Ref_Key") or "").strip()
                if ref_key:
                    return dict(row), ref_key
    try:
        for row in live_rows.load(force=True):
            row_number = core._normalize_kp_number(row.get("number") or "")
            if row_number == core._normalize_kp_number(number):
                ref = str(row.get("refKey") or row.get("Ref_Key") or "").strip()
                if ref:
                    return dict(row), ref
    except Exception:
        pass
    raise RuntimeError(f"КП {number} не найдено")


def _fetch_kp_keys(ref_key: str) -> dict:
    response = requests.get(
        f"{_base()}/{core.ENTITY}(guid'{ref_key}')",
        headers=core._build_headers(),
        timeout=30,
    )
    if response.status_code != 200:
        raise RuntimeError(f"1C KP read HTTP {response.status_code}")
    data = response.json()
    return data if isinstance(data, dict) else {}


def session_get(user_id: str) -> dict | None:
    now = int(time.time())
    with _LOCK:
        session = _SESSIONS.get(str(user_id))
        if not session:
            return None
        if int(session.get("expiresAt") or 0) <= now:
            _SESSIONS.pop(str(user_id), None)
            return None
        return dict(session)


def clear(user_id: str) -> None:
    with _LOCK:
        _SESSIONS.pop(str(user_id), None)


def _touch(user_id: str, **updates) -> dict:
    with _LOCK:
        session = _SESSIONS.get(str(user_id))
        if not session:
            raise RuntimeError("customer session expired")
        session.update(updates)
        session["expiresAt"] = int(time.time()) + SESSION_TTL_SECONDS
        return dict(session)


def start(user_id: str, number: str, status_idx: int, page: int) -> dict:
    row, ref_key = _find_kp(number)
    doc = _fetch_kp_keys(ref_key)
    with _LOCK:
        _SESSIONS[str(user_id)] = {
            "number": str(number),
            "refKey": ref_key,
            "statusIdx": int(status_idx),
            "page": max(0, int(page)),
            "originalClientKey": str(doc.get("Клиент_Key") or ZERO_GUID),
            "originalCounterpartyKey": str(doc.get("Контрагент_Key") or ZERO_GUID),
            "originalName": str(row.get("customerName") or "").strip(),
            "stage": "await_query",
            "expiresAt": int(time.time()) + SESSION_TTL_SECONDS,
        }
    return prompt_menu(user_id)


def prompt_menu(user_id: str) -> dict:
    session = session_get(user_id)
    if not session:
        raise RuntimeError("customer session expired")
    current = str(session.get("originalName") or "—")
    text = (
        f"КЛИЕНТ — КП {session['number']}\n"
        f"Текущий клиент: {current}\n\n"
        "Введи часть названия клиента и отправь сообщением."
    )
    return {
        "text": text,
        "attachments": _keyboard([[_cb("⬅ ОТМЕНА И ВЕРНУТЬСЯ К КП", "cust:cancel")]]),
    }


def again(user_id: str) -> dict:
    _touch(
        user_id,
        stage="await_query",
        selectedPartnerKey="",
        selectedPartnerName="",
        selectedCounterpartyKey="",
        selectedCounterpartyName="",
    )
    return prompt_menu(user_id)


def _get_rows(entity: str, params: dict, timeout: int = 30) -> list[dict]:
    response = requests.get(
        f"{_base()}/{entity}",
        headers=core._build_headers(),
        params=params,
        timeout=timeout,
    )
    if response.status_code != 200:
        raise RuntimeError(f"1C {entity} HTTP {response.status_code}")
    payload = response.json()
    rows = payload.get("value", []) if isinstance(payload, dict) else []
    return [row for row in rows if isinstance(row, dict)]


def _partner_sort_key(item: dict, query_norm: str) -> tuple[int, int, str]:
    name = str(item.get("Description") or "").strip()
    norm = _norm(name)
    if norm.startswith(query_norm):
        rank = 0
    elif any(part.startswith(query_norm) for part in norm.split()):
        rank = 1
    else:
        rank = 2
    return rank, len(name), norm


def _load_partner_cache() -> list[dict]:
    global _PARTNER_CACHE, _PARTNER_CACHE_AT
    now = int(time.time())
    if _PARTNER_CACHE and now - _PARTNER_CACHE_AT < _PARTNER_CACHE_TTL:
        return list(_PARTNER_CACHE)
    collected: list[dict] = []
    for skip in range(0, 10000, 250):
        rows = _get_rows(
            "Catalog_Партнеры",
            {"$select": "Ref_Key,Description,DeletionMark", "$top": "250", "$skip": str(skip)},
            timeout=30,
        )
        if not rows:
            break
        collected.extend(row for row in rows if not bool(row.get("DeletionMark")))
        if len(rows) < 250:
            break
    _PARTNER_CACHE = collected
    _PARTNER_CACHE_AT = now
    return list(collected)


def _search_partners(query: str) -> list[dict]:
    query_norm = _norm(query)
    if len(query_norm) < 2:
        return []
    rows: list[dict] = []
    try:
        rows = _get_rows(
            "Catalog_Партнеры",
            {
                "$select": "Ref_Key,Description,DeletionMark",
                "$filter": f"substringof('{_escape(query)}',Description) eq true",
                "$top": "40",
            },
            timeout=20,
        )
    except Exception:
        rows = []
    matched = [
        row for row in rows
        if not bool(row.get("DeletionMark")) and query_norm in _norm(row.get("Description") or "")
    ]
    if not matched:
        matched = [row for row in _load_partner_cache() if query_norm in _norm(row.get("Description") or "")]
    unique: dict[str, dict] = {}
    for row in matched:
        key = str(row.get("Ref_Key") or "").strip()
        name = str(row.get("Description") or "").strip()
        if key and name:
            unique[key] = {"Ref_Key": key, "Description": name}
    result = list(unique.values())
    result.sort(key=lambda item: _partner_sort_key(item, query_norm))
    return result[:MAX_RESULTS]


def _load_counterparty_cache() -> list[dict]:
    global _COUNTERPARTY_CACHE, _COUNTERPARTY_CACHE_AT
    now = int(time.time())
    if _COUNTERPARTY_CACHE and now - _COUNTERPARTY_CACHE_AT < _COUNTERPARTY_CACHE_TTL:
        return list(_COUNTERPARTY_CACHE)
    collected: list[dict] = []
    for skip in range(0, 30000, 250):
        rows = _get_rows(
            "Catalog_Контрагенты",
            {"$select": "Ref_Key,Description,Партнер_Key,DeletionMark", "$top": "250", "$skip": str(skip)},
            timeout=30,
        )
        if not rows:
            break
        collected.extend(row for row in rows if not bool(row.get("DeletionMark")))
        if len(rows) < 250:
            break
    _COUNTERPARTY_CACHE = collected
    _COUNTERPARTY_CACHE_AT = now
    return list(collected)


def _search_counterparties(query: str) -> list[dict]:
    query_norm = _norm(query)
    if len(query_norm) < 2:
        return []
    matched: list[dict] = []
    try:
        for skip in range(0, 400, 50):
            rows = _get_rows(
                "Catalog_Контрагенты",
                {
                    "$select": "Ref_Key,Description,Партнер_Key,DeletionMark",
                    "$filter": f"substringof('{_escape(query)}',Description) eq true",
                    "$top": "50",
                    "$skip": str(skip),
                },
                timeout=20,
            )
            matched.extend(
                row for row in rows
                if not bool(row.get("DeletionMark")) and query_norm in _norm(row.get("Description") or "")
            )
            if len(rows) < 50:
                break
    except Exception:
        matched = []
    if not matched:
        matched = [row for row in _load_counterparty_cache() if query_norm in _norm(row.get("Description") or "")]
    unique: dict[str, dict] = {}
    for row in matched:
        key = str(row.get("Ref_Key") or "").strip()
        name = str(row.get("Description") or "").strip()
        partner_key = str(row.get("Партнер_Key") or "").strip()
        if key and name:
            unique[key] = {"Ref_Key": key, "Description": name, "Партнер_Key": partner_key}
    result = list(unique.values())
    result.sort(key=lambda item: _partner_sort_key(item, query_norm))
    return result[:MAX_RESULTS]


def _counterparty_by_key(counterparty_key: str) -> dict:
    response = requests.get(
        f"{_base()}/Catalog_Контрагенты(guid'{counterparty_key}')",
        headers=core._build_headers(),
        params={"$select": "Ref_Key,Description,Партнер_Key,DeletionMark"},
        timeout=20,
    )
    if response.status_code != 200:
        raise RuntimeError(f"1C counterparty HTTP {response.status_code}")
    item = response.json()
    if not isinstance(item, dict) or bool(item.get("DeletionMark")):
        raise RuntimeError("counterparty is unavailable")
    return item


def search_menu(user_id: str, query: str) -> dict:
    session = session_get(user_id)
    if not session or session.get("stage") != "await_query":
        raise RuntimeError("customer search is not active")
    if len(_norm(query)) < 2:
        menu = prompt_menu(user_id)
        menu["text"] += "\n\nВведи минимум 2 символа."
        return menu
    counterparties = _search_counterparties(query)
    partners = _search_partners(query)
    _touch(user_id, lastQuery=str(query))
    buttons: list[list[dict]] = []
    seen_names: set[str] = set()
    for item in counterparties:
        key = str(item.get("Ref_Key") or "").strip()
        name = str(item.get("Description") or "").strip()
        if not key or not name:
            continue
        buttons.append([_cb(name[:100], f"cust:x:{key}")])
        seen_names.add(_norm(name))
        if len(buttons) >= MAX_RESULTS:
            break
    if len(buttons) < MAX_RESULTS:
        for item in partners:
            key = str(item.get("Ref_Key") or "").strip()
            name = str(item.get("Description") or "").strip()
            if not key or not name or _norm(name) in seen_names:
                continue
            buttons.append([_cb(name[:100], f"cust:p:{key}")])
            seen_names.add(_norm(name))
            if len(buttons) >= MAX_RESULTS:
                break
    if not buttons:
        return {
            "text": f"По запросу «{query}» клиенты не найдены. Введи другой фрагмент названия.",
            "attachments": _keyboard([[_cb("⬅ ОТМЕНА И ВЕРНУТЬСЯ К КП", "cust:cancel")]]),
        }
    buttons.append([_cb("🔎 ИСКАТЬ ДРУГОГО", "cust:again")])
    buttons.append([_cb("⬅ ОТМЕНА И ВЕРНУТЬСЯ К КП", "cust:cancel")])
    return {
        "text": f"Найдено вариантов: {len(buttons) - 2}. Выбери клиента:",
        "attachments": _keyboard(buttons),
    }


def _partner_by_key(partner_key: str) -> dict:
    response = requests.get(
        f"{_base()}/Catalog_Партнеры(guid'{partner_key}')",
        headers=core._build_headers(),
        params={"$select": "Ref_Key,Description,DeletionMark"},
        timeout=20,
    )
    if response.status_code != 200:
        raise RuntimeError(f"1C partner HTTP {response.status_code}")
    item = response.json()
    if not isinstance(item, dict) or bool(item.get("DeletionMark")):
        raise RuntimeError("partner is unavailable")
    return item


def _counterparties_for_partner(partner_key: str) -> list[dict]:
    try:
        rows = _get_rows(
            "Catalog_Контрагенты",
            {
                "$select": "Ref_Key,Description,Партнер_Key,DeletionMark",
                "$filter": f"Партнер_Key eq guid'{partner_key}'",
                "$top": "50",
            },
            timeout=25,
        )
        return [row for row in rows if not bool(row.get("DeletionMark"))]
    except Exception:
        result: list[dict] = []
        for skip in range(0, 10000, 250):
            rows = _get_rows(
                "Catalog_Контрагенты",
                {"$select": "Ref_Key,Description,Партнер_Key,DeletionMark", "$top": "250", "$skip": str(skip)},
                timeout=30,
            )
            if not rows:
                break
            result.extend(row for row in rows if not bool(row.get("DeletionMark")) and str(row.get("Партнер_Key") or "") == partner_key)
            if len(rows) < 250:
                break
        return result


def _stage_confirm(user_id: str, partner: dict, counterparty: dict) -> dict:
    partner_key = str(partner.get("Ref_Key") or "").strip()
    partner_name = str(partner.get("Description") or "").strip()
    counterparty_key = str(counterparty.get("Ref_Key") or "").strip()
    counterparty_name = str(counterparty.get("Description") or "").strip()
    session = _touch(
        user_id,
        stage="confirm",
        selectedPartnerKey=partner_key,
        selectedPartnerName=partner_name,
        selectedCounterpartyKey=counterparty_key,
        selectedCounterpartyName=counterparty_name,
    )
    old_name = str(session.get("originalName") or "—")
    text = (
        f"КП {session['number']}\n"
        f"Старый клиент: {old_name}\n"
        f"Новый клиент: {counterparty_name or partner_name}\n\n"
        "Проверь выбор и нажми СОХРАНИТЬ."
    )
    return {"text": text, "attachments": _keyboard([
        [_cb("СОХРАНИТЬ", "cust:save")],
        [_cb("🔎 ВЫБРАТЬ ДРУГОГО", "cust:again")],
        [_cb("ОТМЕНА", "cust:cancel")],
    ])}


def pick_partner(user_id: str, partner_key: str) -> dict:
    session = session_get(user_id)
    if not session:
        raise RuntimeError("customer session expired")
    partner = _partner_by_key(partner_key)
    counterparties = _counterparties_for_partner(partner_key)
    if not counterparties:
        _touch(user_id, stage="await_query")
        return {
            "text": "У выбранного клиента не найден связанный контрагент. Выбери другого клиента или уточни поиск.",
            "attachments": _keyboard([
                [_cb("🔎 ИСКАТЬ ДРУГОГО", "cust:again")],
                [_cb("ОТМЕНА", "cust:cancel")],
            ]),
        }
    if len(counterparties) == 1:
        return _stage_confirm(user_id, partner, counterparties[0])
    same_name = [cp for cp in counterparties if _norm(cp.get("Description") or "") == _norm(partner.get("Description") or "")]
    if len(same_name) == 1:
        return _stage_confirm(user_id, partner, same_name[0])
    _touch(
        user_id,
        stage="await_counterparty",
        selectedPartnerKey=str(partner.get("Ref_Key") or ""),
        selectedPartnerName=str(partner.get("Description") or ""),
    )
    buttons = []
    for cp in counterparties[:20]:
        cp_key = str(cp.get("Ref_Key") or "").strip()
        cp_name = str(cp.get("Description") or "—").strip()
        if cp_key:
            buttons.append([_cb(cp_name[:100], f"cust:c:{cp_key}")])
    buttons.append([_cb("🔎 ВЫБРАТЬ ДРУГОГО КЛИЕНТА", "cust:again")])
    buttons.append([_cb("ОТМЕНА", "cust:cancel")])
    return {
        "text": (
            f"У клиента «{partner.get('Description') or '—'}» несколько контрагентов.\n"
            "Выбери нужного контрагента:"
        ),
        "attachments": _keyboard(buttons),
    }


def pick_counterparty_direct(user_id: str, counterparty_key: str) -> dict:
    session = session_get(user_id)
    if not session:
        raise RuntimeError("customer session expired")
    counterparty = _counterparty_by_key(counterparty_key)
    partner_key = str(counterparty.get("Партнер_Key") or "").strip()
    if not partner_key or partner_key == ZERO_GUID:
        _touch(user_id, stage="await_query")
        return {
            "text": "У выбранного контрагента не указан связанный клиент (партнер) в 1С. Выбери другой вариант.",
            "attachments": _keyboard([
                [_cb("🔎 ИСКАТЬ ДРУГОГО", "cust:again")],
                [_cb("ОТМЕНА", "cust:cancel")],
            ]),
        }
    partner = _partner_by_key(partner_key)
    return _stage_confirm(user_id, partner, counterparty)


def pick_counterparty(user_id: str, counterparty_key: str) -> dict:
    session = session_get(user_id)
    if not session or session.get("stage") != "await_counterparty":
        raise RuntimeError("counterparty selection is not active")
    partner_key = str(session.get("selectedPartnerKey") or "")
    partner = _partner_by_key(partner_key)
    rows = _counterparties_for_partner(partner_key)
    selected = next(
        (row for row in rows if str(row.get("Ref_Key") or "").strip() == str(counterparty_key).strip()),
        None,
    )
    if not selected:
        raise RuntimeError("counterparty does not belong to selected partner")
    return _stage_confirm(user_id, partner, selected)


def cancel_menu(user_id: str) -> dict:
    session = session_get(user_id)
    if not session:
        return nav.root_menu("user")
    number = str(session.get("number") or "")
    status_idx = int(session.get("statusIdx") or 0)
    page = int(session.get("page") or 0)
    clear(user_id)
    return nav.kp_level3(number, status_idx, page)


def _audit(user_id: str, role: str, session: dict) -> None:
    path = Path(os.getenv("KP_MAX_ACCESS_FILE", "/opt/kp-api/data/kp_max_access.json")).parent / "kp_max_customer_audit.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts": int(time.time()),
        "userId": str(user_id),
        "role": str(role),
        "kp": str(session.get("number") or ""),
        "oldClientKey": str(session.get("originalClientKey") or ""),
        "oldCounterpartyKey": str(session.get("originalCounterpartyKey") or ""),
        "newClientKey": str(session.get("selectedPartnerKey") or ""),
        "newCounterpartyKey": str(session.get("selectedCounterpartyKey") or ""),
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def commit(user_id: str, role: str) -> tuple[dict, dict]:
    session = session_get(user_id)
    if not session or session.get("stage") != "confirm":
        raise RuntimeError("no customer change awaiting confirmation")
    ref_key = str(session.get("refKey") or "")
    current = _fetch_kp_keys(ref_key)
    if str(current.get("Клиент_Key") or ZERO_GUID) != str(session.get("originalClientKey") or ZERO_GUID):
        clear(user_id)
        raise RuntimeError("customer changed concurrently")
    if str(current.get("Контрагент_Key") or ZERO_GUID) != str(session.get("originalCounterpartyKey") or ZERO_GUID):
        clear(user_id)
        raise RuntimeError("customer changed concurrently")
    partner_key = str(session.get("selectedPartnerKey") or "")
    counterparty_key = str(session.get("selectedCounterpartyKey") or "")
    response = requests.patch(
        f"{_base()}/{core.ENTITY}(guid'{ref_key}')",
        headers={**core._build_headers(), "Content-Type": "application/json; charset=utf-8"},
        json={"Клиент_Key": partner_key, "Контрагент_Key": counterparty_key},
        timeout=30,
    )
    if response.status_code not in {200, 204}:
        raise RuntimeError(f"1C customer PATCH HTTP {response.status_code}")
    verified = _fetch_kp_keys(ref_key)
    if str(verified.get("Клиент_Key") or ZERO_GUID) != partner_key:
        raise RuntimeError("1C customer write verification failed")
    if str(verified.get("Контрагент_Key") or ZERO_GUID) != counterparty_key:
        raise RuntimeError("1C counterparty write verification failed")
    row, _ = _find_kp(str(session.get("number") or ""))
    row["customerName"] = str(session.get("selectedPartnerName") or "").strip()
    row["clientFilled"] = bool(row["customerName"])
    live_rows.inject_into_core(row)
    _audit(user_id, role, session)
    number = str(session.get("number") or "")
    status_idx = int(session.get("statusIdx") or 0)
    page = int(session.get("page") or 0)
    name = str(session.get("selectedPartnerName") or "").strip()
    clear(user_id)
    menu = nav.kp_level3(number, status_idx, page)
    menu["text"] = f"Клиент сохранён в 1С.\n\n{menu['text']}"
    return menu, {"number": number, "name": name}


def confirm_menu(user_id: str) -> dict:
    session = session_get(user_id)
    if not session or session.get("stage") != "confirm":
        raise RuntimeError("no customer change awaiting confirmation")
    old_name = str(session.get("originalName") or "—")
    new_name = str(session.get("selectedPartnerName") or "—")
    return {
        "text": (
            f"КП {session['number']}\n"
            f"Старый клиент: {old_name}\n"
            f"Новый клиент: {new_name}\n\n"
            "Проверь выбор и нажми СОХРАНИТЬ."
        ),
        "attachments": _keyboard([
            [_cb("СОХРАНИТЬ", "cust:save")],
            [_cb("🔎 ВЫБРАТЬ ДРУГОГО", "cust:again")],
            [_cb("ОТМЕНА", "cust:cancel")],
        ]),
    }
