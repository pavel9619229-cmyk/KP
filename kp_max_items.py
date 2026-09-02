import hashlib
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

ITEM_ENTITY = "Document_КоммерческоеПредложениеКлиенту_Товары"
SESSION_TTL_SECONDS = 30 * 60
LINE_PAGE_SIZE = 12
MAX_TEXT_CHARS = 20000
MAX_PRODUCT_RESULTS = 12
_LOCK = Lock()
_SESSIONS: dict[str, dict] = {}
_NOM_CACHE: dict[str, str] = {}

FIELD_MAP = {
    "internal": ("ПОСТАВЩИК И ЦЕНА", "КомментарийВнутренний"),
    "buyer": ("КОММЕНТАРИЙ", "КомментарийДляПокупателя"),
    "product": ("ТОВАР", "Номенклатура_Key"),
    "price": ("ЦЕНА", "Цена"),
    "qty": ("КОЛИЧЕСТВО", "Количество"),
}


def _base() -> str:
    return str(core.BASE).strip().strip('"').strip("'").rstrip("/")


def _cb(text: str, payload: str) -> dict:
    return {"type": "callback", "text": str(text), "payload": str(payload)}


def _keyboard(rows: list[list[dict]]) -> list[dict]:
    return [{"type": "inline_keyboard", "payload": {"buttons": rows}}]


def _norm_number(value: str) -> str:
    m = re.search(r"(\d+)$", str(value or ""))
    return str(int(m.group(1))) if m else (str(value or "").lstrip("0") or "0")


def _fmt_num(value) -> str:
    try:
        n = float(value)
        if n.is_integer():
            return f"{int(n):,}".replace(",", " ")
        return f"{n:,.3f}".rstrip("0").rstrip(".").replace(",", " ")
    except Exception:
        return str(value or "—")


def _compact(value: str, limit: int) -> str:
    text = " ".join(str(value or "").split()) or "—"
    return text if len(text) <= limit else text[: max(1, limit - 1)].rstrip() + "…"


def _find_kp(number: str) -> tuple[dict, str]:
    wanted = _norm_number(number)
    sources = []
    try:
        sources.append(live_rows.load())
    except Exception:
        pass
    sources.append(list(core._cached_rows))
    for candidates in sources:
        for row in candidates:
            if _norm_number(row.get("number") or "") == wanted:
                ref = str(row.get("refKey") or row.get("Ref_Key") or "").strip()
                if ref:
                    return dict(row), ref
    raise RuntimeError(f"КП {number} не найдено")


def _row_url(ref_key: str, line_number: int) -> str:
    return f"{_base()}/{ITEM_ENTITY}(Ref_Key=guid'{ref_key}',LineNumber={int(line_number)})"


def _fetch_row(ref_key: str, line_number: int) -> dict:
    r = requests.get(_row_url(ref_key, line_number), headers=core._build_headers(), timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"1C item read HTTP {r.status_code}")
    data = r.json()
    return data if isinstance(data, dict) else {}


def _fetch_items(ref_key: str) -> list[dict]:
    r = requests.get(f"{_base()}/{core.ENTITY}(guid'{ref_key}')", headers=core._build_headers(), timeout=40)
    if r.status_code != 200:
        raise RuntimeError(f"1C KP read HTTP {r.status_code}")
    data = r.json() if r.content else {}
    rows = data.get("Товары", []) if isinstance(data, dict) else []
    result = [dict(x) for x in rows if isinstance(x, dict)]
    result.sort(key=lambda x: int(x.get("LineNumber") or 0))
    return result


def _nomenclature_name(key: str) -> str:
    key = str(key or "").strip()
    if not key:
        return "—"
    if key in _NOM_CACHE:
        return _NOM_CACHE[key]
    r = requests.get(
        f"{_base()}/Catalog_Номенклатура(guid'{key}')",
        headers=core._build_headers(), params={"$select": "Ref_Key,Description"}, timeout=20,
    )
    name = ""
    if r.status_code == 200:
        obj = r.json() if r.content else {}
        name = str(obj.get("Description") or "").strip() if isinstance(obj, dict) else ""
    _NOM_CACHE[key] = name or "—"
    return _NOM_CACHE[key]


def _supplier_text(row: dict) -> str:
    raw = str(row.get("КомментарийВнутренний") or "").strip()
    first = next((x.strip() for x in raw.splitlines() if x.strip()), "")
    return first or "—"


def _editable_snapshot(row: dict) -> dict:
    return {field: row.get(field) for _, field in FIELD_MAP.values()}


def _row_hash(row: dict) -> str:
    raw = json.dumps(_editable_snapshot(row), ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def session_get(user_id: str) -> dict | None:
    now = int(time.time())
    with _LOCK:
        s = _SESSIONS.get(str(user_id))
        if not s:
            return None
        if int(s.get("expiresAt") or 0) <= now:
            _SESSIONS.pop(str(user_id), None)
            return None
        return dict(s)


def _set_session(user_id: str, **values) -> dict:
    with _LOCK:
        current = dict(_SESSIONS.get(str(user_id)) or {})
        current.update(values)
        current["expiresAt"] = int(time.time()) + SESSION_TTL_SECONDS
        _SESSIONS[str(user_id)] = current
        return dict(current)


def clear(user_id: str) -> None:
    with _LOCK:
        _SESSIONS.pop(str(user_id), None)


def list_menu(number: str, status_idx: int, status_page: int, item_page: int = 0) -> dict:
    _, ref_key = _find_kp(number)
    items = _fetch_items(ref_key)
    total_pages = max(1, (len(items) + LINE_PAGE_SIZE - 1) // LINE_PAGE_SIZE)
    item_page = max(0, min(int(item_page), total_pages - 1))
    start = item_page * LINE_PAGE_SIZE
    current = items[start:start + LINE_PAGE_SIZE]
    key = nav.status_key(status_idx)
    rows = [
        [_cb("ВЕРНУТЬСЯ НА ГЛАВНОЕ МЕНЮ", "nav:root")],
        [_cb("🟢 ← ВЕРНУТЬСЯ НА УРОВЕНЬ ВЫШЕ", f"nav:k:{number}:{key}:{status_page}")],
        [_cb("ДОБАВИТЬ СТРОКУ", f"itm:addrow:{number}:{key}:{status_page}:{item_page}")],
    ]
    for item in current:
        line = int(item.get("LineNumber") or 0)
        supplier = _compact(_supplier_text(item), 22)
        product = _compact(_nomenclature_name(item.get("Номенклатура_Key")), 38)
        qty = _fmt_num(item.get("Количество"))
        price = _fmt_num(item.get("Цена"))
        label = f"{supplier}  {product}  {qty}  {price}"
        rows.append([_cb(label[:118], f"itm:open:{number}:{line}:{nav.status_key(status_idx)}:{status_page}:{item_page}")])
    pager = []
    if item_page > 0:
        pager.append(_cb("◀ ПРЕДЫДУЩИЕ", f"itm:list:{number}:{nav.status_key(status_idx)}:{status_page}:{item_page-1}"))
    if item_page + 1 < total_pages:
        pager.append(_cb("СЛЕДУЮЩИЕ ▶", f"itm:list:{number}:{nav.status_key(status_idx)}:{status_page}:{item_page+1}"))
    if pager:
        rows.append(pager)
    text = f"СТРОКИ ТОВАРА — КП {number}\nСтрок: {len(items)}. Страница {item_page + 1}/{total_pages}."
    if not items:
        text += "\nСтрок товаров нет."
    return {"text": text, "attachments": _keyboard(rows)}


def item_menu(number: str, line: int, status_idx: int, status_page: int, item_page: int) -> dict:
    _, ref_key = _find_kp(number)
    row = _fetch_row(ref_key, line)
    product = _nomenclature_name(row.get("Номенклатура_Key"))
    text = (
        f"КП {number} — строка {line}\n"
        f"Товар: {product}\n"
        f"Количество: {_fmt_num(row.get('Количество'))}\n"
        f"Цена: {_fmt_num(row.get('Цена'))}"
    )
    key = nav.status_key(status_idx)
    rows = [
        [_cb("ВЕРНУТЬСЯ НА ГЛАВНОЕ МЕНЮ", "nav:root")],
        [_cb("🟢 ← ВЕРНУТЬСЯ НА УРОВЕНЬ ВЫШЕ", f"itm:list:{number}:{key}:{status_page}:{item_page}")],
        [_cb("ПОСТАВЩИК И ЦЕНА", f"itm:view:internal:{number}:{line}:{key}:{status_page}:{item_page}")],
        [_cb("КОММЕНТАРИЙ", f"itm:view:buyer:{number}:{line}:{key}:{status_page}:{item_page}")],
        [_cb("ТОВАР", f"itm:view:product:{number}:{line}:{key}:{status_page}:{item_page}")],
        [_cb("ЦЕНА", f"itm:view:price:{number}:{line}:{key}:{status_page}:{item_page}")],
        [_cb("КОЛИЧЕСТВО", f"itm:view:qty:{number}:{line}:{key}:{status_page}:{item_page}")],
    ]
    return {"text": text, "attachments": _keyboard(rows)}


def _field_value(field: str, row: dict):
    if field == "product":
        return _nomenclature_name(row.get("Номенклатура_Key"))
    odata_field = FIELD_MAP[field][1]
    value = row.get(odata_field)
    if field in {"price", "qty"}:
        return _fmt_num(value)
    return str(value or "")


def field_menu(field: str, number: str, line: int, status_idx: int, status_page: int, item_page: int) -> dict:
    if field not in FIELD_MAP:
        raise ValueError("invalid item field")
    _, ref_key = _find_kp(number)
    row = _fetch_row(ref_key, line)
    label = FIELD_MAP[field][0]
    value = _field_value(field, row) or "[ПОЛЕ ПУСТО]"
    text = f"{label} — КП {number}, строка {line}\n\n{value}"
    key = nav.status_key(status_idx)
    rows = [
        [_cb("ВЕРНУТЬСЯ НА ГЛАВНОЕ МЕНЮ", "nav:root")],
        [_cb("🟢 ← ВЕРНУТЬСЯ НА УРОВЕНЬ ВЫШЕ", f"itm:open:{number}:{line}:{key}:{status_page}:{item_page}")],
        [_cb("РЕДАКТИРОВАТЬ", f"itm:edit:{field}:{number}:{line}:{key}:{status_page}:{item_page}")],
    ]
    return {"text": text, "attachments": _keyboard(rows)}


def start_edit(user_id: str, field: str, number: str, line: int, status_idx: int, status_page: int, item_page: int) -> dict:
    if field not in FIELD_MAP:
        raise ValueError("invalid item field")
    _, ref_key = _find_kp(number)
    row = _fetch_row(ref_key, line)
    odata_field = FIELD_MAP[field][1]
    original_raw = row.get(odata_field)
    original_display = _field_value(field, row)
    stage = "await_product_query" if field == "product" else "await_value"
    _set_session(
        user_id, field=field, number=str(number), line=int(line), refKey=ref_key,
        statusIdx=int(status_idx), statusPage=int(status_page), itemPage=int(item_page),
        stage=stage, originalHash=_row_hash(row), originalRaw=original_raw,
        originalDisplay=original_display, proposedRaw=None, proposedDisplay="",
    )
    label = FIELD_MAP[field][0]
    if field == "product":
        instruction = "Введи часть названия номенклатуры."
    elif field in {"internal", "buyer"}:
        instruction = "Пришли новый текст одним сообщением. Для очистки отправь ОЧИСТИТЬ."
    elif field == "price":
        instruction = "Введи новую цену числом. Например: 12500 или 12500,50."
    else:
        instruction = "Введи новое количество числом. Например: 4 или 4,5."
    text = f"Редактирование: {label}\nКП {number}, строка {line}\nТекущее значение: {original_display or '[ПОЛЕ ПУСТО]'}\n\n{instruction}"
    return {"text": text, "attachments": _keyboard([[_cb("ОТМЕНА", "itm:cancel")]])}


def _parse_number(value: str) -> float:
    raw = str(value or "").strip().replace(" ", "").replace(",", ".")
    if not re.fullmatch(r"\d+(?:\.\d+)?", raw):
        raise ValueError("Введи корректное неотрицательное число.")
    return float(raw)


def _escape_odata(value: str) -> str:
    return str(value or "").replace("'", "''")


def _search_products(query: str) -> list[dict]:
    q = str(query or "").strip()
    qn = q.casefold().replace("ё", "е")
    if len(qn) < 2:
        return []
    params = {
        "$select": "Ref_Key,Description,DeletionMark",
        "$filter": f"substringof('{_escape_odata(q)}',Description) eq true",
        "$top": "50",
    }
    r = requests.get(f"{_base()}/Catalog_Номенклатура", headers=core._build_headers(), params=params, timeout=30)
    rows = []
    if r.status_code == 200:
        payload = r.json() if r.content else {}
        rows = payload.get("value", []) if isinstance(payload, dict) else []
    valid = []
    for x in rows:
        if not isinstance(x, dict) or bool(x.get("DeletionMark")):
            continue
        name = str(x.get("Description") or "").strip()
        key = str(x.get("Ref_Key") or "").strip()
        if key and name and qn in name.casefold().replace("ё", "е"):
            valid.append({"Ref_Key": key, "Description": name})
    valid.sort(key=lambda x: (0 if str(x["Description"]).casefold().startswith(qn) else 1, len(str(x["Description"]))))
    return valid[:MAX_PRODUCT_RESULTS]


def product_search_menu(user_id: str, query: str) -> dict:
    s = session_get(user_id)
    if not s or s.get("stage") != "await_product_query":
        raise RuntimeError("product edit is not active")
    if len(str(query or "").strip()) < 2:
        return {"text": "Введи минимум 2 символа названия товара.", "attachments": _keyboard([[_cb("ОТМЕНА", "itm:cancel")]])}
    results = _search_products(query)
    if not results:
        return {"text": f"По запросу «{query}» номенклатура не найдена. Введи другой фрагмент.", "attachments": _keyboard([[_cb("ОТМЕНА", "itm:cancel")]])}
    rows = [[_cb(_compact(x["Description"], 100), f"itm:prod:{x['Ref_Key']}")] for x in results]
    rows.append([_cb("ОТМЕНА", "itm:cancel")])
    return {"text": f"Найдено вариантов: {len(results)}. Выбери товар:", "attachments": _keyboard(rows)}


def _confirm_menu(user_id: str) -> dict:
    s = session_get(user_id)
    if not s or s.get("stage") != "confirm":
        raise RuntimeError("item confirmation is not active")
    label = FIELD_MAP[str(s["field"])][0]
    old = str(s.get("originalDisplay") or "[ПОЛЕ ПУСТО]")
    new = str(s.get("proposedDisplay") or "[ПОЛЕ БУДЕТ ОЧИЩЕНО]")
    text = (
        f"{label} — КП {s['number']}, строка {s['line']}\n"
        f"Старое значение:\n{old}\n\nНовое значение:\n{new}\n\n"
        "Нажми СОХРАНИТЬ для записи в 1С."
    )
    return {"text": text, "attachments": _keyboard([
        [_cb("СОХРАНИТЬ", "itm:save")],
        [_cb("ИЗМЕНИТЬ", "itm:again")],
        [_cb("ОТМЕНА", "itm:cancel")],
    ])}


def set_value(user_id: str, text: str) -> dict:
    s = session_get(user_id)
    if not s or s.get("stage") != "await_value":
        raise RuntimeError("item value edit is not active")
    field = str(s.get("field") or "")
    upper = str(text or "").strip().upper()
    if field in {"internal", "buyer"}:
        proposed = "" if upper == "ОЧИСТИТЬ" else str(text or "")
        if len(proposed) > MAX_TEXT_CHARS:
            raise ValueError(f"Текст слишком длинный. Максимум {MAX_TEXT_CHARS} символов.")
        display = proposed or "[ПОЛЕ БУДЕТ ОЧИЩЕНО]"
    elif field in {"price", "qty"}:
        proposed = _parse_number(text)
        display = _fmt_num(proposed)
    else:
        raise ValueError("Это поле выбирается из справочника.")
    _set_session(user_id, stage="confirm", proposedRaw=proposed, proposedDisplay=display)
    return _confirm_menu(user_id)


def pick_product(user_id: str, product_key: str) -> dict:
    s = session_get(user_id)
    if not s or s.get("stage") != "await_product_query" or s.get("field") != "product":
        raise RuntimeError("product selection is not active")
    name = _nomenclature_name(product_key)
    if not name or name == "—":
        raise RuntimeError("Номенклатура не найдена")
    _set_session(user_id, stage="confirm", proposedRaw=str(product_key), proposedDisplay=name)
    return _confirm_menu(user_id)


def again(user_id: str) -> dict:
    s = session_get(user_id)
    if not s:
        raise RuntimeError("item edit session expired")
    field = str(s.get("field") or "")
    stage = "await_product_query" if field == "product" else "await_value"
    _set_session(user_id, stage=stage, proposedRaw=None, proposedDisplay="")
    label = FIELD_MAP[field][0]
    instruction = "Введи часть названия номенклатуры." if field == "product" else "Введи новое значение."
    if field in {"internal", "buyer"}:
        instruction = "Пришли новый текст одним сообщением. Для очистки отправь ОЧИСТИТЬ."
    elif field == "price":
        instruction = "Введи новую цену числом."
    elif field == "qty":
        instruction = "Введи новое количество числом."
    return {"text": f"Редактирование: {label}\n{instruction}", "attachments": _keyboard([[_cb("ОТМЕНА", "itm:cancel")]])}


def cancel_menu(user_id: str) -> dict:
    s = session_get(user_id)
    if not s:
        return nav.root_menu("user")
    number, line = str(s["number"]), int(s["line"])
    status_idx, status_page, item_page = int(s["statusIdx"]), int(s["statusPage"]), int(s["itemPage"])
    clear(user_id)
    return item_menu(number, line, status_idx, status_page, item_page)


def _audit(user_id: str, role: str, s: dict, new_hash: str) -> None:
    path = Path(os.getenv("KP_MAX_ACCESS_FILE", "/opt/kp-api/data/kp_max_access.json")).parent / "kp_max_item_audit.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts": int(time.time()), "userId": str(user_id), "role": str(role),
        "kp": str(s.get("number") or ""), "line": int(s.get("line") or 0),
        "field": str(s.get("field") or ""), "oldHash": str(s.get("originalHash") or ""),
        "newHash": str(new_hash),
    }
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass


def commit(user_id: str, role: str) -> tuple[dict, dict]:
    s = session_get(user_id)
    if not s or s.get("stage") != "confirm":
        raise RuntimeError("item confirmation is not active")
    field = str(s.get("field") or "")
    odata_field = FIELD_MAP[field][1]
    ref_key = str(s.get("refKey") or "")
    line = int(s.get("line") or 0)
    current = _fetch_row(ref_key, line)
    if _row_hash(current) != str(s.get("originalHash") or ""):
        clear(user_id)
        raise RuntimeError("item changed concurrently")
    proposed = s.get("proposedRaw")
    headers = {**core._build_headers(), "Content-Type": "application/json; charset=utf-8"}
    r = requests.patch(_row_url(ref_key, line), headers=headers, json={odata_field: proposed}, timeout=35)
    if r.status_code not in (200, 204):
        raise RuntimeError(f"1C item PATCH HTTP {r.status_code}: {r.text[:300]}")
    verified = _fetch_row(ref_key, line)
    actual = verified.get(odata_field)
    ok = False
    if field in {"price", "qty"}:
        try:
            ok = abs(float(actual) - float(proposed)) < 1e-9
        except Exception:
            ok = False
    else:
        ok = str(actual or "") == str(proposed or "")
    if not ok:
        raise RuntimeError("1C item verification failed")
    new_hash = _row_hash(verified)
    _audit(user_id, role, s, new_hash)
    number = str(s["number"]); status_idx = int(s["statusIdx"])
    status_page = int(s["statusPage"]); item_page = int(s["itemPage"])
    label = FIELD_MAP[field][0]
    clear(user_id)
    menu = field_menu(field, number, line, status_idx, status_page, item_page)
    menu["text"] = f"{label} сохранено в 1С.\n\n" + menu["text"]
    return menu, {"number": number, "line": line, "field": field}


def _items_hash(rows: list[dict]) -> str:
    raw = json.dumps(rows, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def start_add(user_id: str, number: str, status_idx: int, status_page: int, item_page: int) -> dict:
    _, ref_key = _find_kp(number)
    current = _fetch_items(ref_key)
    _set_session(
        user_id, mode="add", stage="add_product_query", number=str(number), refKey=ref_key,
        statusIdx=int(status_idx), statusPage=int(status_page), itemPage=int(item_page),
        originalItemsHash=_items_hash(current), productKey="", productName="", addQty=None, addPrice=None,
    )
    return {
        "text": f"ДОБАВИТЬ СТРОКУ — КП {number}\n\nВведи часть названия номенклатуры.",
        "attachments": _keyboard([[_cb("ОТМЕНА", "itm:addcancel")]]),
    }


def add_product_search_menu(user_id: str, query: str) -> dict:
    s = session_get(user_id)
    if not s or s.get("mode") != "add" or s.get("stage") != "add_product_query":
        raise RuntimeError("add item product search is not active")
    if len(str(query or "").strip()) < 2:
        return {"text": "Введи минимум 2 символа названия товара.", "attachments": _keyboard([[_cb("ОТМЕНА", "itm:addcancel")]])}
    results = _search_products(query)
    if not results:
        return {"text": f"По запросу «{query}» номенклатура не найдена. Введи другой фрагмент.", "attachments": _keyboard([[_cb("ОТМЕНА", "itm:addcancel")]])}
    rows = [[_cb(_compact(x["Description"], 100), f"itm:addprod:{x['Ref_Key']}")] for x in results]
    rows.append([_cb("ОТМЕНА", "itm:addcancel")])
    return {"text": f"Найдено вариантов: {len(results)}. Выбери товар:", "attachments": _keyboard(rows)}


def pick_add_product(user_id: str, product_key: str) -> dict:
    s = session_get(user_id)
    if not s or s.get("mode") != "add" or s.get("stage") != "add_product_query":
        raise RuntimeError("add item product selection is not active")
    name = _nomenclature_name(product_key)
    if not name or name == "—":
        raise RuntimeError("Номенклатура не найдена")
    _set_session(user_id, stage="add_qty", productKey=str(product_key), productName=name)
    return {
        "text": f"Новая строка — КП {s['number']}\nТовар: {name}\n\nВведи количество числом.",
        "attachments": _keyboard([[_cb("ОТМЕНА", "itm:addcancel")]]),
    }


def _add_confirm_menu(user_id: str) -> dict:
    s = session_get(user_id)
    if not s or s.get("mode") != "add" or s.get("stage") != "add_confirm":
        raise RuntimeError("add item confirmation is not active")
    text = (
        f"ДОБАВИТЬ СТРОКУ — КП {s['number']}\n"
        f"Товар: {s.get('productName') or '—'}\n"
        f"Количество: {_fmt_num(s.get('addQty'))}\n"
        f"Цена: {_fmt_num(s.get('addPrice'))}\n\n"
        "Нажми СОХРАНИТЬ для добавления строки в 1С."
    )
    return {"text": text, "attachments": _keyboard([
        [_cb("СОХРАНИТЬ", "itm:addsave")],
        [_cb("ИЗМЕНИТЬ", "itm:addrestart")],
        [_cb("ОТМЕНА", "itm:addcancel")],
    ])}


def set_add_value(user_id: str, text: str) -> dict:
    s = session_get(user_id)
    if not s or s.get("mode") != "add":
        raise RuntimeError("add item session is not active")
    stage = str(s.get("stage") or "")
    if stage == "add_qty":
        qty = _parse_number(text)
        if qty <= 0:
            raise ValueError("Количество должно быть больше нуля.")
        _set_session(user_id, stage="add_price", addQty=qty)
        return {
            "text": f"Новая строка — КП {s['number']}\nТовар: {s.get('productName') or '—'}\nКоличество: {_fmt_num(qty)}\n\nВведи цену числом.",
            "attachments": _keyboard([[_cb("ОТМЕНА", "itm:addcancel")]]),
        }
    if stage == "add_price":
        price = _parse_number(text)
        _set_session(user_id, stage="add_confirm", addPrice=price)
        return _add_confirm_menu(user_id)
    raise RuntimeError("add item value is not expected")


def restart_add(user_id: str) -> dict:
    s = session_get(user_id)
    if not s or s.get("mode") != "add":
        raise RuntimeError("add item session is not active")
    _set_session(user_id, stage="add_product_query", productKey="", productName="", addQty=None, addPrice=None)
    return {
        "text": f"ДОБАВИТЬ СТРОКУ — КП {s['number']}\n\nВведи часть названия номенклатуры.",
        "attachments": _keyboard([[_cb("ОТМЕНА", "itm:addcancel")]]),
    }


def cancel_add_menu(user_id: str) -> dict:
    s = session_get(user_id)
    if not s or s.get("mode") != "add":
        return nav.root_menu("user")
    number = str(s["number"])
    status_idx = int(s["statusIdx"]); status_page = int(s["statusPage"]); item_page = int(s["itemPage"])
    clear(user_id)
    return list_menu(number, status_idx, status_page, item_page)


def commit_add(user_id: str, role: str) -> tuple[dict, dict]:
    s = session_get(user_id)
    if not s or s.get("mode") != "add" or s.get("stage") != "add_confirm":
        raise RuntimeError("add item confirmation is not active")
    ref_key = str(s.get("refKey") or "")
    current = _fetch_items(ref_key)
    if _items_hash(current) != str(s.get("originalItemsHash") or ""):
        clear(user_id)
        raise RuntimeError("items changed concurrently")
    old_lines = {int(x.get("LineNumber") or 0) for x in current}
    new_line = max(old_lines or {0}) + 1
    qty = float(s.get("addQty") or 0)
    price = float(s.get("addPrice") or 0)
    new_row = {
        "Ref_Key": ref_key,
        "LineNumber": new_line,
        "Номенклатура_Key": str(s.get("productKey") or ""),
        "Количество": qty,
        "Цена": price,
        "Сумма": qty * price,
        "КомментарийВнутренний": "",
        "КомментарийДляПокупателя": "",
    }
    headers = {**core._build_headers(), "Content-Type": "application/json; charset=utf-8"}
    url = f"{_base()}/{core.ENTITY}(guid'{ref_key}')"
    r = requests.patch(url, headers=headers, json={"Товары": current + [new_row]}, timeout=40)
    if r.status_code not in (200, 204):
        raise RuntimeError(f"1C add item PATCH HTTP {r.status_code}: {r.text[:300]}")
    after = _fetch_items(ref_key)
    added = [x for x in after if int(x.get("LineNumber") or 0) not in old_lines]
    if len(added) != 1:
        raise RuntimeError("1C add item verification failed")
    created = added[0]
    created_line = int(created.get("LineNumber") or 0)
    same_product = str(created.get("Номенклатура_Key") or "") == str(s.get("productKey") or "")
    try:
        same_qty = abs(float(created.get("Количество") or 0) - qty) < 1e-9
        same_price = abs(float(created.get("Цена") or 0) - price) < 1e-9
    except Exception:
        same_qty = same_price = False
    if not (same_product and same_qty and same_price):
        raise RuntimeError("1C added row values verification failed")
    audit_s = dict(s); audit_s["line"] = created_line; audit_s["field"] = "add"
    _audit(user_id, role, audit_s, _items_hash(after))
    number = str(s["number"]); status_idx = int(s["statusIdx"])
    status_page = int(s["statusPage"]); item_page = int(s["itemPage"])
    clear(user_id)
    menu = item_menu(number, created_line, status_idx, status_page, item_page)
    menu["text"] = "Строка добавлена в 1С.\n\n" + menu["text"]
    return menu, {"number": number, "line": created_line, "field": "add"}
