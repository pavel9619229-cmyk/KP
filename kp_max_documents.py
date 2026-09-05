import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from threading import Lock

import requests

import api_proxy as core
import kp_max_items as items
import kp_max_live_rows as live_rows
import kp_max_navigation as nav

ORDER_ENTITY = "Document_ЗаказКлиента"
ZERO_GUID = "00000000-0000-0000-0000-000000000000"
KP_TYPE = "StandardODATA.Document_КоммерческоеПредложениеКлиенту"
_SCAN_LIMIT = 1200
_LOCK = Lock()
_INDEX_LOCK = Lock()


def _base() -> str:
    return str(core.BASE).strip().strip('"').strip("'").rstrip("/")


def _now_1c() -> str:
    tz = getattr(core, "_TZ_MSK", None)
    current = datetime.now(tz) if tz is not None else datetime.now()
    return current.replace(tzinfo=None, microsecond=0).isoformat()


def _short_number(value) -> str:
    raw = str(value or "").strip()
    m = re.search(r"(\d+)$", raw)
    return str(int(m.group(1))) if m else raw


def _index_path() -> Path:
    base = Path(os.getenv("KP_MAX_ACCESS_FILE", "/opt/kp-api/data/kp_max_access.json")).parent
    return base / "kp_max_documents.json"


def _load_index() -> dict:
    path = _index_path()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_index(data: dict) -> None:
    path = _index_path(); path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass


def _remember(kp_ref: str, doc: dict) -> None:
    ref = str(doc.get("Ref_Key") or doc.get("refKey") or "").strip()
    if not ref:
        return
    entry = {
        "refKey": ref,
        "number": str(doc.get("Number") or doc.get("number") or "").strip(),
        "date": str(doc.get("Date") or doc.get("date") or ""),
        "sum": float(doc.get("СуммаДокумента") or doc.get("sum") or 0),
        "posted": bool(doc.get("Posted")),
    }
    with _INDEX_LOCK:
        data = _load_index(); bucket = data.setdefault(str(kp_ref), [])
        bucket = [x for x in bucket if str(x.get("refKey") or "") != ref]
        bucket.insert(0, entry); data[str(kp_ref)] = bucket[:20]; _save_index(data)


def _find_kp(number: str) -> tuple[dict, str]:
    wanted = _short_number(number)
    for row in live_rows.load(force=True):
        if _short_number(row.get("number")) == wanted:
            ref = str(row.get("refKey") or row.get("Ref_Key") or "").strip()
            if ref:
                return dict(row), ref
    raise RuntimeError(f"КП {number} не найдено")


def _fetch_kp(ref_key: str) -> dict:
    select = (
        "Ref_Key,Number,Date,Организация_Key,Клиент_Key,Контрагент_Key,Менеджер_Key,"
        "Валюта_Key,ЦенаВключаетНДС,СрокДействия,СуммаДокумента,Комментарий,Товары"
    )
    r = requests.get(
        f"{_base()}/{core.ENTITY}(guid'{ref_key}')",
        headers=core._build_headers(), params={"$select": select}, timeout=45,
    )
    if r.status_code != 200:
        raise RuntimeError(f"1C KP read HTTP {r.status_code}: {r.text[:300]}")
    data = r.json() if r.content else {}
    return data if isinstance(data, dict) else {}


def _valid_guid(value) -> str:
    text = str(value or "").strip()
    return text if re.fullmatch(r"[0-9a-fA-F-]{36}", text) else ""


def _vat_key_from_kp_item(row: dict) -> str:
    direct = _valid_guid(row.get("СтавкаНДС")) or _valid_guid(row.get("СтавкаНДС_Key"))
    return direct or items._vat22_key()


def _calc_amounts(row: dict, price_includes_vat: bool, vat_rate: float = 22.0) -> tuple[float, float, float]:
    qty = float(row.get("Количество") or 0); price = float(row.get("Цена") or 0)
    base_sum = float(row.get("Сумма") or (qty * price))
    src_vat = float(row.get("СуммаНДС") or 0); src_total = float(row.get("СуммаСНДС") or 0)
    if src_total > 0 or src_vat > 0:
        total = src_total if src_total > 0 else (base_sum if price_includes_vat else base_sum + src_vat)
        return base_sum, src_vat, total
    if price_includes_vat:
        total = base_sum
        vat = round(total * vat_rate / (100.0 + vat_rate), 2)
        return base_sum, vat, total
    vat = round(base_sum * vat_rate / 100.0, 2)
    return base_sum, vat, round(base_sum + vat, 2)


def _order_items(kp: dict) -> list[dict]:
    source = kp.get("Товары") or []
    if not isinstance(source, list) or not source:
        raise RuntimeError("В КП нет строк товара")
    price_includes_vat = bool(kp.get("ЦенаВключаетНДС", True))
    result = []
    for idx, row in enumerate(source, 1):
        if not isinstance(row, dict):
            continue
        product = _valid_guid(row.get("Номенклатура_Key"))
        qty = float(row.get("Количество") or 0); price = float(row.get("Цена") or 0)
        if not product or qty <= 0:
            raise RuntimeError(f"Строка {idx}: не заполнен товар или количество")
        amount, vat, total = _calc_amounts(row, price_includes_vat)
        item = {
            "LineNumber": idx, "КодСтроки": idx,
            "Номенклатура_Key": product,
            "Характеристика_Key": _valid_guid(row.get("Характеристика_Key")) or ZERO_GUID,
            "Количество": qty, "Цена": price, "Сумма": amount,
            "СтавкаНДС_Key": _vat_key_from_kp_item(row),
            "СуммаНДС": vat, "СуммаСНДС": total, "Отменено": False,
        }
        result.append(item)
    if not result:
        raise RuntimeError("В КП нет корректных строк товара")
    return result


def _ensure_kp_required_fields(kp_ref: str, kp: dict) -> dict:
    patch = {}
    deadline = str(kp.get("СрокДействия") or "")
    if not deadline or deadline.startswith("0001-01-01"):
        source_date = str(kp.get("Date") or _now_1c())[:10]
        patch["СрокДействия"] = source_date + "T00:00:00"
    order_items = _order_items(kp)
    total = round(sum(float(x.get("СуммаСНДС") or x.get("Сумма") or 0) for x in order_items), 2)
    try:
        current_total = float(kp.get("СуммаДокумента") or 0)
    except Exception:
        current_total = 0.0
    if abs(current_total - total) > 0.009:
        patch["СуммаДокумента"] = total
    if not patch:
        return kp
    headers = {**core._build_headers(), "Content-Type": "application/json; charset=utf-8"}
    r = requests.patch(f"{_base()}/{core.ENTITY}(guid'{kp_ref}')", headers=headers, json=patch, timeout=40)
    if r.status_code not in (200, 204):
        raise RuntimeError(f"1C KP required fields PATCH HTTP {r.status_code}: {r.text[:300]}")
    refreshed = _fetch_kp(kp_ref)
    return refreshed


def _build_order_payload(kp_ref: str, kp: dict) -> dict:
    partner = _valid_guid(kp.get("Клиент_Key")); counterparty = _valid_guid(kp.get("Контрагент_Key"))
    org = _valid_guid(kp.get("Организация_Key")); currency = _valid_guid(kp.get("Валюта_Key"))
    manager = _valid_guid(kp.get("Менеджер_Key")); order_items = _order_items(kp)
    if not partner or not counterparty or not org:
        raise RuntimeError("В КП не заполнены клиент, контрагент или организация")
    total = round(sum(float(x.get("СуммаСНДС") or x.get("Сумма") or 0) for x in order_items), 2)
    payload = {
        "Date": _now_1c(), "DeletionMark": False, "Posted": False,
        "Партнер_Key": partner, "Контрагент_Key": counterparty, "Организация_Key": org,
        "Валюта_Key": currency or ZERO_GUID, "Менеджер_Key": manager or ZERO_GUID,
        "СуммаДокумента": total, "ЦенаВключаетНДС": bool(kp.get("ЦенаВключаетНДС", True)),
        "ДокументОснование": str(kp_ref), "ДокументОснование_Type": KP_TYPE,
        "ФормаОплаты": "Безналичная", "НалогообложениеНДС": "ПродажаОблагаетсяНДС",
        "ХозяйственнаяОперация": "РеализацияКлиенту",
        "Комментарий": f"Создано из КП {_short_number(kp.get('Number'))} через MAX",
        "Товары": order_items,
    }
    return payload


def _fetch_order(ref_key: str) -> dict:
    select = "Ref_Key,Number,Date,Posted,DeletionMark,СуммаДокумента,ДокументОснование,ДокументОснование_Type"
    r = requests.get(f"{_base()}/{ORDER_ENTITY}(guid'{ref_key}')", headers=core._build_headers(), params={"$select": select}, timeout=35)
    if r.status_code != 200:
        return {}
    data = r.json() if r.content else {}
    return data if isinstance(data, dict) else {}


def _tail_linked_orders(kp_ref: str) -> list[dict]:
    try:
        c = requests.get(f"{_base()}/{ORDER_ENTITY}/$count", headers=core._build_headers(), timeout=45)
        total = int(c.text.strip()) if c.status_code == 200 else 0
        start = max(0, total - _SCAN_LIMIT)
        select = "Ref_Key,Number,Date,Posted,DeletionMark,СуммаДокумента,ДокументОснование,ДокументОснование_Type"
        r = requests.get(
            f"{_base()}/{ORDER_ENTITY}", headers=core._build_headers(),
            params={"$select": select, "$top": str(_SCAN_LIMIT), "$skip": str(start)}, timeout=60,
        )
        rows = r.json().get("value", []) if r.status_code == 200 else []
    except Exception:
        rows = []
    result = []
    for row in rows:
        if not isinstance(row, dict) or bool(row.get("DeletionMark")):
            continue
        if str(row.get("ДокументОснование") or "") != str(kp_ref):
            continue
        if not str(row.get("ДокументОснование_Type") or "").endswith("Document_КоммерческоеПредложениеКлиенту"):
            continue
        result.append(row)
    result.sort(key=lambda x: str(x.get("Date") or ""), reverse=True)
    return result


def linked_orders(kp_ref: str) -> list[dict]:
    by_ref = {str(x.get("Ref_Key") or ""): x for x in _tail_linked_orders(kp_ref) if x.get("Ref_Key")}
    with _INDEX_LOCK:
        saved = list(_load_index().get(str(kp_ref), []) or [])
    for entry in saved:
        ref = str(entry.get("refKey") or "").strip()
        if not ref or ref in by_ref:
            continue
        doc = _fetch_order(ref)
        if doc and not bool(doc.get("DeletionMark")) and str(doc.get("ДокументОснование") or "") == str(kp_ref):
            by_ref[ref] = doc
    docs = list(by_ref.values()); docs.sort(key=lambda x: str(x.get("Date") or ""), reverse=True)
    return docs


def _date_label(value: str) -> str:
    try:
        dt = core._parse_odata_datetime(str(value or ""))
        if dt is not None:
            return dt.strftime("%d.%m.%y")
    except Exception:
        pass
    return str(value or "").replace("T", " ")[:10] or "—"


def _money(value) -> str:
    try:
        n = float(value or 0)
        return f"{n:,.2f}".replace(",", " ").replace(".00", "")
    except Exception:
        return "0"


def group_menu(number: str, status_idx: int, page: int, prefix: str = "") -> dict:
    _, kp_ref = _find_kp(number)
    docs = linked_orders(kp_ref)
    key = nav.status_key(status_idx); page = max(0, int(page))
    rows = [
        [nav._cb("🟢🟢 ← ВЕРНУТЬСЯ НА ГЛАВНОЕ МЕНЮ", "nav:root")],
        [nav._cb("🟢 ← ВЕРНУТЬСЯ НА УРОВЕНЬ ВЫШЕ", f"nav:k:{number}:{key}:{page}")],
    ]
    for doc in docs[:20]:
        ref = str(doc.get("Ref_Key") or "").strip(); raw = str(doc.get("Number") or "").strip()
        label = f"СЧЕТ {_short_number(raw)}  {_date_label(doc.get('Date'))}  {_money(doc.get('СуммаДокумента'))} ₽"
        rows.append([nav._cb(label, f"nav:doc:{number}:{ref}:{key}:{page}")])
    text = f"ГРУППА ДОКУМЕНТОВ — КП {number}\n"
    if prefix:
        text += f"\n{prefix}\n"
    text += f"\nСвязанных документов: {len(docs)}."
    if not docs:
        text += "\nСвязанных счетов/заказов клиента пока нет."
    return {"text": text, "attachments": nav._keyboard(rows)}


def document_menu(number: str, ref_key: str, status_idx: int, page: int) -> dict:
    doc = _fetch_order(ref_key)
    key = nav.status_key(status_idx); page = max(0, int(page))
    if not doc:
        return group_menu(number, status_idx, page, "Документ не найден в 1С.")
    raw = str(doc.get("Number") or "").strip()
    text = (
        f"СЧЕТ {_short_number(raw)}\n"
        f"Дата: {_date_label(doc.get('Date'))}\n"
        f"Сумма: {_money(doc.get('СуммаДокумента'))} ₽\n"
        f"Проведен: {'ДА' if bool(doc.get('Posted')) else 'НЕТ'}"
    )
    rows = [
        [nav._cb("🟢🟢 ← ВЕРНУТЬСЯ НА ГЛАВНОЕ МЕНЮ", "nav:root")],
        [nav._cb("🟢 ← ВЕРНУТЬСЯ НА УРОВЕНЬ ВЫШЕ", f"nav:docs:{number}:{key}:{page}")],
        [nav._cb("РЕДАКТИРОВАТЬ ИНФОРМАЦИЮ ДЛЯ ПЕЧАТИ", f"nav:docedit:{number}:{ref_key}:{key}:{page}")],
        [nav._cb("ЗАГРУЗИТЬ ПЕЧАТНУЮ ФОРМУ В МАКС", f"nav:docprint:{number}:{ref_key}:{key}:{page}")],
    ]
    return {"text": text, "attachments": nav._keyboard(rows)}


def print_info_menu(number: str, ref_key: str, status_idx: int, page: int) -> dict:
    key=nav.status_key(status_idx); page=max(0,int(page))
    text=f"РЕДАКТИРОВАТЬ ИНФОРМАЦИЮ ДЛЯ ПЕЧАТИ — КП {number}\n\nРаздел готов. Поля для редактирования настроим следующим шагом."
    rows=[[nav._cb("🟢🟢 ← ВЕРНУТЬСЯ НА ГЛАВНОЕ МЕНЮ","nav:root")],[nav._cb("🟢 ← ВЕРНУТЬСЯ НА УРОВЕНЬ ВЫШЕ",f"nav:doc:{number}:{ref_key}:{key}:{page}")]]
    return {"text":text,"attachments":nav._keyboard(rows)}

def print_form_menu(number: str, ref_key: str, status_idx: int, page: int) -> dict:
    key=nav.status_key(status_idx); page=max(0,int(page))
    text=f"ЗАГРУЗИТЬ ПЕЧАТНУЮ ФОРМУ В МАКС — КП {number}\n\nРаздел готов. Генерацию и отправку файла подключим следующим шагом."
    rows=[[nav._cb("🟢🟢 ← ВЕРНУТЬСЯ НА ГЛАВНОЕ МЕНЮ","nav:root")],[nav._cb("🟢 ← ВЕРНУТЬСЯ НА УРОВЕНЬ ВЫШЕ",f"nav:doc:{number}:{ref_key}:{key}:{page}")]]
    return {"text":text,"attachments":nav._keyboard(rows)}


def _audit(user_id: str, role: str, kp_number: str, kp_ref: str, doc: dict) -> None:
    path = _index_path().with_name("kp_max_invoice_audit.jsonl")
    payload = {
        "ts": int(time.time()), "userId": str(user_id), "role": str(role),
        "kp": str(kp_number), "kpRef": str(kp_ref),
        "orderRef": str(doc.get("Ref_Key") or ""), "orderNumber": str(doc.get("Number") or ""),
    }
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass


def create_invoice_and_menu(user_id: str, role: str, number: str, status_idx: int, page: int) -> tuple[dict, dict]:
    with _LOCK:
        _, kp_ref = _find_kp(number)
        existing = linked_orders(kp_ref)
        if existing:
            first = existing[0]
            return group_menu(number, status_idx, page, f"Счет уже существует: {_short_number(first.get('Number'))}."), first
        kp = _ensure_kp_required_fields(kp_ref, _fetch_kp(kp_ref))
        payload = _build_order_payload(kp_ref, kp)
        headers = {**core._build_headers(), "Content-Type": "application/json; charset=utf-8"}
        r = requests.post(f"{_base()}/{ORDER_ENTITY}", headers=headers, json=payload, timeout=45)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"1C create invoice HTTP {r.status_code}: {r.text[:500]}")
        created = r.json() if r.content else {}
        if not isinstance(created, dict):
            created = {}
        ref = str(created.get("Ref_Key") or "").strip()
        if ref and not str(created.get("Number") or "").strip():
            created = _fetch_order(ref)
        if not ref:
            ref = str(created.get("Ref_Key") or "").strip()
        if not ref:
            raise RuntimeError("1C не вернула Ref_Key созданного счета")
        verified = _fetch_order(ref)
        if verified:
            created = verified
        if str(created.get("ДокументОснование") or "") != str(kp_ref):
            raise RuntimeError("Созданный счет не связан с исходным КП")
        _remember(kp_ref, created); _audit(user_id, role, number, kp_ref, created)
        raw_number = _short_number(created.get("Number"))
        menu = group_menu(number, status_idx, page, f"Счет {raw_number} создан в 1С.")
        return menu, created
