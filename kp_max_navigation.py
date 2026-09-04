import math
from datetime import datetime

import api_proxy as core
import kp_max_live_rows as live_rows

PAGE_SIZE = 10
MAX_ROWS = 300
STATUS_LABELS = [
    "ВСЕ",
    "ОБРАБОТАТЬ И ОТПРАВИТЬ",
    "ПОЛУЧИТЬ ОБРАТНУЮ СВЯЗЬ",
    "ОПЛАТА ПРИШЛА",
    "ОТГРУЗИТЬ",
    "ПРОВЕСТИ В ЭДО",
    "ПРОБЛЕМА",
    "ОТГРУЖЕНО ОПЛАЧЕНО И КОРРЕКТНО ОФОРМЛЕНО",
    "ОТКАЗ",
]


def _cb(text: str, payload: str) -> dict:
    return {"type": "callback", "text": text, "payload": payload}


def _keyboard(rows: list[list[dict]]) -> list[dict]:
    return [{"type": "inline_keyboard", "payload": {"buttons": rows}}]


def _sort_key(row: dict) -> str:
    return str(row.get("createdAt") or "")


def recent_rows() -> list[dict]:
    try:
        source = live_rows.load()
    except Exception:
        source = list(core._cached_rows)
    rows = sorted((dict(r) for r in source), key=_sort_key, reverse=True)[:MAX_ROWS]
    return core.build_rows_with_computed_status(rows)


def status_key(index: int) -> str:
    if index < 0 or index >= len(STATUS_LABELS):
        raise ValueError("invalid status index")
    return str(index)


def status_index(key: str) -> int:
    value = int(str(key))
    if value < 0 or value >= len(STATUS_LABELS):
        raise ValueError("invalid status key")
    return value


def workflow_status(row: dict) -> str:
    first = str(row.get("additionalInfoFirstLine") or "").strip().upper()
    if bool(row.get("problem")) or first.startswith("ПРОБЛЕМА"):
        return "ПРОБЛЕМА"
    if bool(row.get("rejected")) or first.startswith("ОТКАЗ"):
        return "ОТКАЗ"
    payment = bool(row.get("paymentReceived"))
    invoice = bool(row.get("invoiceCreated"))
    edo = bool(row.get("edoSent"))
    if payment and invoice and edo:
        return "ОТГРУЖЕНО ОПЛАЧЕНО И КОРРЕКТНО ОФОРМЛЕНО"
    if invoice and not edo:
        return "ПРОВЕСТИ В ЭДО"
    if bool(row.get("shipmentPending")) or first.startswith("ОТГРУЗИТЬ"):
        return "ОТГРУЗИТЬ"
    if payment or first.startswith("ОПЛАТА ПРИШЛА"):
        return "ОПЛАТА ПРИШЛА"
    if bool(row.get("kpSent")) or bool(row.get("receiptConfirmed")):
        return "ПОЛУЧИТЬ ОБРАТНУЮ СВЯЗЬ"
    return "ОБРАБОТАТЬ И ОТПРАВИТЬ"


def rows_for_status(index: int) -> list[dict]:
    rows = recent_rows()
    if index == 0:
        return rows
    wanted = STATUS_LABELS[index]
    return [row for row in rows if workflow_status(row) == wanted]


def _date_label(value: str) -> str:
    raw = str(value or "").split(" ", 1)[0]
    try:
        return datetime.strptime(raw, "%Y-%m-%d").strftime("%d.%m.%y")
    except Exception:
        return raw or "—"


def _compact(value: str, limit: int) -> str:
    text = " ".join(str(value or "").split()) or "—"
    return text if len(text) <= limit else text[: max(1, limit - 1)].rstrip() + "…"


def kp_button_text(row: dict) -> str:
    number = str(row.get("number") or "—")
    date = _date_label(row.get("createdAt") or "")
    client = _compact(row.get("customerName") or "—", 32)
    comment = _compact(row.get("additionalInfoFirstLine") or "—", 52)
    label = f"{number}\u00a0\u00a0{date}\u00a0\u00a0{client} {comment}"
    return label if len(label) <= 118 else label[:117].rstrip() + "…"


def root_menu(role: str) -> dict:
    rows = [[_cb("СОЗДАТЬ НОВОЕ КП", "nav:create")]]
    rows.append([_cb("СПИСОК КП ПО СТАТУСАМ", "nav:statuses")])
    rows.append([_cb("🔎 ПОИСК КП", "find:menu")])
    rows.append([_cb("ПРОВЕРИТЬ ДОСТУП", "nav:access")])
    if role == "admin":
        rows.append([_cb("ВЫДАТЬ КОД СОТРУДНИКУ", "nav:invite")])
    return {
        "text": "Главное меню КП",
        "attachments": _keyboard(rows),
    }


def create_kp_menu() -> dict:
    return {
        "text": "СОЗДАТЬ НОВОЕ КП\n\nЭкран создания нового КП. Параметры нового документа настроим отдельно; сейчас нажатие ничего в 1С не создаёт.",
        "attachments": _keyboard([
            [_cb("🟢🟢 ← ВЕРНУТЬСЯ НА ГЛАВНОЕ МЕНЮ", "nav:root")],
            [_cb("🟢 ← ВЕРНУТЬСЯ НА УРОВЕНЬ ВЫШЕ", "nav:root")],
        ]),
    }


def statuses_menu() -> dict:
    try:
        live_rows.load(force=True)
    except Exception:
        pass
    rows = [
        [_cb("🟢🟢 ← ВЕРНУТЬСЯ НА ГЛАВНОЕ МЕНЮ", "nav:root")],
        [_cb("🟢 ← ВЕРНУТЬСЯ НА УРОВЕНЬ ВЫШЕ", "nav:root")],
    ]
    for index, label in enumerate(STATUS_LABELS):
        rows.append([_cb(label, f"nav:s:{status_key(index)}:0")])
    return {
        "text": "Уровень 1 — выбери статус КП.\nПоказаны статусы для последних 300 КП.",
        "attachments": _keyboard(rows),
    }


def status_page(index: int, page: int) -> dict:
    items = rows_for_status(index)
    total_pages = max(1, math.ceil(len(items) / PAGE_SIZE))
    page = max(0, min(int(page), total_pages - 1))
    start = page * PAGE_SIZE
    current = items[start : start + PAGE_SIZE]
    rows = [
        [_cb("🟢🟢 ← ВЕРНУТЬСЯ НА ГЛАВНОЕ МЕНЮ", "nav:root")],
        [_cb("🟢 ← ВЕРНУТЬСЯ НА УРОВЕНЬ ВЫШЕ", "nav:statuses")],
    ]
    for row in current:
        number = str(row.get("number") or "")
        rows.append([_cb(kp_button_text(row), f"nav:k:{number}:{status_key(index)}:{page}")])
    pager = []
    if page > 0:
        pager.append(_cb("◀ ПРЕДЫДУЩИЕ", f"nav:s:{status_key(index)}:{page - 1}"))
    if page + 1 < total_pages:
        pager.append(_cb("СЛЕДУЮЩИЕ ▶", f"nav:s:{status_key(index)}:{page + 1}"))
    if pager:
        rows.append(pager)
    label = STATUS_LABELS[index]
    shown_from = start + 1 if current else 0
    shown_to = start + len(current)
    text = (
        f"Уровень 2 — {label}\n"
        f"КП: {len(items)} из последних {MAX_ROWS}. "
        f"Показаны {shown_from}–{shown_to}. Страница {page + 1}/{total_pages}."
    )
    if not current:
        text += "\nПо этому статусу КП нет."
    return {"text": text, "attachments": _keyboard(rows)}


def find_row(number: str) -> dict | None:
    normalized = str(number).lstrip("0") or "0"
    for row in recent_rows():
        row_number = str(row.get("number") or "").lstrip("0") or "0"
        if row_number == normalized:
            return row
    return None


def kp_level3(number: str, status_idx: int, page: int) -> dict:
    row = find_row(number)
    if not row:
        return status_page(status_idx, page)
    number_label = str(row.get("number") or number)
    date_label = _date_label(row.get("createdAt") or "")
    text = f"КП {number_label}\nДата: {date_label}"
    key = status_key(status_idx)
    page = max(0, int(page))
    rows = [
        [_cb("🟢🟢 ← ВЕРНУТЬСЯ НА ГЛАВНОЕ МЕНЮ", "nav:root")],
        [_cb("🟢 ← ВЕРНУТЬСЯ НА УРОВЕНЬ ВЫШЕ", f"nav:s:{key}:{page}")],
        [_cb("КЛИЕНТ", f"nav:f:client:{number_label}:{key}:{page}")],
        [_cb("СТРОКИ ТОВАРА", f"nav:f:items:{number_label}:{key}:{page}")],
        [_cb("КОММЕНТАРИЙ", f"nav:f:comment:{number_label}:{key}:{page}")],
    ]
    return {"text": text, "attachments": _keyboard(rows)}


def comment_menu(number: str, status_idx: int, page: int, comment: str, *, overflow: bool = False) -> dict:
    key = status_key(status_idx)
    page = max(0, int(page))
    clean = str(comment or "").strip() or "Комментарий не заполнен."
    if overflow:
        text = f"КОММЕНТАРИЙ — КП {number}\nПолный текст отправлен отдельными сообщениями ниже."
    else:
        text = f"КОММЕНТАРИЙ — КП {number}\n\n{clean}"
    rows = [
        [_cb("🟢🟢 ← ВЕРНУТЬСЯ НА ГЛАВНОЕ МЕНЮ", "nav:root")],
        [_cb("🟢 ← ВЕРНУТЬСЯ НА УРОВЕНЬ ВЫШЕ", f"nav:k:{number}:{key}:{page}")],
        [_cb("РЕДАКТИРОВАТЬ", f"nav:ce:{number}:{key}:{page}")],
    ]
    return {"text": text, "attachments": _keyboard(rows)}


def comment_edit_started_menu(number: str) -> dict:
    return {
        "text": (
            f"Редактирование комментария КП {number}.\n"
            "Пришли новый текст комментария одним сообщением.\n"
            "Для очистки поля отправь ОЧИСТИТЬ. Для выхода — ОТМЕНА."
        ),
        "attachments": [],
    }


def field_placeholder(field: str, number: str, status_idx: int, page: int) -> dict:
    labels = {"client": "КЛИЕНТ", "items": "СТРОКИ ТОВАРА"}
    label = labels.get(str(field), "РАЗДЕЛ")
    text = f"{label} — КП {number}\nСодержимое этого раздела настроим следующим этапом."
    rows = [
        [_cb("🟢🟢 ← ВЕРНУТЬСЯ НА ГЛАВНОЕ МЕНЮ", "nav:root")],
        [_cb(
        "🟢 ← ВЕРНУТЬСЯ НА УРОВЕНЬ ВЫШЕ",
        f"nav:k:{number}:{status_key(status_idx)}:{max(0, int(page))}",
        )],
    ]
    return {"text": text, "attachments": _keyboard(rows)}
