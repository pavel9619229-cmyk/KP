import re
import time
from threading import Lock

import api_proxy as core
import kp_max_navigation as nav
import kp_max_live_rows as live_rows
import kp_max_full_search as full_search

SESSION_TTL_SECONDS = 30 * 60
PAGE_SIZE = 10
_LOCK = Lock()
_SESSIONS: dict[str, dict] = {}


def _norm(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().casefold().replace("ё", "е")


def _cb(text: str, payload: str) -> dict:
    return {"type": "callback", "text": str(text), "payload": str(payload)}


def _keyboard(rows: list[list[dict]]) -> list[dict]:
    return [{"type": "inline_keyboard", "payload": {"buttons": rows}}]


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


def _set(user_id: str, **values) -> dict:
    with _LOCK:
        current = dict(_SESSIONS.get(str(user_id)) or {})
        current.update(values)
        current["expiresAt"] = int(time.time()) + SESSION_TTL_SECONDS
        _SESSIONS[str(user_id)] = current
        return dict(current)


def search_menu() -> dict:
    return {
        "text": "ПОИСК КП\nВыбери способ поиска.",
        "attachments": _keyboard([
            [_cb("ПО НОМЕРУ", "find:number")],
            [_cb("ПО КЛИЕНТУ", "find:client")],
            [_cb("🟢🟢 ← ВЕРНУТЬСЯ НА ГЛАВНОЕ МЕНЮ", "nav:root")],
            [_cb("🟢 ← ВЕРНУТЬСЯ НА УРОВЕНЬ ВЫШЕ", "nav:root")],
        ]),
    }


def start(user_id: str, mode: str) -> dict:
    if mode not in {"number", "client"}:
        raise ValueError("invalid search mode")
    _set(user_id, mode=mode, query="", page=0)
    if mode == "number":
        text = "ПОИСК КП ПО НОМЕРУ\nВведи номер КП или его часть. Например: 695 или 69."
    else:
        text = "ПОИСК КП ПО КЛИЕНТУ\nВведи часть названия клиента. Например: АЛЬФА ПАРТ."
    return {
        "text": text,
        "attachments": _keyboard([
            [_cb("⬅ К ВЫБОРУ ПОИСКА", "find:menu")],
            [_cb("ОТМЕНА", "find:cancel")],
        ]),
    }


def _number_value(row: dict) -> str:
    raw = str(row.get("number") or "").strip()
    match = re.search(r"(\d+)$", raw)
    if match:
        return str(int(match.group(1)))
    stripped = raw.lstrip("0")
    return stripped or "0"


def _search_number(query: str) -> list[dict]:
    q = re.sub(r"\D+", "", str(query or ""))
    q = q.lstrip("0") or ("0" if q else "")
    if not q:
        return []
    try:
        rows = live_rows.load()
    except Exception as exc:
        core.log(f"KP MAX live number search fallback: {type(exc).__name__}: {exc}")
        rows = nav.recent_rows()
    matched = [row for row in rows if q in _number_value(row)]
    def rank(row: dict) -> tuple[int, int]:
        number = _number_value(row)
        if number == q:
            r = 0
        elif number.startswith(q):
            r = 1
        else:
            r = 2
        try:
            numeric = int(number)
        except Exception:
            numeric = 0
        return r, -numeric
    return sorted(matched, key=rank)


def _search_client(query: str) -> list[dict]:
    q = _norm(query)
    if len(q) < 2:
        return []
    try:
        return full_search.search_client(query)
    except Exception as exc:
        core.log(f"KP MAX full client search fallback: {type(exc).__name__}: {exc}")
    tokens = [x for x in re.findall(r"[0-9a-zа-я]+", q) if len(x) >= 2 or x.isdigit()]
    try:
        source_rows = live_rows.load()
    except Exception:
        source_rows = nav.recent_rows()
    return [row for row in source_rows if all(t in _norm(row.get("customerName") or "") for t in tokens)]

def _results(session: dict) -> list[dict]:
    mode = str(session.get("mode") or "")
    query = str(session.get("query") or "")
    return _search_number(query) if mode == "number" else _search_client(query)


def results_menu(user_id: str, page: int | None = None) -> dict:
    session = session_get(user_id)
    if not session:
        return search_menu()
    items = _results(session)
    total_pages = max(1, (len(items) + PAGE_SIZE - 1) // PAGE_SIZE)
    current_page = int(session.get("page") or 0) if page is None else int(page)
    current_page = max(0, min(current_page, total_pages - 1))
    _set(user_id, page=current_page)
    start_at = current_page * PAGE_SIZE
    current = items[start_at:start_at + PAGE_SIZE]
    buttons: list[list[dict]] = []
    for row in current:
        number = _number_value(row)
        buttons.append([_cb(nav.kp_button_text(row), f"find:open:{number}")])
    pager: list[dict] = []
    if current_page > 0:
        pager.append(_cb("◀ ПРЕДЫДУЩИЕ", f"find:page:{current_page - 1}"))
    if current_page + 1 < total_pages:
        pager.append(_cb("СЛЕДУЮЩИЕ ▶", f"find:page:{current_page + 1}"))
    if pager:
        buttons.append(pager)
    buttons.append([_cb("🔎 НОВЫЙ ПОИСК", "find:menu")])
    buttons.append([_cb("⬅ В ГЛАВНОЕ МЕНЮ", "find:cancel")])
    mode_label = "номеру" if str(session.get("mode")) == "number" else "клиенту"
    text = f"ПОИСК КП ПО {mode_label.upper()}\nЗапрос: {session.get('query') or '—'}\nНайдено: {len(items)}. Страница {current_page + 1}/{total_pages}."
    if not current:
        text += "\nСовпадений не найдено." if str(session.get("mode")) == "client" else "\nСовпадений среди последних 300 КП нет."
    return {"text": text, "attachments": _keyboard(buttons)}


def submit(user_id: str, query: str) -> dict:
    session = session_get(user_id)
    if not session:
        return search_menu()
    mode = str(session.get("mode") or "")
    clean = str(query or "").strip()
    if mode == "number" and not re.search(r"\d", clean):
        menu = start(user_id, mode)
        menu["text"] += "\n\nВведи хотя бы одну цифру."
        return menu
    if mode == "client" and len(_norm(clean)) < 2:
        menu = start(user_id, mode)
        menu["text"] += "\n\nВведи минимум 2 символа."
        return menu
    _set(user_id, query=clean, page=0)
    return results_menu(user_id, 0)


def open_result(user_id: str, number: str) -> dict:
    normalized = str(number).lstrip("0") or "0"
    row = None
    session = session_get(user_id)
    if session:
        try:
            row = next((r for r in _results(session) if _number_value(r) == normalized), None)
        except Exception:
            row = None
    if not row:
        row = full_search.find_number(normalized)
    if not row:
        return results_menu(user_id)
    try:
        row = full_search.enrich_row(row)
    except Exception:
        row = dict(row)
    row = live_rows.inject_into_core(row)
    status = nav.workflow_status(row)
    try:
        status_idx = nav.STATUS_LABELS.index(status)
    except ValueError:
        status_idx = 0
    clear(user_id)
    return nav.kp_level3(normalized, status_idx, 0)

