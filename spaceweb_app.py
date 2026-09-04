import asyncio
import hashlib
import hmac
import json
import os
import re
import secrets
import time
from pathlib import Path
from threading import Lock

import requests
from fastapi import HTTPException, Request

import api_proxy as core
import kp_max_navigation as nav
import kp_max_customer as customer
import kp_max_search as kp_search
import kp_max_items as items
import kp_max_create as kp_create

app = core.app
KP_MAX_BOT_TOKEN = os.getenv("KP_MAX_BOT_TOKEN", "").strip()
KP_MAX_WEBHOOK_SECRET = os.getenv("KP_MAX_WEBHOOK_SECRET", "").strip()
KP_MAX_CA_BUNDLE = os.getenv("KP_MAX_CA_BUNDLE", "").strip()
KP_MAX_ACCESS_FILE = Path(os.getenv("KP_MAX_ACCESS_FILE", "/opt/kp-api/data/kp_max_access.json"))
INVITE_TTL_SECONDS = 24 * 60 * 60
EDIT_TTL_SECONDS = 30 * 60
MAX_COMMENT_CHARS = 20000
_ACCESS_LOCK = Lock()
_EDIT_LOCK = Lock()
_EDIT_SESSIONS: dict[str, dict] = {}
_CODE_ALPHABET = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"


def _max_verify():
    return KP_MAX_CA_BUNDLE if KP_MAX_CA_BUNDLE and Path(KP_MAX_CA_BUNDLE).is_file() else True


def _message_context(payload: dict) -> tuple[str, str, str, bool, str]:
    message = payload.get("message") if isinstance(payload.get("message"), dict) else {}
    body = message.get("body") if isinstance(message.get("body"), dict) else {}
    recipient = message.get("recipient") if isinstance(message.get("recipient"), dict) else {}
    sender = message.get("sender") if isinstance(message.get("sender"), dict) else {}
    text = str(body.get("text") or message.get("text") or "").strip()
    chat_id = str(recipient.get("chat_id") or message.get("chat_id") or payload.get("chat_id") or "").strip()
    sender_id = str(sender.get("user_id") or sender.get("id") or "").strip()
    chat_type = str(recipient.get("chat_type") or recipient.get("type") or "").strip().lower()
    return text, chat_id, sender_id, bool(sender.get("is_bot")), chat_type


def _kp_number(text: str) -> str:
    match = re.fullmatch(r"(?:КП|KP)\s*(?:№\s*)?0*(\d{1,12})", str(text or "").strip(), flags=re.IGNORECASE)
    return match.group(1) if match else ""


def _comment_number(text: str) -> str:
    match = re.fullmatch(
        r"(?:КОММЕНТАРИЙ|КОМ|COMMENT)\s*(?:КП\s*)?(?:№\s*)?0*(\d{1,12})",
        str(text or "").strip(), flags=re.IGNORECASE,
    )
    return match.group(1) if match else ""


def _edit_comment_number(text: str) -> str:
    match = re.fullmatch(
        r"(?:РЕДКОМ|РЕДКОММЕНТАРИЙ|EDITCOMMENT)\s*(?:КП\s*)?(?:№\s*)?0*(\d{1,12})",
        str(text or "").strip(), flags=re.IGNORECASE,
    )
    return match.group(1) if match else ""


def _normalized_comment(value: str) -> str:
    return str(value or "").replace("\r\n", "\n").replace("\r", "\n")


def _comment_hash(value: str) -> str:
    return hashlib.sha256(_normalized_comment(value).encode("utf-8")).hexdigest()


def _find_kp_target(number: str) -> tuple[dict, str]:
    target = next(
        (row for row in core._cached_rows if core._normalize_kp_number(row.get("number") or "") == number),
        None,
    )
    if not target:
        raise HTTPException(status_code=404, detail=f"КП {number} не найдено")
    ref_key = str(target.get("refKey") or target.get("Ref_Key") or "").strip()
    if not ref_key:
        raise RuntimeError(f"KP {number} has no refKey")
    return target, ref_key

def _fetch_comment_raw_by_ref(ref_key: str) -> str:
    base = str(core.BASE).strip().strip('\"').strip("'").rstrip("/")
    response = requests.get(
        f"{base}/{core.ENTITY}(guid'{ref_key}')",
        headers=core._build_headers(),
        params={"$select": "Number,Комментарий"},
        timeout=30,
    )
    if response.status_code != 200:
        raise RuntimeError(f"1C OData HTTP {response.status_code}")
    return str(response.json().get("Комментарий") or "")


def _comment_display(raw: str) -> str:
    text = core.strip_html(str(raw or ""))
    return _normalized_comment(text).strip() or "Комментарий не заполнен."


def _build_full_comment_text(number: str) -> str:
    _, ref_key = _find_kp_target(number)
    raw = _fetch_comment_raw_by_ref(ref_key)
    return f"Полный комментарий КП №{number}:\n{_comment_display(raw)}"


def _edit_session_get(user_id: str) -> dict | None:
    now = int(time.time())
    with _EDIT_LOCK:
        session = _EDIT_SESSIONS.get(str(user_id))
        if not session:
            return None
        if int(session.get("expiresAt") or 0) <= now:
            _EDIT_SESSIONS.pop(str(user_id), None)
            return None
        return dict(session)


def _edit_session_clear(user_id: str) -> None:
    with _EDIT_LOCK:
        _EDIT_SESSIONS.pop(str(user_id), None)


def _start_comment_edit(user_id: str, number: str) -> str:
    _, ref_key = _find_kp_target(number)
    raw = _fetch_comment_raw_by_ref(ref_key)
    with _EDIT_LOCK:
        _EDIT_SESSIONS[str(user_id)] = {
            "number": number,
            "refKey": ref_key,
            "originalHash": _comment_hash(raw),
            "stage": "await_text",
            "expiresAt": int(time.time()) + EDIT_TTL_SECONDS,
        }
    return _comment_display(raw)


def _set_comment_proposal(user_id: str, new_text: str) -> dict:
    if len(new_text) > MAX_COMMENT_CHARS:
        raise ValueError(f"Комментарий слишком длинный: максимум {MAX_COMMENT_CHARS} символов")
    with _EDIT_LOCK:
        session = _EDIT_SESSIONS.get(str(user_id))
        if not session or int(session.get("expiresAt") or 0) <= int(time.time()):
            _EDIT_SESSIONS.pop(str(user_id), None)
            raise RuntimeError("edit session expired")
        session["newText"] = new_text
        session["stage"] = "confirm"
        session["expiresAt"] = int(time.time()) + EDIT_TTL_SECONDS
        return dict(session)

def _update_comment_memory_cache(number: str, new_text: str) -> None:
    target = next(
        (row for row in core._cached_rows if core._normalize_kp_number(row.get("number") or "") == number),
        None,
    )
    if not target:
        return
    clean = _normalized_comment(core.strip_html(new_text))
    upper = clean.upper()
    top = upper.split("\n")[:5]
    target["additionalInfoFirstLine"] = core.first_line(new_text) or ""
    target["kpSent"] = any("КП ОТПРАВЛЕНО" in line for line in top)
    target["receiptConfirmed"] = any("КЛИЕНТ КП УВИДЕЛ" in line for line in top)
    target["edoSent"] = "В ЭДО ОТПРАВЛЕНО" in upper
    target["rejected"] = "ОТКАЗ" in upper
    target["problem"] = "ПРОБЛЕМА" in upper
    target["shipmentPending"] = "ОТГРУЗИТЬ" in upper


def _audit_comment_edit(user_id: str, role: str, number: str, old_hash: str, new_text: str) -> None:
    path = KP_MAX_ACCESS_FILE.parent / "kp_max_edit_audit.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts": int(time.time()), "userId": str(user_id), "role": str(role), "kp": str(number),
        "oldHash": old_hash, "newHash": _comment_hash(new_text), "newChars": len(new_text),
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def _commit_comment_edit(user_id: str, role: str) -> dict:
    session = _edit_session_get(user_id)
    if not session or session.get("stage") != "confirm":
        raise RuntimeError("no pending edit")
    number = str(session["number"])
    ref_key = str(session["refKey"])
    new_text = str(session.get("newText") or "")
    current_raw = _fetch_comment_raw_by_ref(ref_key)
    if _comment_hash(current_raw) != str(session.get("originalHash") or ""):
        _edit_session_clear(user_id)
        raise RuntimeError("comment changed concurrently")
    base = str(core.BASE).strip().strip('\"').strip("'").rstrip("/")
    response = requests.patch(
        f"{base}/{core.ENTITY}(guid'{ref_key}')",
        headers={**core._build_headers(), "Content-Type": "application/json; charset=utf-8"},
        json={"Комментарий": new_text}, timeout=30,
    )
    if response.status_code not in {200, 204}:
        raise RuntimeError(f"1C PATCH HTTP {response.status_code}")
    verified = _fetch_comment_raw_by_ref(ref_key)
    if _normalized_comment(verified) != _normalized_comment(new_text):
        raise RuntimeError("1C write verification failed")
    _update_comment_memory_cache(number, new_text)
    _audit_comment_edit(user_id, role, number, str(session.get("originalHash") or ""), new_text)
    _edit_session_clear(user_id)
    return {"number": number, "chars": len(new_text)}

def _build_kp_text(number: str) -> str:
    target = next(
        (row for row in core._cached_rows if core._normalize_kp_number(row.get("number") or "") == number),
        None,
    )
    if not target:
        raise HTTPException(status_code=404, detail=f"КП {number} не найдено")
    row = core.format_row_for_client(target)
    yes_no = lambda value: "да" if bool(value) else "нет"
    lines = [
        f"КП №{row.get('number') or number}",
        f"Дата: {row.get('createdAt') or '—'}",
        f"Покупатель: {row.get('customerName') or '—'}",
        f"Менеджер: {row.get('managerName') or '—'}",
        f"Статус 1С: {row.get('status') or '—'}",
        f"Товар указан: {yes_no(row.get('productSpecified'))}",
        f"Цена заполнена: {yes_no(row.get('priceFilled'))}",
        f"Клиент увидел КП: {yes_no(row.get('receiptConfirmed'))}",
    ]
    comment = str(row.get("additionalInfoFirstLine") or "").strip()
    if comment:
        lines.append(f"Комментарий: {comment}")
    return "\n".join(lines)


def _send_message(chat_id: str, text: str) -> dict:
    if not KP_MAX_BOT_TOKEN:
        raise RuntimeError("KP_MAX_BOT_TOKEN is not configured")
    response = requests.post(
        "https://platform-api2.max.ru/messages",
        params={"chat_id": str(chat_id)},
        headers={
            "Authorization": KP_MAX_BOT_TOKEN,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json={"text": str(text)},
        timeout=20,
        verify=_max_verify(),
    )
    try:
        data = response.json()
    except Exception:
        data = {"raw": response.text[:500]}
    if not 200 <= response.status_code < 300:
        raise RuntimeError(f"MAX send HTTP {response.status_code}: {str(data)[:500]}")
    return {"status": response.status_code, "response": data}


def _send_menu_message(chat_id: str, menu: dict) -> dict:
    response = requests.post(
        "https://platform-api2.max.ru/messages",
        params={"chat_id": str(chat_id)},
        headers={"Authorization": KP_MAX_BOT_TOKEN, "Content-Type": "application/json", "Accept": "application/json"},
        json={"text": str(menu.get("text") or ""), "attachments": list(menu.get("attachments") or [])},
        timeout=20, verify=_max_verify(),
    )
    data = response.json() if response.content else {}
    if not 200 <= response.status_code < 300:
        raise RuntimeError(f"MAX menu HTTP {response.status_code}: {str(data)[:500]}")
    return {"status": response.status_code, "response": data}


def _answer_callback(callback_id: str, menu: dict) -> dict:
    response = requests.post(
        "https://platform-api2.max.ru/answers",
        params={"callback_id": str(callback_id)},
        headers={"Authorization": KP_MAX_BOT_TOKEN, "Content-Type": "application/json", "Accept": "application/json"},
        json={"message": {"text": str(menu.get("text") or ""), "attachments": list(menu.get("attachments") or [])}},
        timeout=20, verify=_max_verify(),
    )
    data = response.json() if response.content else {}
    if response.status_code != 200 or (isinstance(data, dict) and data.get("success") is False):
        raise RuntimeError(f"MAX callback HTTP {response.status_code}: {str(data)[:500]}")
    return data


def _get_chat_type(chat_id: str) -> str:
    if not KP_MAX_BOT_TOKEN or not chat_id:
        return ""
    response = requests.get(
        f"https://platform-api2.max.ru/chats/{chat_id}",
        headers={"Authorization": KP_MAX_BOT_TOKEN, "Accept": "application/json"},
        timeout=12,
        verify=_max_verify(),
    )
    if response.status_code != 200:
        return ""
    try:
        data = response.json()
    except Exception:
        return ""
    return str(data.get("type") or "").strip().lower()


def _default_access_state() -> dict:
    return {"version": 1, "admins": [], "users": {}, "invites": {}, "bootstrap": None}


def _load_access_unlocked() -> dict:
    if not KP_MAX_ACCESS_FILE.exists():
        return _default_access_state()
    try:
        state = json.loads(KP_MAX_ACCESS_FILE.read_text(encoding="utf-8"))
    except Exception:
        core.log("KP MAX access: invalid state file, fail closed")
        return _default_access_state()
    if not isinstance(state, dict):
        return _default_access_state()
    state.setdefault("version", 1)
    state["admins"] = [str(x) for x in state.get("admins", []) if str(x)]
    state["users"] = state.get("users", {}) if isinstance(state.get("users"), dict) else {}
    state["invites"] = state.get("invites", {}) if isinstance(state.get("invites"), dict) else {}
    if state.get("bootstrap") is not None and not isinstance(state.get("bootstrap"), dict):
        state["bootstrap"] = None
    return state


def _save_access_unlocked(state: dict) -> None:
    KP_MAX_ACCESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = KP_MAX_ACCESS_FILE.with_suffix(KP_MAX_ACCESS_FILE.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    os.chmod(tmp, 0o600)
    tmp.replace(KP_MAX_ACCESS_FILE)


def _normalize_code(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", str(text or "")).upper()


def _hash_code(code: str) -> str:
    return hashlib.sha256(_normalize_code(code).encode("ascii")).hexdigest()


def _new_code() -> str:
    raw = "".join(secrets.choice(_CODE_ALPHABET) for _ in range(8))
    return raw[:4] + "-" + raw[4:]


def _prune_invites(state: dict, now: int) -> None:
    state["invites"] = {
        key: value for key, value in state.get("invites", {}).items()
        if isinstance(value, dict) and int(value.get("expires_at") or 0) >= now
    }


def _role_for_user(user_id: str) -> str:
    with _ACCESS_LOCK:
        state = _load_access_unlocked()
        if user_id in state["admins"]:
            return "admin"
        if user_id in state["users"]:
            return "user"
        return ""


def _consume_activation_code(user_id: str, text: str) -> str:
    code = _normalize_code(text)
    if len(code) != 8:
        return ""
    digest = _hash_code(code)
    now = int(time.time())
    with _ACCESS_LOCK:
        state = _load_access_unlocked()
        _prune_invites(state, now)
        bootstrap = state.get("bootstrap")
        if not state["admins"] and isinstance(bootstrap, dict):
            if int(bootstrap.get("expires_at") or 0) >= now and hmac.compare_digest(str(bootstrap.get("hash") or ""), digest):
                state["admins"].append(user_id)
                state["bootstrap"] = None
                _save_access_unlocked(state)
                return "admin"
        invite = state["invites"].get(digest)
        if isinstance(invite, dict) and int(invite.get("expires_at") or 0) >= now:
            state["invites"].pop(digest, None)
            state["users"][user_id] = {"added_at": now, "added_by": str(invite.get("created_by") or "")}
            _save_access_unlocked(state)
            return "user"
        _save_access_unlocked(state)
    return ""


def _create_invite(admin_id: str) -> str:
    now = int(time.time())
    code = _new_code()
    digest = _hash_code(code)
    with _ACCESS_LOCK:
        state = _load_access_unlocked()
        if admin_id not in state["admins"]:
            raise PermissionError("admin required")
        _prune_invites(state, now)
        state["invites"][digest] = {
            "created_by": admin_id,
            "created_at": now,
            "expires_at": now + INVITE_TTL_SECONDS,
        }
        _save_access_unlocked(state)
    return code


def _revoke_user(admin_id: str, target_user_id: str) -> bool:
    with _ACCESS_LOCK:
        state = _load_access_unlocked()
        if admin_id not in state["admins"]:
            raise PermissionError("admin required")
        existed = target_user_id in state["users"]
        state["users"].pop(target_user_id, None)
        _save_access_unlocked(state)
        return existed


def _access_counts() -> tuple[int, int, int]:
    now = int(time.time())
    with _ACCESS_LOCK:
        state = _load_access_unlocked()
        _prune_invites(state, now)
        _save_access_unlocked(state)
        return len(state["admins"]), len(state["users"]), len(state["invites"])


@app.get("/api/max/kp-bot/status")
async def kp_max_bot_status():
    admins, users, invites = _access_counts()
    return {
        "ok": bool(KP_MAX_BOT_TOKEN and KP_MAX_WEBHOOK_SECRET),
        "tokenConfigured": bool(KP_MAX_BOT_TOKEN),
        "secretConfigured": bool(KP_MAX_WEBHOOK_SECRET),
        "caConfigured": bool(KP_MAX_CA_BUNDLE and Path(KP_MAX_CA_BUNDLE).is_file()),
        "accessConfigured": bool(admins),
        "accessAdmins": admins,
        "accessUsers": users,
        "activeInvites": invites,
        "webhook": "/api/max/kp-bot/webhook",
        "testCommand": "КП 588",
    }


async def _reply(chat_id: str, text: str) -> dict:
    return await asyncio.to_thread(_send_message, chat_id, text)


async def _reply_menu(chat_id: str, menu: dict) -> dict:
    return await asyncio.to_thread(_send_menu_message, chat_id, menu)


def _callback_context(payload: dict) -> tuple[str, str, str, str, str]:
    callback = payload.get("callback") if isinstance(payload.get("callback"), dict) else {}
    message = payload.get("message") if isinstance(payload.get("message"), dict) else {}
    recipient = message.get("recipient") if isinstance(message.get("recipient"), dict) else {}
    user = callback.get("user") if isinstance(callback.get("user"), dict) else {}
    if not user and isinstance(payload.get("user"), dict):
        user = payload.get("user")
    callback_id = str(callback.get("callback_id") or "").strip()
    action = str(callback.get("payload") or "").strip()
    chat_id = str(recipient.get("chat_id") or message.get("chat_id") or payload.get("chat_id") or "").strip()
    sender_id = str(user.get("user_id") or user.get("id") or "").strip()
    chat_type = str(recipient.get("chat_type") or recipient.get("type") or "").strip().lower()
    return callback_id, action, chat_id, sender_id, chat_type


async def _reply_long(chat_id: str, text: str, chunk_size: int = 3500) -> None:
    remaining = str(text or "")
    while remaining:
        if len(remaining) <= chunk_size:
            chunk, remaining = remaining, ""
        else:
            cut = remaining.rfind("\n", 0, chunk_size)
            if cut < chunk_size // 2:
                cut = chunk_size
            chunk, remaining = remaining[:cut], remaining[cut:].lstrip("\n")
        await _reply(chat_id, chunk)


async def _handle_navigation_callback(payload: dict) -> dict:
    callback_id, action, chat_id, sender_id, chat_type = _callback_context(payload)
    if not callback_id or not action or not chat_id or not sender_id:
        return {"ok": True, "ignored": "callback-identity"}
    role = _role_for_user(sender_id)
    if not role:
        denied = {"text": "Нет доступа. Введи одноразовый код активации в личном чате с ботом.", "attachments": []}
        await asyncio.to_thread(_answer_callback, callback_id, denied)
        return {"ok": True, "denied": "access"}
    if not chat_type:
        chat_type = await asyncio.to_thread(_get_chat_type, chat_id)
    if chat_type != "dialog":
        denied = {"text": "Данные КП доступны только в личном диалоге с ботом.", "attachments": []}
        await asyncio.to_thread(_answer_callback, callback_id, denied)
        return {"ok": True, "denied": "not-dialog"}
    if _edit_session_get(sender_id):
        blocked = {"text": "Сначала заверши редактирование комментария: СОХРАНИТЬ или ОТМЕНА.", "attachments": []}
        await asyncio.to_thread(_answer_callback, callback_id, blocked)
        return {"ok": True, "denied": "edit-session"}
    customer_session = customer.session_get(sender_id)
    if customer_session and not action.startswith("cust:"):
        blocked = {"text": "Сначала заверши выбор клиента: СОХРАНИТЬ или ОТМЕНА.", "attachments": []}
        await asyncio.to_thread(_answer_callback, callback_id, blocked)
        return {"ok": True, "denied": "customer-session"}
    search_session = kp_search.session_get(sender_id)
    if search_session and not action.startswith("find:"):
        blocked = {"text": "Сначала заверши поиск КП или нажми ОТМЕНА.", "attachments": []}
        await asyncio.to_thread(_answer_callback, callback_id, blocked)
        return {"ok": True, "denied": "search-session"}
    item_session = items.session_get(sender_id)
    if item_session and not action.startswith("itm:"):
        blocked = {"text": "Сначала заверши редактирование строки товара: СОХРАНИТЬ или ОТМЕНА.", "attachments": []}
        await asyncio.to_thread(_answer_callback, callback_id, blocked)
        return {"ok": True, "denied": "item-session"}
    try:
        if action == "find:menu":
            kp_search.clear(sender_id)
            menu = kp_search.search_menu()
        elif action == "find:number":
            menu = kp_search.start(sender_id, "number")
        elif action == "find:client":
            menu = kp_search.start(sender_id, "client")
        elif action == "find:cancel":
            kp_search.clear(sender_id)
            menu = nav.root_menu(role)
        elif action.startswith("find:page:"):
            page = int(action.split(":", 2)[2])
            menu = kp_search.results_menu(sender_id, page)
        elif action.startswith("find:open:"):
            number = action.split(":", 2)[2]
            menu = kp_search.open_result(sender_id, number)
        elif action == "nav:root":
            menu = nav.root_menu(role)
        elif action == "nav:create":
            menu, created = await asyncio.to_thread(kp_create.create_and_menu, sender_id, role)
            core.log(f"KP MAX created: KP {created['number']}, user={sender_id}, role={role}")
        elif action == "nav:statuses":
            menu = nav.statuses_menu()
        elif action == "nav:access":
            admins, users, invites = _access_counts()
            menu = nav.root_menu(role)
            menu["text"] = (
                f"Доступ активен. Роль: {'администратор' if role == 'admin' else 'сотрудник'}.\n"
                f"Твой MAX user_id: {sender_id}\n"
                f"Администраторов: {admins}; сотрудников: {users}; кодов: {invites}."
            )
        elif action == "nav:invite":
            if role != "admin":
                menu = nav.root_menu(role)
                menu["text"] = "Выдавать коды может только администратор."
            else:
                code = _create_invite(sender_id)
                menu = nav.root_menu(role)
                menu["text"] = f"Одноразовый код сотрудника: {code}\nДействует 24 часа и сгорает после использования."
        elif action.startswith("nav:s:"):
            _, _, key, page = action.split(":", 3)
            menu = nav.status_page(nav.status_index(key), int(page))
        elif action.startswith("nav:k:"):
            _, _, number, key, page = action.split(":", 4)
            menu = nav.kp_level3(number, nav.status_index(key), int(page))
        elif action.startswith("nav:invoice:"):
            _, _, number, key, page = action.split(":", 4)
            menu = nav.invoice_menu(number, nav.status_index(key), int(page))
        elif action.startswith("nav:f:"):
            _, _, field, number, key, page = action.split(":", 5)
            status_idx = nav.status_index(key)
            page_num = int(page)
            if field == "client":
                menu = await asyncio.to_thread(customer.start, sender_id, number, status_idx, page_num)
            elif field == "items":
                menu = await asyncio.to_thread(items.list_menu, number, status_idx, page_num, 0)
            elif field == "comment":
                _, ref_key = _find_kp_target(number)
                raw_comment = await asyncio.to_thread(_fetch_comment_raw_by_ref, ref_key)
                comment_text = _comment_display(raw_comment)
                overflow = len(comment_text) > 3000
                menu = nav.comment_menu(number, status_idx, page_num, comment_text, overflow=overflow)
                await asyncio.to_thread(_answer_callback, callback_id, menu)
                if overflow:
                    await _reply_long(chat_id, f"Полный комментарий КП {number}:\n{comment_text}")
                return {"ok": True, "handled": action}
            else:
                menu = nav.field_placeholder(field, number, status_idx, page_num)
        elif action.startswith("nav:ce:"):
            _, _, number, key, page = action.split(":", 4)
            await asyncio.to_thread(_start_comment_edit, sender_id, number)
            menu = nav.comment_edit_started_menu(number)
        elif action.startswith("itm:addrow:"):
            _, _, number, key, status_page, item_page = action.split(":", 5)
            menu = await asyncio.to_thread(items.start_add, sender_id, number, nav.status_index(key), int(status_page), int(item_page))
        elif action.startswith("itm:addprod:"):
            product_key = action.split(":", 2)[2]
            menu = await asyncio.to_thread(items.pick_add_product, sender_id, product_key)
        elif action == "itm:addrestart":
            menu = items.restart_add(sender_id)
        elif action == "itm:addcancel":
            menu = await asyncio.to_thread(items.cancel_add_menu, sender_id)
        elif action == "itm:addsave":
            try:
                menu, saved = await asyncio.to_thread(items.commit_add, sender_id, role)
                core.log(f"KP MAX item added: KP {saved['number']}, line={saved['line']}, user={sender_id}")
            except RuntimeError as exc:
                if "concurrently" in str(exc):
                    menu = nav.root_menu(role)
                    menu["text"] = "Строки товара изменились в 1С после начала добавления. Добавление отменено."
                else:
                    core.log(f"KP MAX item add failed: {type(exc).__name__}: {exc}")
                    menu = items._add_confirm_menu(sender_id) if items.session_get(sender_id) else nav.root_menu(role)
                    menu["text"] = "Не удалось добавить строку в 1С.\n\n" + menu["text"]
        elif action.startswith("itm:list:"):
            _, _, number, key, status_page, item_page = action.split(":", 5)
            menu = await asyncio.to_thread(items.list_menu, number, nav.status_index(key), int(status_page), int(item_page))
        elif action.startswith("itm:open:"):
            _, _, number, line, key, status_page, item_page = action.split(":", 6)
            menu = await asyncio.to_thread(items.item_menu, number, int(line), nav.status_index(key), int(status_page), int(item_page))
        elif action.startswith("itm:view:"):
            _, _, field, number, line, key, status_page, item_page = action.split(":", 7)
            menu = await asyncio.to_thread(items.field_menu, field, number, int(line), nav.status_index(key), int(status_page), int(item_page))
        elif action.startswith("itm:edit:"):
            _, _, field, number, line, key, status_page, item_page = action.split(":", 7)
            menu = await asyncio.to_thread(items.start_edit, sender_id, field, number, int(line), nav.status_index(key), int(status_page), int(item_page))
        elif action.startswith("itm:prod:"):
            product_key = action.split(":", 2)[2]
            menu = await asyncio.to_thread(items.pick_product, sender_id, product_key)
        elif action == "itm:again":
            menu = items.again(sender_id)
        elif action == "itm:cancel":
            menu = await asyncio.to_thread(items.cancel_menu, sender_id)
        elif action == "itm:save":
            try:
                menu, saved = await asyncio.to_thread(items.commit, sender_id, role)
                core.log(f"KP MAX item saved: KP {saved['number']}, line={saved['line']}, field={saved['field']}, user={sender_id}")
            except RuntimeError as exc:
                if "concurrently" in str(exc):
                    menu = nav.root_menu(role)
                    menu["text"] = "Строка товара изменилась в 1С после начала редактирования. Запись отменена."
                else:
                    core.log(f"KP MAX item save failed: {type(exc).__name__}: {exc}")
                    menu = items._confirm_menu(sender_id) if items.session_get(sender_id) else nav.root_menu(role)
                    menu["text"] = "Не удалось сохранить изменение строки в 1С.\n\n" + menu["text"]
        elif action.startswith("cust:x:"):
            counterparty_key = action.split(":", 2)[2]
            menu = await asyncio.to_thread(customer.pick_counterparty_direct, sender_id, counterparty_key)
        elif action.startswith("cust:p:"):
            partner_key = action.split(":", 2)[2]
            menu = await asyncio.to_thread(customer.pick_partner, sender_id, partner_key)
        elif action.startswith("cust:c:"):
            counterparty_key = action.split(":", 2)[2]
            menu = await asyncio.to_thread(customer.pick_counterparty, sender_id, counterparty_key)
        elif action == "cust:again":
            menu = customer.again(sender_id)
        elif action == "cust:cancel":
            menu = customer.cancel_menu(sender_id)
        elif action == "cust:save":
            location = customer.session_get(sender_id)
            try:
                menu, saved = await asyncio.to_thread(customer.commit, sender_id, role)
                core.log(f"KP MAX customer saved: KP {saved['number']}, user={sender_id}, role={role}")
            except RuntimeError as exc:
                if "concurrently" in str(exc) and location:
                    menu = nav.kp_level3(str(location.get("number") or ""), int(location.get("statusIdx") or 0), int(location.get("page") or 0))
                    menu["text"] = "Клиент в 1С изменился после начала выбора. Запись отменена.\n\n" + menu["text"]
                else:
                    core.log(f"KP MAX customer save failed: {type(exc).__name__}: {exc}")
                    menu = customer.confirm_menu(sender_id) if customer.session_get(sender_id) else nav.root_menu(role)
                    menu["text"] = "Не удалось сохранить клиента в 1С. Попробуй ещё раз или отмени изменение.\n\n" + menu["text"]
        else:
            menu = nav.root_menu(role)
        await asyncio.to_thread(_answer_callback, callback_id, menu)
        return {"ok": True, "handled": action}
    except Exception as exc:
        core.log(f"KP MAX navigation callback failed: {type(exc).__name__}: {exc}")
        fallback = nav.root_menu(role)
        fallback["text"] = "Не удалось открыть этот раздел. Вернулся в главное меню."
        try:
            await asyncio.to_thread(_answer_callback, callback_id, fallback)
        except Exception:
            pass
        return {"ok": True, "error": "navigation-callback"}


@app.post("/api/max/kp-bot/webhook")
async def kp_max_bot_webhook(request: Request):
    provided_secret = str(request.headers.get("X-Max-Bot-Api-Secret") or "")
    if not KP_MAX_WEBHOOK_SECRET or not hmac.compare_digest(provided_secret, KP_MAX_WEBHOOK_SECRET):
        raise HTTPException(status_code=401, detail="Invalid MAX webhook secret")
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid MAX update payload")
    update_type = str(payload.get("update_type") or "")
    if update_type == "message_callback":
        return await _handle_navigation_callback(payload)
    if update_type != "message_created":
        return {"ok": True, "ignored": "update_type"}

    text, chat_id, sender_id, sender_is_bot, chat_type = _message_context(payload)
    if sender_is_bot:
        return {"ok": True, "ignored": "bot_message"}
    if not chat_id or not sender_id:
        return {"ok": True, "ignored": "identity"}

    if not chat_type:
        chat_type = await asyncio.to_thread(_get_chat_type, chat_id)
    if chat_type != "dialog":
        await _reply(chat_id, "Данные КП доступны только в личном диалоге с ботом.")
        return {"ok": True, "denied": "not_dialog"}

    role = _role_for_user(sender_id)
    upper = text.strip().upper()

    if not role:
        activated = _consume_activation_code(sender_id, text)
        if activated:
            await _reply(
                chat_id,
                "Доступ активирован. Ты администратор." if activated == "admin"
                else "Доступ активирован. Теперь можно запрашивать КП.",
            )
            core.log(f"KP MAX access activated: role={activated}, user={sender_id}")
            return {"ok": True, "handled": "access-activated"}
        await _reply(chat_id, "Нет доступа. Введи одноразовый код активации, выданный администратором.")
        return {"ok": True, "denied": "access"}

    item_session = items.session_get(sender_id)
    if item_session:
        stage = str(item_session.get("stage") or "")
        if item_session.get("mode") == "add":
            if upper in {"ОТМЕНА", "CANCEL"}:
                await _reply_menu(chat_id, await asyncio.to_thread(items.cancel_add_menu, sender_id))
                return {"ok": True, "handled": "item-add-cancel"}
            if stage == "add_product_query":
                try:
                    menu = await asyncio.to_thread(items.add_product_search_menu, sender_id, text)
                    await _reply_menu(chat_id, menu)
                    return {"ok": True, "handled": "item-add-product-search"}
                except Exception as exc:
                    core.log(f"KP MAX add product search failed: {type(exc).__name__}: {exc}")
                    await _reply(chat_id, "Не удалось выполнить поиск номенклатуры. Попробуй другой фрагмент или отправь ОТМЕНА.")
                    return {"ok": True, "error": "item-add-product-search"}
            if stage in {"add_qty", "add_price"}:
                try:
                    menu = items.set_add_value(sender_id, text)
                    await _reply_menu(chat_id, menu)
                    return {"ok": True, "handled": "item-add-value"}
                except ValueError as exc:
                    await _reply(chat_id, str(exc))
                    return {"ok": True, "error": "item-add-value-invalid"}
            if stage == "add_confirm":
                if upper in {"СОХРАНИТЬ", "SAVE"}:
                    try:
                        menu, saved = await asyncio.to_thread(items.commit_add, sender_id, role)
                        await _reply_menu(chat_id, menu)
                        core.log(f"KP MAX item added: KP {saved['number']}, line={saved['line']}, user={sender_id}")
                        return {"ok": True, "handled": "item-add-saved"}
                    except RuntimeError as exc:
                        core.log(f"KP MAX item add save failed: {type(exc).__name__}: {exc}")
                        await _reply(chat_id, "Не удалось добавить строку в 1С.")
                        return {"ok": True, "error": "item-add-save"}
                await _reply_menu(chat_id, items._add_confirm_menu(sender_id))
                return {"ok": True, "handled": "item-add-await-save"}
            await _reply(chat_id, "Заверши добавление строки или отправь ОТМЕНА.")
            return {"ok": True, "handled": "item-add-await"}
        if upper in {"ОТМЕНА", "CANCEL"}:
            await _reply_menu(chat_id, await asyncio.to_thread(items.cancel_menu, sender_id))
            return {"ok": True, "handled": "item-edit-cancel"}
        if stage == "await_value":
            try:
                menu = items.set_value(sender_id, text)
                await _reply_menu(chat_id, menu)
                return {"ok": True, "handled": "item-edit-value"}
            except ValueError as exc:
                await _reply(chat_id, str(exc))
                return {"ok": True, "error": "item-value-invalid"}
        if stage == "await_product_query":
            try:
                menu = await asyncio.to_thread(items.product_search_menu, sender_id, text)
                await _reply_menu(chat_id, menu)
                return {"ok": True, "handled": "item-product-search"}
            except Exception as exc:
                core.log(f"KP MAX product search failed: {type(exc).__name__}: {exc}")
                await _reply(chat_id, "Не удалось выполнить поиск номенклатуры. Попробуй другой фрагмент или отправь ОТМЕНА.")
                return {"ok": True, "error": "item-product-search"}
        if stage == "confirm":
            if upper in {"СОХРАНИТЬ", "SAVE"}:
                try:
                    menu, saved = await asyncio.to_thread(items.commit, sender_id, role)
                    await _reply_menu(chat_id, menu)
                    core.log(f"KP MAX item saved: KP {saved['number']}, line={saved['line']}, field={saved['field']}, user={sender_id}")
                    return {"ok": True, "handled": "item-edit-saved"}
                except RuntimeError as exc:
                    core.log(f"KP MAX item text save failed: {type(exc).__name__}: {exc}")
                    await _reply(chat_id, "Не удалось сохранить изменение строки в 1С.")
                    return {"ok": True, "error": "item-edit-save"}
            await _reply_menu(chat_id, items._confirm_menu(sender_id))
            return {"ok": True, "handled": "item-await-save"}
        await _reply(chat_id, "Заверши редактирование строки кнопкой или отправь ОТМЕНА.")
        return {"ok": True, "handled": "item-await-button"}

    customer_session = customer.session_get(sender_id)
    if customer_session:
        if upper in {"ОТМЕНА", "CANCEL"}:
            await _reply_menu(chat_id, customer.cancel_menu(sender_id))
            return {"ok": True, "handled": "customer-edit-cancel"}
        stage = str(customer_session.get("stage") or "")
        if stage == "await_query":
            try:
                menu = await asyncio.to_thread(customer.search_menu, sender_id, text)
                await _reply_menu(chat_id, menu)
                return {"ok": True, "handled": "customer-search"}
            except Exception as exc:
                core.log(f"KP MAX customer search failed: {type(exc).__name__}: {exc}")
                await _reply(chat_id, "Не удалось выполнить поиск клиентов в 1С. Попробуй другой фрагмент названия или отправь ОТМЕНА.")
                return {"ok": True, "error": "customer-search"}
        if stage == "confirm":
            if upper in {"СОХРАНИТЬ", "SAVE"}:
                location = customer_session
                try:
                    menu, saved = await asyncio.to_thread(customer.commit, sender_id, role)
                    await _reply_menu(chat_id, menu)
                    core.log(f"KP MAX customer saved: KP {saved['number']}, user={sender_id}, role={role}")
                    return {"ok": True, "handled": f"customer-saved-{saved['number']}"}
                except RuntimeError as exc:
                    if "concurrently" in str(exc):
                        menu = nav.kp_level3(str(location.get("number") or ""), int(location.get("statusIdx") or 0), int(location.get("page") or 0))
                        menu["text"] = "Клиент в 1С изменился после начала выбора. Запись отменена.\n\n" + menu["text"]
                        await _reply_menu(chat_id, menu)
                    else:
                        core.log(f"KP MAX customer save failed: {type(exc).__name__}: {exc}")
                        await _reply_menu(chat_id, customer.confirm_menu(sender_id))
                    return {"ok": True, "error": "customer-save"}
            await _reply_menu(chat_id, customer.confirm_menu(sender_id))
            return {"ok": True, "handled": "customer-await-save"}
        await _reply(chat_id, "Выбери вариант кнопкой или отправь ОТМЕНА.")
        return {"ok": True, "handled": "customer-await-button"}

    search_session = kp_search.session_get(sender_id)
    if search_session:
        if upper in {"ОТМЕНА", "CANCEL"}:
            kp_search.clear(sender_id)
            await _reply_menu(chat_id, nav.root_menu(role))
            return {"ok": True, "handled": "kp-search-cancel"}
        menu = kp_search.submit(sender_id, text)
        await _reply_menu(chat_id, menu)
        return {"ok": True, "handled": "kp-search-query"}

    edit_number = _edit_comment_number(text)
    if edit_number:
        try:
            current = await asyncio.to_thread(_start_comment_edit, sender_id, edit_number)
            await _reply_long(chat_id, f"Редактирование комментария КП №{edit_number}.\nТекущее значение:\n{current}")
            await _reply(chat_id, "Пришли новый текст комментария одним сообщением. Для очистки поля отправь ОЧИСТИТЬ. Для выхода — ОТМЕНА.")
            return {"ok": True, "handled": f"comment-edit-start-{edit_number}"}
        except Exception as exc:
            core.log(f"KP MAX comment edit start failed: {type(exc).__name__}: {exc}")
            await _reply(chat_id, "Не удалось начать редактирование комментария.")
            return {"ok": True, "error": "comment-edit-start"}

    edit_session = _edit_session_get(sender_id)
    if edit_session:
        if upper in {"ОТМЕНА", "CANCEL"}:
            _edit_session_clear(sender_id)
            await _reply(chat_id, "Редактирование отменено. В 1С ничего не изменено.")
            return {"ok": True, "handled": "comment-edit-cancel"}
        if edit_session.get("stage") == "await_text":
            if upper in {"СОХРАНИТЬ", "SAVE"}:
                await _reply(chat_id, "Сначала пришли новый текст комментария.")
                return {"ok": True, "handled": "comment-edit-await-text"}
            proposed = "" if upper == "ОЧИСТИТЬ" else text
            try:
                staged = _set_comment_proposal(sender_id, proposed)
            except ValueError as exc:
                await _reply(chat_id, str(exc))
                return {"ok": True, "error": "comment-too-long"}
            preview = proposed if proposed else "[ПОЛЕ БУДЕТ ОЧИЩЕНО]"
            await _reply_long(chat_id, f"Новое значение комментария КП №{staged['number']}:\n{preview}")
            await _reply(chat_id, "Для записи в 1С отправь СОХРАНИТЬ. Для отказа — ОТМЕНА.")
            return {"ok": True, "handled": "comment-edit-staged"}
        if edit_session.get("stage") == "confirm":
            if upper in {"СОХРАНИТЬ", "SAVE"}:
                try:
                    saved = await asyncio.to_thread(_commit_comment_edit, sender_id, role)
                    await _reply(chat_id, f"Комментарий КП №{saved['number']} сохранён в 1С. Символов: {saved['chars']}.")
                    core.log(f"KP MAX comment saved: KP {saved['number']}, user={sender_id}, role={role}, chars={saved['chars']}")
                    return {"ok": True, "handled": f"comment-edit-saved-{saved['number']}"}
                except RuntimeError as exc:
                    if "concurrently" in str(exc):
                        await _reply(chat_id, "Комментарий в 1С изменился после начала редактирования. Запись отменена. Начни заново командой РЕДКОМ <номер>.")
                    else:
                        core.log(f"KP MAX comment save failed: {type(exc).__name__}: {exc}")
                        await _reply(chat_id, "Не удалось сохранить комментарий в 1С. Исходное значение не перезаписано ботом.")
                    return {"ok": True, "error": "comment-edit-save"}
            await _reply(chat_id, "Изменение подготовлено. Отправь СОХРАНИТЬ или ОТМЕНА. Чтобы заменить текст заново, отправь РЕДКОМ <номер>.")
            return {"ok": True, "handled": "comment-edit-confirm"}

    if upper in {"МЕНЮ", "MENU", "НАВИГАЦИЯ", "СПИСОК", "СПИСОК КП"}:
        await _reply_menu(chat_id, nav.root_menu(role))
        return {"ok": True, "handled": "navigation-root"}

    if upper in {"ДОСТУП", "ACCESS"}:
        admins, users, invites = _access_counts()
        await _reply(
            chat_id,
            f"Доступ активен. Роль: {'администратор' if role == 'admin' else 'сотрудник'}.\n"
            f"Твой MAX user_id: {sender_id}\n"
            f"Администраторов: {admins}; сотрудников: {users}; неиспользованных кодов: {invites}.",
        )
        return {"ok": True, "handled": "access-status"}

    if upper in {"КОД", "CODE"}:
        if role != "admin":
            await _reply(chat_id, "Команда доступна только администратору.")
            return {"ok": True, "denied": "admin"}
        code = _create_invite(sender_id)
        await _reply(
            chat_id,
            f"Одноразовый код сотрудника: {code}\n"
            "Действует 24 часа и сгорает после первого успешного использования.",
        )
        return {"ok": True, "handled": "invite-created"}

    revoke_match = re.fullmatch(r"(?:ОТКЛЮЧИТЬ|REVOKE)\s+(\d{1,20})", text.strip(), flags=re.IGNORECASE)
    if revoke_match:
        if role != "admin":
            await _reply(chat_id, "Команда доступна только администратору.")
            return {"ok": True, "denied": "admin"}
        target_id = revoke_match.group(1)
        removed = _revoke_user(sender_id, target_id)
        await _reply(chat_id, "Доступ сотрудника отключён." if removed else "Сотрудник с таким user_id не найден.")
        return {"ok": True, "handled": "user-revoked"}

    comment_number = _comment_number(text)
    if comment_number:
        try:
            comment_text = await asyncio.to_thread(_build_full_comment_text, comment_number)
            await _reply_long(chat_id, comment_text)
            core.log(f"KP MAX bot: full comment KP {comment_number} sent to user={sender_id}")
            return {"ok": True, "handled": f"comment-{comment_number}"}
        except Exception as exc:
            core.log(f"KP MAX full comment failed: {type(exc).__name__}: {exc}")
            await _reply(chat_id, "Не удалось получить полный комментарий из 1С.")
            return {"ok": True, "error": "comment-read"}

    number = _kp_number(text)
    if not number:
        await _reply_menu(chat_id, nav.root_menu(role))
        return {"ok": True, "handled": "navigation-root"}

    kp_text = _build_kp_text(number)
    try:
        result = await _reply(chat_id, kp_text)
        core.log(f"KP MAX bot: KP {number} sent to chat={chat_id}, user={sender_id}, status={result.get('status')}")
        return {"ok": True, "handled": f"kp-{number}"}
    except Exception as exc:
        core.log(f"KP MAX bot send failed: {type(exc).__name__}: {exc}")
        raise HTTPException(status_code=502, detail="MAX reply failed") from exc


def _promote_kp_routes_before_static_mount() -> None:
    target_paths = {"/api/max/kp-bot/status", "/api/max/kp-bot/webhook"}
    promoted = [route for route in app.routes if getattr(route, "path", "") in target_paths]
    if not promoted:
        return
    for route in promoted:
        app.routes.remove(route)
    mount_index = next(
        (index for index, route in enumerate(app.routes) if getattr(route, "path", "") == "/"),
        len(app.routes),
    )
    app.routes[mount_index:mount_index] = promoted


_promote_kp_routes_before_static_mount()
