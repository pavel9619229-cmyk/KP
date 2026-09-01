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

app = core.app
KP_MAX_BOT_TOKEN = os.getenv("KP_MAX_BOT_TOKEN", "").strip()
KP_MAX_WEBHOOK_SECRET = os.getenv("KP_MAX_WEBHOOK_SECRET", "").strip()
KP_MAX_CA_BUNDLE = os.getenv("KP_MAX_CA_BUNDLE", "").strip()
KP_MAX_ACCESS_FILE = Path(os.getenv("KP_MAX_ACCESS_FILE", "/opt/kp-api/data/kp_max_access.json"))
INVITE_TTL_SECONDS = 24 * 60 * 60
_ACCESS_LOCK = Lock()
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


@app.post("/api/max/kp-bot/webhook")
async def kp_max_bot_webhook(request: Request):
    provided_secret = str(request.headers.get("X-Max-Bot-Api-Secret") or "")
    if not KP_MAX_WEBHOOK_SECRET or not hmac.compare_digest(provided_secret, KP_MAX_WEBHOOK_SECRET):
        raise HTTPException(status_code=401, detail="Invalid MAX webhook secret")
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid MAX update payload")
    if str(payload.get("update_type") or "") != "message_created":
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

    number = _kp_number(text)
    if not number:
        await _reply(
            chat_id,
            "Команды:\nКП 588 — показать КП\nДОСТУП — проверить доступ"
            + ("\nКОД — выдать одноразовый код сотруднику\nОТКЛЮЧИТЬ <user_id> — отключить сотрудника" if role == "admin" else ""),
        )
        return {"ok": True, "handled": "help"}

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
