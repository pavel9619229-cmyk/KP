import asyncio
import hmac
import os
import re
from pathlib import Path

import requests
from fastapi import HTTPException, Request

import api_proxy as core

app = core.app
KP_MAX_BOT_TOKEN = os.getenv("KP_MAX_BOT_TOKEN", "").strip()
KP_MAX_WEBHOOK_SECRET = os.getenv("KP_MAX_WEBHOOK_SECRET", "").strip()
KP_MAX_CA_BUNDLE = os.getenv("KP_MAX_CA_BUNDLE", "").strip()


def _message_context(payload: dict) -> tuple[str, str, bool]:
    message = payload.get("message") if isinstance(payload.get("message"), dict) else {}
    body = message.get("body") if isinstance(message.get("body"), dict) else {}
    recipient = message.get("recipient") if isinstance(message.get("recipient"), dict) else {}
    sender = message.get("sender") if isinstance(message.get("sender"), dict) else {}
    text = str(body.get("text") or message.get("text") or "").strip()
    chat_id = str(recipient.get("chat_id") or message.get("chat_id") or payload.get("chat_id") or "").strip()
    return text, chat_id, bool(sender.get("is_bot"))


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
    verify = KP_MAX_CA_BUNDLE if KP_MAX_CA_BUNDLE and Path(KP_MAX_CA_BUNDLE).is_file() else True
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
        verify=verify,
    )
    try:
        data = response.json()
    except Exception:
        data = {"raw": response.text[:500]}
    if not 200 <= response.status_code < 300:
        raise RuntimeError(f"MAX send HTTP {response.status_code}: {str(data)[:500]}")
    return {"status": response.status_code, "response": data}


@app.get("/api/max/kp-bot/status")
async def kp_max_bot_status():
    return {
        "ok": bool(KP_MAX_BOT_TOKEN and KP_MAX_WEBHOOK_SECRET),
        "tokenConfigured": bool(KP_MAX_BOT_TOKEN),
        "secretConfigured": bool(KP_MAX_WEBHOOK_SECRET),
        "caConfigured": bool(KP_MAX_CA_BUNDLE and Path(KP_MAX_CA_BUNDLE).is_file()),
        "webhook": "/api/max/kp-bot/webhook",
        "testCommand": "КП 588",
    }


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
    text, chat_id, sender_is_bot = _message_context(payload)
    if sender_is_bot:
        return {"ok": True, "ignored": "bot_message"}
    if not chat_id:
        return {"ok": True, "ignored": "chat_id"}
    number = _kp_number(text)
    if not number:
        return {"ok": True, "ignored": "command"}
    kp_text = _build_kp_text(number)
    try:
        result = await asyncio.to_thread(_send_message, chat_id, kp_text)
        core.log(f"KP MAX bot: KP {number} sent to chat={chat_id}, status={result.get('status')}")
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
