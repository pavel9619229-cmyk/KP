import hashlib
import html
import json
import os
import time
from pathlib import Path
from threading import Lock

import requests

import api_proxy as core
import kp_max_documents as documents
import kp_max_navigation as nav

SESSION_TTL = 30 * 60
ZERO_GUID = "00000000-0000-0000-0000-000000000000"
_LOCK = Lock()
_SESSIONS: dict[str, dict] = {}

FIELDS = {
    "extra": ("ДОПОЛНИТЕЛЬНАЯ ИНФОРМАЦИЯ", "ДополнительнаяИнформация"),
    "purpose": ("НАЗНАЧЕНИЕ ПЛАТЕЖА", "НазначениеПлатежа"),
    "comment": ("КОММЕНТАРИЙ", "Комментарий"),
}


def _base() -> str:
    return str(core.BASE).strip().strip('"').strip("'").rstrip("/")


def _hash(value) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _fetch_order(ref_key: str) -> dict:
    r = requests.get(
        f"{_base()}/{documents.ORDER_ENTITY}(guid'{ref_key}')",
        headers=core._build_headers(), timeout=40,
    )
    if r.status_code != 200:
        raise RuntimeError(f"1C order read HTTP {r.status_code}: {r.text[:300]}")
    data = r.json() if r.content else {}
    return data if isinstance(data, dict) else {}


def _patch_order(ref_key: str, patch: dict) -> None:
    r = requests.patch(
        f"{_base()}/{documents.ORDER_ENTITY}(guid'{ref_key}')",
        headers={**core._build_headers(), "Content-Type": "application/json; charset=utf-8"},
        json=patch, timeout=35,
    )
    if r.status_code not in (200, 204):
        raise RuntimeError(f"1C order PATCH HTTP {r.status_code}: {r.text[:300]}")


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


def clear(user_id: str) -> None:
    with _LOCK:
        _SESSIONS.pop(str(user_id), None)


def _set_session(user_id: str, **values) -> dict:
    with _LOCK:
        s = _SESSIONS.setdefault(str(user_id), {})
        s.update(values)
        s["expiresAt"] = int(time.time()) + SESSION_TTL
        return dict(s)


def _compact(value, limit: int = 300) -> str:
    text = str(value or "").strip()
    if not text:
        return "[ПОЛЕ ПУСТО]"
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _catalog_one(entity: str, ref_key: str) -> dict:
    key = str(ref_key or "").strip()
    if not key or key == ZERO_GUID:
        return {}
    r = requests.get(
        f"{_base()}/{entity}(guid'{key}')",
        headers=core._build_headers(), timeout=25,
    )
    if r.status_code != 200:
        return {}
    data = r.json() if r.content else {}
    return data if isinstance(data, dict) else {}


def _bank_accounts(org_key: str) -> list[dict]:
    r = requests.get(
        f"{_base()}/Catalog_БанковскиеСчетаОрганизаций",
        headers=core._build_headers(),
        params={"$top": "500"}, timeout=45,
    )
    rows = r.json().get("value", []) if r.status_code == 200 else []
    result = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("Owner_Key") or "") != str(org_key):
            continue
        if bool(row.get("DeletionMark")) or bool(row.get("Закрыт")):
            continue
        if str(row.get("НомерСчета") or "").strip():
            result.append(row)
    return result


def info_menu(number: str, ref_key: str, status_idx: int, page: int) -> dict:
    doc = _fetch_order(ref_key)
    key = nav.status_key(status_idx); page = max(0, int(page))
    bank_key = str(doc.get("БанковскийСчет_Key") or "").strip()
    bank = _catalog_one("Catalog_БанковскиеСчетаОрганизаций", bank_key)
    bank_label = str(bank.get("Description") or bank.get("НомерСчета") or "").strip() or "[НЕ ВЫБРАН]"
    text = (
        f"ИНФОРМАЦИЯ ДЛЯ ПЕЧАТИ — КП {number}\n\n"
        f"Доп. информация: {_compact(doc.get('ДополнительнаяИнформация'))}\n\n"
        f"Назначение платежа: {_compact(doc.get('НазначениеПлатежа'))}\n\n"
        f"Комментарий: {_compact(doc.get('Комментарий'))}\n\n"
        f"Банковский счет: {bank_label}"
    )
    rows = [
        [nav._cb("🟢🟢 ← ВЕРНУТЬСЯ НА ГЛАВНОЕ МЕНЮ", "nav:root")],
        [nav._cb("🟢 ← ВЕРНУТЬСЯ НА УРОВЕНЬ ВЫШЕ", f"nav:doc:{number}:{ref_key}:{key}:{page}")],
        [nav._cb("ДОПОЛНИТЕЛЬНАЯ ИНФОРМАЦИЯ", f"prn:edit:extra:{number}:{ref_key}:{key}:{page}")],
        [nav._cb("НАЗНАЧЕНИЕ ПЛАТЕЖА", f"prn:edit:purpose:{number}:{ref_key}:{key}:{page}")],
        [nav._cb("КОММЕНТАРИЙ", f"prn:edit:comment:{number}:{ref_key}:{key}:{page}")],
        [nav._cb("БАНКОВСКИЙ СЧЕТ", f"prn:bank:{number}:{ref_key}:{key}:{page}")],
    ]
    return {"text": text, "attachments": nav._keyboard(rows)}


def start_edit(user_id: str, field: str, number: str, ref_key: str, status_idx: int, page: int) -> dict:
    if field not in FIELDS:
        raise ValueError("invalid print field")
    doc = _fetch_order(ref_key)
    label, odata = FIELDS[field]
    original = str(doc.get(odata) or "")
    _set_session(
        user_id, mode="text", stage="await_text", field=field, number=str(number),
        refKey=str(ref_key), statusIdx=int(status_idx), page=int(page), original=original,
        originalHash=_hash(original), proposed="",
    )
    text = (
        f"{label} — СЧЕТ ПО КП {number}\n"
        f"Текущее значение:\n{_compact(original, 2500)}\n\n"
        "Пришли новый текст одним сообщением. Для очистки отправь ОЧИСТИТЬ."
    )
    return {"text": text, "attachments": nav._keyboard([[nav._cb("ОТМЕНА", "prn:cancel")]])}


def set_text(user_id: str, text: str) -> dict:
    s = session_get(user_id)
    if not s or s.get("mode") != "text" or s.get("stage") != "await_text":
        raise RuntimeError("print edit is not active")
    value = "" if str(text or "").strip().upper() == "ОЧИСТИТЬ" else str(text or "")
    if len(value) > 20000:
        raise ValueError("Максимум 20000 символов")
    _set_session(user_id, stage="confirm", proposed=value)
    return confirm_menu(user_id)


def bank_menu(user_id: str, number: str, ref_key: str, status_idx: int, page: int) -> dict:
    doc = _fetch_order(ref_key)
    org_key = str(doc.get("Организация_Key") or "").strip()
    banks = _bank_accounts(org_key)
    current = str(doc.get("БанковскийСчет_Key") or "").strip()
    _set_session(
        user_id, mode="bank", stage="bank_pick", number=str(number), refKey=str(ref_key),
        statusIdx=int(status_idx), page=int(page), original=current, originalHash=_hash(current),
    )
    rows = [[nav._cb("ОТМЕНА", "prn:cancel")]]
    for bank in banks[:20]:
        bank_key = str(bank.get("Ref_Key") or "").strip()
        account = str(bank.get("НомерСчета") or "").strip()
        name = str(bank.get("Description") or bank.get("НаименованиеБанка") or "").strip()
        mark = "✓ " if bank_key == current else ""
        rows.append([nav._cb(f"{mark}{account} {name}"[:110], f"prn:bankpick:{bank_key}")])
    text = f"БАНКОВСКИЙ СЧЕТ — КП {number}\n\nВыбери счет организации. Найдено: {len(banks)}."
    if not banks:
        text += "\nВ 1С не найдено активных банковских счетов этой организации."
    return {"text": text, "attachments": nav._keyboard(rows)}


def pick_bank(user_id: str, bank_key: str) -> dict:
    s = session_get(user_id)
    if not s or s.get("mode") != "bank" or s.get("stage") != "bank_pick":
        raise RuntimeError("bank selection is not active")
    bank = _catalog_one("Catalog_БанковскиеСчетаОрганизаций", bank_key)
    if not bank or bool(bank.get("DeletionMark")) or bool(bank.get("Закрыт")):
        raise RuntimeError("Банковский счет недоступен")
    _set_session(user_id, stage="confirm", proposed=str(bank_key), proposedLabel=str(bank.get("НомерСчета") or bank.get("Description") or bank_key))
    return confirm_menu(user_id)


def confirm_menu(user_id: str) -> dict:
    s = session_get(user_id)
    if not s or s.get("stage") != "confirm":
        raise RuntimeError("print confirmation is not active")
    if s.get("mode") == "bank":
        label = "БАНКОВСКИЙ СЧЕТ"
        old = str(s.get("original") or "[НЕ ВЫБРАН]")
        new = str(s.get("proposedLabel") or s.get("proposed") or "")
    else:
        label = FIELDS[str(s.get("field"))][0]
        old = _compact(s.get("original"), 1200)
        new = _compact(s.get("proposed"), 1200)
    text = f"{label} — КП {s['number']}\n\nСтарое значение:\n{old}\n\nНовое значение:\n{new}"
    rows = [
        [nav._cb("СОХРАНИТЬ", "prn:save")],
        [nav._cb("ИЗМЕНИТЬ", "prn:again")],
        [nav._cb("ОТМЕНА", "prn:cancel")],
    ]
    return {"text": text, "attachments": nav._keyboard(rows)}


def again(user_id: str) -> dict:
    s = session_get(user_id)
    if not s:
        raise RuntimeError("print session expired")
    if s.get("mode") == "bank":
        return bank_menu(user_id, s["number"], s["refKey"], int(s["statusIdx"]), int(s["page"]))
    return start_edit(user_id, str(s["field"]), s["number"], s["refKey"], int(s["statusIdx"]), int(s["page"]))


def cancel_menu(user_id: str) -> dict:
    s = session_get(user_id)
    if not s:
        return nav.root_menu("user")
    number, ref_key = str(s["number"]), str(s["refKey"])
    status_idx, page = int(s["statusIdx"]), int(s["page"])
    clear(user_id)
    return info_menu(number, ref_key, status_idx, page)


def _audit(user_id: str, role: str, s: dict, odata_field: str, new_value) -> None:
    path = Path(os.getenv("KP_MAX_ACCESS_FILE", "/opt/kp-api/data/kp_max_access.json")).parent / "kp_max_print_edit_audit.jsonl"
    payload = {
        "ts": int(time.time()), "userId": str(user_id), "role": str(role),
        "kp": str(s.get("number") or ""), "orderRef": str(s.get("refKey") or ""),
        "field": str(odata_field), "oldHash": str(s.get("originalHash") or ""),
        "newHash": _hash(new_value),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass


def commit(user_id: str, role: str) -> tuple[dict, dict]:
    s = session_get(user_id)
    if not s or s.get("stage") != "confirm":
        raise RuntimeError("print confirmation is not active")
    current = _fetch_order(str(s["refKey"]))
    if s.get("mode") == "bank":
        odata_field = "БанковскийСчет_Key"
        proposed = str(s.get("proposed") or "")
    else:
        odata_field = FIELDS[str(s["field"])][1]
        proposed = str(s.get("proposed") or "")
    actual_before = str(current.get(odata_field) or "")
    if _hash(actual_before) != str(s.get("originalHash") or ""):
        clear(user_id)
        raise RuntimeError("print field changed concurrently")
    _patch_order(str(s["refKey"]), {odata_field: proposed})
    verified = _fetch_order(str(s["refKey"]))
    actual_after = str(verified.get(odata_field) or "")
    if actual_after != proposed:
        raise RuntimeError("1C print field verification failed")
    _audit(user_id, role, s, odata_field, proposed)
    number, ref_key = str(s["number"]), str(s["refKey"])
    status_idx, page = int(s["statusIdx"]), int(s["page"])
    clear(user_id)
    menu = info_menu(number, ref_key, status_idx, page)
    menu["text"] = "Изменение сохранено в 1С.\n\n" + menu["text"]
    return menu, {"number": number, "field": odata_field}


def _display_name(row: dict) -> str:
    return str(row.get("НаименованиеПолное") or row.get("Description") or "").strip()


def _resolve_bank(order: dict) -> dict:
    bank_key = str(order.get("БанковскийСчет_Key") or "").strip()
    if bank_key and bank_key != ZERO_GUID:
        bank = _catalog_one("Catalog_БанковскиеСчетаОрганизаций", bank_key)
        if bank:
            return bank
    banks = _bank_accounts(str(order.get("Организация_Key") or ""))
    if len(banks) == 1:
        return banks[0]
    if len(banks) > 1:
        raise RuntimeError("В счете не выбран банковский счет. Выбери его через РЕДАКТИРОВАТЬ ИНФОРМАЦИЮ ДЛЯ ПЕЧАТИ.")
    raise RuntimeError("У организации в 1С нет активного банковского счета.")


def _date_long(value) -> str:
    try:
        dt = core._parse_odata_datetime(str(value or ""))
        if dt is not None:
            return dt.strftime("%d.%m.%Y")
    except Exception:
        pass
    return str(value or "")[:10]


def _money(value) -> str:
    try:
        return f"{float(value or 0):,.2f}".replace(",", " ")
    except Exception:
        return "0,00"


def _invoice_data(ref_key: str) -> dict:
    order = _fetch_order(ref_key)
    org = _catalog_one("Catalog_Организации", str(order.get("Организация_Key") or ""))
    buyer = _catalog_one("Catalog_Контрагенты", str(order.get("Контрагент_Key") or ""))
    if not buyer:
        buyer = _catalog_one("Catalog_Партнеры", str(order.get("Партнер_Key") or ""))
    bank = _resolve_bank(order)
    rows = order.get("Товары") or []
    products: dict[str, dict] = {}
    vats: dict[str, dict] = {}
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        pkey = str(row.get("Номенклатура_Key") or "")
        if pkey and pkey not in products:
            products[pkey] = _catalog_one("Catalog_Номенклатура", pkey)
        vkey = str(row.get("СтавкаНДС_Key") or "")
        if vkey and vkey != ZERO_GUID and vkey not in vats:
            vats[vkey] = _catalog_one("Catalog_СтавкиНДС", vkey)
    return {"order": order, "org": org, "buyer": buyer, "bank": bank, "products": products, "vats": vats}


def _safe(value) -> str:
    return html.escape(str(value or "").strip()).replace("\n", "<br/>")


def _pdf_path(order: dict) -> Path:
    base = Path(os.getenv("KP_MAX_ACCESS_FILE", "/opt/kp-api/data/kp_max_access.json")).parent / "print_forms"
    base.mkdir(parents=True, exist_ok=True)
    num = documents._short_number(order.get("Number")) or "invoice"
    ref = str(order.get("Ref_Key") or "")[:8]
    return base / f"invoice_{num}_{ref}.pdf"


def generate_pdf(ref_key: str) -> tuple[Path, dict]:
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_RIGHT
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    data = _invoice_data(ref_key)
    order, org, buyer, bank = data["order"], data["org"], data["buyer"], data["bank"]
    font = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
    bold = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
    pdfmetrics.registerFont(TTFont("InvoiceSans", font))
    pdfmetrics.registerFont(TTFont("InvoiceSansBold", bold))
    path = _pdf_path(order)
    doc = SimpleDocTemplate(str(path), pagesize=A4, leftMargin=14*mm, rightMargin=14*mm, topMargin=12*mm, bottomMargin=12*mm)
    styles = getSampleStyleSheet()
    body = ParagraphStyle("body", parent=styles["Normal"], fontName="InvoiceSans", fontSize=9, leading=12)
    bold_style = ParagraphStyle("bold", parent=body, fontName="InvoiceSansBold")
    title = ParagraphStyle("title", parent=bold_style, fontSize=15, leading=19, alignment=TA_CENTER, spaceAfter=10)
    right = ParagraphStyle("right", parent=bold_style, alignment=TA_RIGHT)
    story = []
    seller_name = _display_name(org) or "Организация не указана"
    buyer_name = _display_name(buyer) or "Покупатель не указан"
    seller_tax = f"ИНН {org.get('ИНН') or '—'} / КПП {org.get('КПП') or '—'}"
    buyer_tax = f"ИНН {buyer.get('ИНН') or '—'} / КПП {buyer.get('КПП') or '—'}"
    bank_name = str(bank.get("НаименованиеБанка") or bank.get("Description") or "").strip()
    bank_account = str(bank.get("НомерСчета") or "").strip()
    bank_bik = str(bank.get("БИКБанка") or "").strip()
    bank_corr = str(bank.get("КоррСчетБанка") or "").strip()
    bank_rows = [
        [Paragraph("Получатель", bold_style), Paragraph(_safe(seller_name), body)],
        [Paragraph("ИНН / КПП", bold_style), Paragraph(_safe(seller_tax), body)],
        [Paragraph("Счет получателя", bold_style), Paragraph(_safe(bank_account), body)],
        [Paragraph("Банк", bold_style), Paragraph(_safe(bank_name), body)],
        [Paragraph("БИК", bold_style), Paragraph(_safe(bank_bik), body)],
        [Paragraph("Корр. счет", bold_style), Paragraph(_safe(bank_corr), body)],
    ]
    bank_table = Table(bank_rows, colWidths=[38*mm, 140*mm])
    bank_table.setStyle(TableStyle([
        ("GRID", (0,0), (-1,-1), 0.45, colors.black),
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("LEFTPADDING", (0,0), (-1,-1), 4), ("RIGHTPADDING", (0,0), (-1,-1), 4),
        ("TOPPADDING", (0,0), (-1,-1), 3), ("BOTTOMPADDING", (0,0), (-1,-1), 3),
    ]))
    story.extend([bank_table, Spacer(1, 8)])
    number = documents._short_number(order.get("Number"))
    story.append(Paragraph(f"Счет на оплату № {_safe(number)} от {_safe(_date_long(order.get('Date')))}", title))
    story.append(Paragraph(f"<b>Поставщик:</b> {_safe(seller_name)}, {_safe(seller_tax)}", body))
    story.append(Spacer(1, 4))
    story.append(Paragraph(f"<b>Покупатель:</b> {_safe(buyer_name)}, {_safe(buyer_tax)}", body))
    story.append(Spacer(1, 9))

    headers = ["№", "Товар", "Кол-во", "Цена", "НДС", "Сумма"]
    table_data = [[Paragraph(h, bold_style) for h in headers]]
    goods = order.get("Товары") or []
    vat_total = 0.0
    grand_total = 0.0
    for idx, row in enumerate(goods if isinstance(goods, list) else [], 1):
        if not isinstance(row, dict):
            continue
        pkey = str(row.get("Номенклатура_Key") or "")
        product = data["products"].get(pkey, {})
        name = _display_name(product) or pkey or "Товар"
        qty = float(row.get("Количество") or 0)
        price = float(row.get("Цена") or 0)
        total = float(row.get("СуммаСНДС") or row.get("Сумма") or (qty * price))
        vat_sum = float(row.get("СуммаНДС") or 0)
        vat_total += vat_sum; grand_total += total
        vkey = str(row.get("СтавкаНДС_Key") or "")
        vat = data["vats"].get(vkey, {})
        vat_label = str(vat.get("Description") or "22%").strip() or "22%"
        table_data.append([
            Paragraph(str(idx), body), Paragraph(_safe(name), body),
            Paragraph(_safe(f"{qty:g}"), body), Paragraph(_safe(_money(price)), body),
            Paragraph(_safe(vat_label), body), Paragraph(_safe(_money(total)), body),
        ])
    goods_table = Table(table_data, colWidths=[9*mm, 82*mm, 20*mm, 25*mm, 18*mm, 28*mm], repeatRows=1)
    goods_table.setStyle(TableStyle([
        ("GRID", (0,0), (-1,-1), 0.4, colors.black),
        ("BACKGROUND", (0,0), (-1,0), colors.whitesmoke),
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("ALIGN", (2,1), (-1,-1), "RIGHT"),
        ("LEFTPADDING", (0,0), (-1,-1), 3), ("RIGHTPADDING", (0,0), (-1,-1), 3),
        ("TOPPADDING", (0,0), (-1,-1), 3), ("BOTTOMPADDING", (0,0), (-1,-1), 3),
    ]))
    story.extend([goods_table, Spacer(1, 8)])
    invoice_total = float(order.get("СуммаДокумента") or grand_total)
    story.append(Paragraph(f"Итого: {_safe(_money(invoice_total))} руб.", right))
    story.append(Paragraph(f"В том числе НДС: {_safe(_money(vat_total))} руб.", right))
    story.append(Spacer(1, 8))
    purpose = str(order.get("НазначениеПлатежа") or "").strip()
    extra = str(order.get("ДополнительнаяИнформация") or "").strip()
    comment = str(order.get("Комментарий") or "").strip()
    if purpose:
        story.append(Paragraph(f"<b>Назначение платежа:</b> {_safe(purpose)}", body))
        story.append(Spacer(1, 4))
    if extra:
        story.append(Paragraph(f"<b>Дополнительная информация:</b> {_safe(extra)}", body))
        story.append(Spacer(1, 4))
    if comment:
        story.append(Paragraph(f"<b>Комментарий:</b> {_safe(comment)}", body))
    story.append(Spacer(1, 14))
    story.append(Paragraph("Руководитель ____________________", body))
    story.append(Spacer(1, 8))
    story.append(Paragraph("Главный бухгалтер _______________", body))
    doc.build(story)
    if not path.is_file() or path.stat().st_size < 500:
        raise RuntimeError("Не удалось сформировать PDF счета")
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass
    return path, order
