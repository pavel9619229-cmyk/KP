#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import base64
import hashlib
import json
import os
import re
import threading
import time
from datetime import datetime
from html import unescape
from pathlib import Path

import requests
import urllib3
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

urllib3.disable_warnings()

app = FastAPI(title="1C KP Realtime API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE = os.getenv(
    "ODATA_BASE_URL",
    "https://aclient.1c-hosting.com/1R88669/1R88669_UT11_bfimz0bdj3/odata/standard.odata",
)
USERNAME = os.getenv("ODATA_USERNAME", "павел")
PASSWORD = os.getenv("ODATA_PASSWORD", "1")
ENTITY = os.getenv("ODATA_ENTITY", "Document_КоммерческоеПредложениеКлиенту")
DATA_FILE = os.getenv("DATA_FILE", "kp_2026_march_april.json")
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "10"))
STALE_REFRESH_AFTER_SECONDS = int(os.getenv("STALE_REFRESH_AFTER_SECONDS", "20"))
ENRICH_PER_REFRESH = int(os.getenv("ENRICH_PER_REFRESH", "20"))
FORCE_INFO_REFRESH_TOP_ROWS = int(os.getenv("FORCE_INFO_REFRESH_TOP_ROWS", "20"))
GROUP_ENRICH_INTERVAL_SECONDS = int(os.getenv("GROUP_ENRICH_INTERVAL_SECONDS", "300"))
DOC_TIMEOUT_SECONDS = float(os.getenv("DOC_TIMEOUT_SECONDS", "1.5"))
NAV_TIMEOUT_SECONDS = float(os.getenv("NAV_TIMEOUT_SECONDS", "0.8"))
GROUP_CHECK_TIMEOUT_SECONDS = float(os.getenv("GROUP_CHECK_TIMEOUT_SECONDS", "8"))
NAV_LINK_LIMIT = int(os.getenv("NAV_LINK_LIMIT", "4"))
STATUS_KP_PROPERTY_KEY = os.getenv(
    "STATUS_KP_PROPERTY_KEY",
    "e1c7a0e4-4f8d-11f0-8d50-bc97e15eb091",
)

TARGET_START = datetime(2026, 3, 1, 0, 0, 0)
TARGET_END = datetime(2026, 4, 30, 23, 59, 59)

LIGHT_SELECT_FIELDS = [
    "Number",
    "Date",
    "Статус",
    "ДополнительныеРеквизиты",
]

_cached_rows = []
_cached_fp = ""
_last_refresh = None
_last_group_enrich = None
_customer_name_cache = {}
_additional_info_cache = {}
_status_kp_value_cache = {}
_group_doc_flags_cache = {}
_refresh_lock = threading.Lock()


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def _build_headers() -> dict:
    creds = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode("utf-8")).decode("ascii")
    return {
        "Authorization": f"Basic {creds}",
        "Accept": "application/json",
    }


def strip_html(text: str) -> str:
    if not text:
        return ""
    text = unescape(text)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    return text


def first_line(*values: str) -> str:
    for raw in values:
        if not raw:
            continue
        cleaned = strip_html(str(raw)).replace("\r\n", "\n").replace("\r", "\n")
        for line in cleaned.split("\n"):
            line = line.strip()
            if line:
                return line
    return ""


def rows_fingerprint(rows: list) -> str:
    return json.dumps(rows, ensure_ascii=False, sort_keys=True)


def score_customer_candidate(nav_obj: dict) -> int:
    description = str(nav_obj.get("Description") or "").strip()
    if not description:
        return 0

    score = 1
    upper = description.upper()
    if any(token in upper for token in ["ООО", "ИП", "АО", "ПАО", "ЗАО"]):
        score += 4

    digit_like = 0
    for value in nav_obj.values():
        if isinstance(value, str):
            only_digits = "".join(ch for ch in value if ch.isdigit())
            if len(only_digits) in (9, 10, 12):
                digit_like += 1
    if digit_like >= 2:
        score += 3

    if len(description) > 4:
        score += 1
    return score


def _fetch_doc_by_ref(ref_key: str, headers: dict, timeout: float = DOC_TIMEOUT_SECONDS) -> dict:
    for attempt in range(3):
        try:
            doc_resp = requests.get(
                f"{BASE}/{ENTITY}(guid'{ref_key}')",
                headers=headers,
                timeout=timeout,
                verify=False,
            )
            if doc_resp.status_code != 200:
                time.sleep(0.4 * (attempt + 1))
                continue
            doc = doc_resp.json()
            return doc if isinstance(doc, dict) else {}
        except Exception:
            time.sleep(0.4 * (attempt + 1))
    return {}


def _get_json_with_retry(
    url: str,
    headers: dict,
    *,
    params: dict | None = None,
    timeout: float = 20,
    retries: int = 4,
) -> tuple[dict | None, str | None]:
    last_error = None
    for attempt in range(retries):
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=timeout,
                verify=False,
            )
            if response.status_code >= 500:
                last_error = f"HTTP {response.status_code}: {response.text[:300]}"
                time.sleep(0.4 * (attempt + 1))
                continue
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                return payload, None
            return {}, None
        except Exception as exc:
            last_error = str(exc)
            time.sleep(0.4 * (attempt + 1))
    return None, last_error


def _parse_odata_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _iterate_tail_pages(entity_name: str, headers: dict, select_fields: list[str], page_size: int = 200):
    raw_count = ""
    try:
        response = requests.get(
            f"{BASE}/{entity_name}/$count",
            headers=headers,
            timeout=GROUP_CHECK_TIMEOUT_SECONDS,
            verify=False,
        )
        if response.status_code != 200:
            return
        raw_count = response.text.strip()
        total_count = int(raw_count)
    except Exception:
        return

    if total_count <= 0:
        return

    skip = ((total_count - 1) // page_size) * page_size
    select_expr = ",".join(select_fields)

    while True:
        payload, error = _get_json_with_retry(
            f"{BASE}/{entity_name}",
            headers,
            params={"$select": select_expr, "$top": str(page_size), "$skip": str(skip)},
            timeout=GROUP_CHECK_TIMEOUT_SECONDS,
            retries=2,
        )
        if error or not isinstance(payload, dict):
            return

        batch = payload.get("value", [])
        if not batch:
            return

        yield batch

        batch_dates = [_parse_odata_datetime(item.get("Date")) for item in batch]
        batch_dates = [d for d in batch_dates if d is not None]
        if batch_dates and max(batch_dates) < TARGET_START:
            return

        if skip == 0:
            return
        skip = max(0, skip - page_size)


def _enrich_group_flags_bulk(rows: list[dict], headers: dict) -> None:
    target_refs = [str(r.get("refKey") or "") for r in rows]
    target_refs = [r for r in target_refs if r]
    if not target_refs:
        return

    unresolved_refs = {
        r.get("refKey")
        for r in rows
        if r.get("refKey") and (r.get("invoiceCreated") is None or r.get("paymentReceived") is None)
    }
    if not unresolved_refs:
        return

    kp_ref_set = set(unresolved_refs)

    kp_to_orders: dict[str, set[str]] = {kp: set() for kp in kp_ref_set}
    order_to_kp: dict[str, str] = {}
    order_tokens: dict[str, set[str]] = {}

    def build_order_tokens(order_number: str) -> set[str]:
        raw = str(order_number or "").strip().lower()
        if not raw:
            return set()

        compact = re.sub(r"[^0-9a-zа-я]", "", raw)
        digits = "".join(ch for ch in raw if ch.isdigit())
        digits_trim = digits.lstrip("0")

        tokens = {raw, compact}
        if digits:
            tokens.add(digits)
        if digits_trim:
            tokens.add(digits_trim)
            # Common short form in payment purpose: ут-219 for УТ-000219.
            tokens.add(f"ут-{digits_trim}")
            tokens.add(f"ут{digits_trim}")

        return {t for t in tokens if len(t) >= 3}

    for batch in _iterate_tail_pages(
        "Document_ЗаказКлиента",
        headers,
        ["Ref_Key", "Date", "Number", "ДокументОснование", "ДокументОснование_Type"],
    ) or []:
        for item in batch:
            base_type = str(item.get("ДокументОснование_Type") or "")
            base_ref = str(item.get("ДокументОснование") or "")
            order_ref = str(item.get("Ref_Key") or "")
            if (
                order_ref
                and base_type == "StandardODATA.Document_КоммерческоеПредложениеКлиенту"
                and base_ref in kp_ref_set
            ):
                kp_to_orders[base_ref].add(order_ref)
                order_to_kp[order_ref] = base_ref
                order_tokens[order_ref] = build_order_tokens(str(item.get("Number") or ""))

    target_order_refs = set(order_to_kp.keys())
    if not target_order_refs:
        for row in rows:
            if row.get("refKey") in kp_ref_set:
                row["invoiceCreated"] = False
                row["paymentReceived"] = False
        return

    invoice_order_refs: set[str] = set()
    for batch in _iterate_tail_pages(
        "Document_РеализацияТоваровУслуг",
        headers,
        ["Ref_Key", "Date", "ЗаказКлиента", "ЗаказКлиента_Type"],
    ) or []:
        for item in batch:
            order_type = str(item.get("ЗаказКлиента_Type") or "")
            order_ref = str(item.get("ЗаказКлиента") or "")
            if order_type == "StandardODATA.Document_ЗаказКлиента" and order_ref in target_order_refs:
                invoice_order_refs.add(order_ref)

    payment_order_refs: set[str] = set()
    for batch in _iterate_tail_pages(
        "Document_ПоступлениеБезналичныхДенежныхСредств",
        headers,
        ["Ref_Key", "Date", "ОбъектРасчетов_Key", "ДокументОснование", "ДокументОснование_Type", "НазначениеПлатежа"],
    ) or []:
        for item in batch:
            settlement_order = str(item.get("ОбъектРасчетов_Key") or "")
            if settlement_order in target_order_refs:
                payment_order_refs.add(settlement_order)
                continue

            base_type = str(item.get("ДокументОснование_Type") or "")
            base_ref = str(item.get("ДокументОснование") or "")
            if base_type == "StandardODATA.Document_ЗаказКлиента" and base_ref in target_order_refs:
                payment_order_refs.add(base_ref)
                continue

            # Fallback: if explicit links are absent, detect order number in payment purpose.
            purpose = str(item.get("НазначениеПлатежа") or "").lower()
            if purpose:
                for order_ref, tokens in order_tokens.items():
                    if order_ref in payment_order_refs:
                        continue
                    if any(token and token in purpose for token in tokens):
                        payment_order_refs.add(order_ref)

    kp_invoice_map = {kp: False for kp in kp_ref_set}
    kp_payment_map = {kp: False for kp in kp_ref_set}

    for order_ref in invoice_order_refs:
        kp_ref = order_to_kp.get(order_ref)
        if kp_ref:
            kp_invoice_map[kp_ref] = True

    for order_ref in payment_order_refs:
        kp_ref = order_to_kp.get(order_ref)
        if kp_ref:
            kp_payment_map[kp_ref] = True

    for row in rows:
        kp_ref = row.get("refKey")
        if kp_ref in kp_ref_set:
            row["invoiceCreated"] = kp_invoice_map.get(kp_ref, False)
            row["paymentReceived"] = kp_payment_map.get(kp_ref, False)


def _fetch_kp_group_flags(ref_key: str, headers: dict) -> dict:
    if not ref_key:
        return {"invoiceCreated": None, "paymentReceived": None}

    if ref_key in _group_doc_flags_cache:
        return _group_doc_flags_cache[ref_key]

    order_filter = (
        "ДокументОснование_Type eq 'StandardODATA.Document_КоммерческоеПредложениеКлиенту' "
        f"and ДокументОснование eq guid'{ref_key}'"
    )
    orders_payload, orders_error = _get_json_with_retry(
        f"{BASE}/Document_ЗаказКлиента",
        headers,
        params={"$select": "Ref_Key", "$filter": order_filter, "$top": "200"},
        timeout=GROUP_CHECK_TIMEOUT_SECONDS,
        retries=2,
    )
    if orders_error:
        result = {"invoiceCreated": None, "paymentReceived": None}
        _group_doc_flags_cache[ref_key] = result
        return result

    orders = orders_payload.get("value", []) if isinstance(orders_payload, dict) else []
    if not orders:
        result = {"invoiceCreated": False, "paymentReceived": False}
        _group_doc_flags_cache[ref_key] = result
        return result

    has_invoice = False
    has_payment = False

    for order in orders:
        order_ref = str(order.get("Ref_Key") or "").strip()
        if not order_ref:
            continue

        if not has_invoice:
            real_filter = (
                "ЗаказКлиента_Type eq 'StandardODATA.Document_ЗаказКлиента' "
                f"and ЗаказКлиента eq guid'{order_ref}'"
            )
            real_payload, real_error = _get_json_with_retry(
                f"{BASE}/Document_РеализацияТоваровУслуг",
                headers,
                params={"$select": "Ref_Key", "$filter": real_filter, "$top": "1"},
                timeout=GROUP_CHECK_TIMEOUT_SECONDS,
                retries=2,
            )
            if real_error:
                result = {"invoiceCreated": None, "paymentReceived": None}
                _group_doc_flags_cache[ref_key] = result
                return result
            has_invoice = bool((real_payload or {}).get("value"))

        if not has_payment:
            pay_filters = [
                f"ОбъектРасчетов_Key eq guid'{order_ref}'",
                (
                    "ДокументОснование_Type eq 'StandardODATA.Document_ЗаказКлиента' "
                    f"and ДокументОснование eq guid'{order_ref}'"
                ),
            ]
            for pay_filter in pay_filters:
                pay_payload, pay_error = _get_json_with_retry(
                    f"{BASE}/Document_ПоступлениеБезналичныхДенежныхСредств",
                    headers,
                    params={"$select": "Ref_Key", "$filter": pay_filter, "$top": "1"},
                    timeout=GROUP_CHECK_TIMEOUT_SECONDS,
                    retries=2,
                )
                if pay_error:
                    result = {"invoiceCreated": None, "paymentReceived": None}
                    _group_doc_flags_cache[ref_key] = result
                    return result
                if bool((pay_payload or {}).get("value")):
                    has_payment = True
                    break

        if has_invoice and has_payment:
            break

    result = {"invoiceCreated": has_invoice, "paymentReceived": has_payment}
    _group_doc_flags_cache[ref_key] = result
    return result


def resolve_customer_name_for_ref(
    ref_key: str,
    headers: dict,
    doc: dict | None = None,
    use_cache: bool = True,
) -> str:
    if not ref_key:
        return ""
    if use_cache and ref_key in _customer_name_cache:
        return _customer_name_cache[ref_key]

    row = doc or _fetch_doc_by_ref(ref_key, headers, timeout=DOC_TIMEOUT_SECONDS)
    if not row:
        _customer_name_cache[ref_key] = ""
        return ""

    # Collect nav links: prefer "Контрагент", skip "Организация" (seller's own org)
    SELLER_KEYS = {"организация", "organisation", "organization"}
    CUSTOMER_KEYS = {"контрагент", "клиент", "покупатель"}

    priority_links = []
    fallback_links = []
    for k, v in row.items():
        if not k.endswith("@navigationLinkUrl"):
            continue
        field = k[: k.index("@")].lower()
        if any(s in field for s in SELLER_KEYS):
            continue  # skip seller's org link
        if any(c in field for c in CUSTOMER_KEYS):
            priority_links.append(v)
        else:
            fallback_links.append(v)

    # If priority (customer) links exist, only use them — never fall back to
    # unrelated links (e.g. Валюта → "руб.") when Контрагент is empty in 1C.
    nav_links = (priority_links if priority_links else fallback_links)[:NAV_LINK_LIMIT]

    best_description = ""
    best_score = 0

    for rel in nav_links:
        try:
            nav_resp = requests.get(
                f"{BASE}/{rel}",
                headers=headers,
                timeout=NAV_TIMEOUT_SECONDS,
                verify=False,
            )
            if nav_resp.status_code != 200:
                continue
            nav_obj = nav_resp.json()
            if not isinstance(nav_obj, dict):
                continue

            # If this is a priority (customer) link, use it immediately
            rel_url_lower = rel.lower()
            if any(c in rel_url_lower for c in CUSTOMER_KEYS):
                description = str(nav_obj.get("Description") or "").strip()
                if description:
                    best_description = description
                    break
                # Priority link exists but Description is empty → Клиент not filled
                continue

            candidate_score = score_customer_candidate(nav_obj)
            if candidate_score > best_score:
                best_score = candidate_score
                best_description = str(nav_obj.get("Description") or "").strip()
        except Exception:
            continue

    _customer_name_cache[ref_key] = best_description
    return best_description


def resolve_additional_info_for_ref(
    ref_key: str,
    headers: dict,
    doc: dict | None = None,
    use_cache: bool = True,
) -> str:
    if not ref_key:
        return ""
    if use_cache and ref_key in _additional_info_cache:
        return _additional_info_cache[ref_key]

    row = doc or _fetch_doc_by_ref(ref_key, headers, timeout=DOC_TIMEOUT_SECONDS)
    if not row:
        _additional_info_cache[ref_key] = ""
        return ""

    comment_line = first_line(row.get("Комментарий") or "")
    if comment_line:
        _additional_info_cache[ref_key] = comment_line
        return comment_line

    best_line = ""
    best_score = -1

    for key, value in row.items():
        if not isinstance(value, str):
            continue

        line = first_line(value)
        if not line:
            continue

        key_l = str(key).lower()
        if key_l.endswith("@navigationlinkurl") or key_l.endswith("_key"):
            continue
        if line.startswith("http://") or line.startswith("https://"):
            continue
        if re.fullmatch(r"[0-9a-fA-F-]{36}", line):
            continue
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}.*", line):
            continue

        score = 1
        if key_l == "комментарий":
            score += 10
        elif key_l == "прочаядополнительнаяинформациятекст":
            score += 6
        elif key_l == "дополнительнаяинформацияклиентуhtml":
            score += 4
        if len(line) >= 12:
            score += 2
        if any(ch.isalpha() for ch in line):
            score += 2
        if "@" in line or "-" in line or " " in line:
            score += 1
        if "{" in line or "}" in line:
            score -= 2

        if score > best_score:
            best_score = score
            best_line = line

    _additional_info_cache[ref_key] = best_line
    return best_line


def resolve_status_kp_from_requisites(requisites: list, headers: dict) -> str:
    if not isinstance(requisites, list):
        return ""

    for req in requisites:
        if not isinstance(req, dict):
            continue
        if str(req.get("Свойство_Key") or "").lower() != STATUS_KP_PROPERTY_KEY.lower():
            continue

        text_value = str(req.get("ТекстоваяСтрока") or "").strip()
        if text_value:
            return text_value

        value_guid = str(req.get("Значение") or "").strip()
        if not value_guid:
            continue

        if value_guid in _status_kp_value_cache:
            return _status_kp_value_cache[value_guid]

        try:
            value_resp = requests.get(
                f"{BASE}/Catalog_ЗначенияСвойствОбъектов(guid'{value_guid}')",
                headers=headers,
                timeout=NAV_TIMEOUT_SECONDS,
                verify=False,
            )
            if value_resp.status_code == 200:
                value_obj = value_resp.json()
                description = str(value_obj.get("Description") or "").strip()
                _status_kp_value_cache[value_guid] = description
                return description
        except Exception:
            continue

    return ""


def load_rows_from_file() -> list:
    path = Path(DATA_FILE)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    for row in data:
        if "customerName" not in row:
            row["customerName"] = ""
        row["clientFilled"] = bool((row.get("customerName") or "").strip())
        if "statusKp" not in row:
            row["statusKp"] = ""
        if "invoiceCreated" not in row:
            row["invoiceCreated"] = None
        if "paymentReceived" not in row:
            row["paymentReceived"] = None
        if "statusHash" not in row:
            row["statusHash"] = ""
    data.sort(key=lambda x: x.get("createdAt", ""), reverse=True)
    return data


def save_rows(rows: list) -> None:
    for row in rows:
        if "customerName" not in row:
            row["customerName"] = ""
        row["clientFilled"] = bool((row.get("customerName") or "").strip())
        if "statusKp" not in row:
            row["statusKp"] = ""
        if "invoiceCreated" not in row:
            row["invoiceCreated"] = None
        if "paymentReceived" not in row:
            row["paymentReceived"] = None
        if "statusHash" not in row:
            row["statusHash"] = ""
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def build_known_rows_lookup() -> dict:
    known = {}
    for source_row in load_rows_from_file() + list(_cached_rows):
        number = source_row.get("number")
        if number and number not in known:
            known[number] = source_row
    return known


def get_total_count(headers: dict) -> int:
    resp = requests.get(
        f"{BASE}/{ENTITY}/$count",
        headers=headers,
        timeout=30,
        verify=False,
    )
    resp.raise_for_status()
    return int(resp.text.strip())


def fetch_rows_from_odata() -> list:
    headers = _build_headers()
    known_rows = build_known_rows_lookup()

    rows = []
    page_size = 50

    total_count = get_total_count(headers)
    if total_count <= 0:
        return []

    skip = ((total_count - 1) // page_size) * page_size

    while True:
        params = {
            "$select": "Ref_Key," + ",".join(LIGHT_SELECT_FIELDS),
            "$top": str(page_size),
            "$skip": str(skip),
        }

        resp = None
        for _ in range(3):
            try:
                resp = requests.get(
                    f"{BASE}/{ENTITY}",
                    headers=headers,
                    params=params,
                    timeout=45,
                    verify=False,
                )
                if resp.status_code == 200:
                    break
            except requests.RequestException:
                resp = None
            time.sleep(1)

        if resp is None or resp.status_code != 200:
            break

        batch = resp.json().get("value", [])
        if not batch:
            break

        batch_dates = []

        for item in batch:
            ref_key = item.get("Ref_Key") or ""
            values = [item.get(f) or "" for f in LIGHT_SELECT_FIELDS]
            number, dt_raw, status, requisites = values[0], values[1], values[2], values[3]
            try:
                dt = datetime.fromisoformat(str(dt_raw).replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                continue

            batch_dates.append(dt)

            if TARGET_START <= dt <= TARGET_END:
                known_row = known_rows.get(number, {})

                # Detect changes in 1C requisites by hashing Статус + ДополнительныеРеквизиты.
                # If the hash changed compared to what we cached, force re-enrich this row.
                current_hash = hashlib.md5(
                    (str(status) + str(requisites)).encode("utf-8")
                ).hexdigest()[:10]
                requisites_changed = current_hash != known_row.get("statusHash", "")

                status_kp = known_row.get("statusKp", "")
                if not status_kp or requisites_changed:
                    status_kp = resolve_status_kp_from_requisites(requisites, headers)

                rows.append(
                    {
                        "refKey": str(ref_key),
                        "number": number,
                        "createdAt": dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "customerName": "" if requisites_changed else known_row.get("customerName", ""),
                        "status": status,
                        "statusKp": status_kp,
                        "additionalInfoFirstLine": "" if requisites_changed else known_row.get("additionalInfoFirstLine", ""),
                        "invoiceCreated": known_row.get("invoiceCreated"),
                        "paymentReceived": known_row.get("paymentReceived"),
                        "statusHash": current_hash,
                    }
                )

        if batch_dates and max(batch_dates) < TARGET_START:
            break

        if skip == 0:
            break

        skip = max(0, skip - page_size)

    rows.sort(key=lambda x: x["createdAt"], reverse=True)

    global _last_group_enrich
    now = datetime.now()
    group_age = (now - _last_group_enrich).total_seconds() if _last_group_enrich else None
    if group_age is None or group_age >= GROUP_ENRICH_INTERVAL_SECONDS:
        _enrich_group_flags_bulk(rows, headers)
        _last_group_enrich = now
    else:
        # Re-use cached invoiceCreated/paymentReceived from known_rows
        for row in rows:
            known = known_rows.get(row.get("number", ""), {})
            if row.get("invoiceCreated") is None:
                row["invoiceCreated"] = known.get("invoiceCreated")
            if row.get("paymentReceived") is None:
                row["paymentReceived"] = known.get("paymentReceived")

    enriched = 0
    for index, row in enumerate(rows):
        ref_key = row.get("refKey", "")
        if not ref_key:
            continue

        should_refresh_info = index < FORCE_INFO_REFRESH_TOP_ROWS
        should_refresh_customer = index < FORCE_INFO_REFRESH_TOP_ROWS
        # Also re-enrich if requisites changed (customerName/additionalInfo were cleared above)
        need_customer = should_refresh_customer or not (row.get("customerName") or "").strip()
        need_info = should_refresh_info or not (row.get("additionalInfoFirstLine") or "").strip()
        if enriched >= ENRICH_PER_REFRESH:
            break
        if not need_customer and not need_info:
            continue

        doc = {}
        if need_customer or need_info:
            doc = _fetch_doc_by_ref(ref_key, headers, timeout=DOC_TIMEOUT_SECONDS)
            if not doc and (need_customer or need_info):
                continue

        if need_info:
            line = resolve_additional_info_for_ref(
                ref_key,
                headers,
                doc=doc,
                use_cache=not should_refresh_info,
            )
            row["additionalInfoFirstLine"] = line

        if need_customer:
            customer = resolve_customer_name_for_ref(
                ref_key,
                headers,
                doc=doc,
                use_cache=not should_refresh_customer,
            )
            if customer:
                row["customerName"] = customer

        enriched += 1

    for row in rows:
        row.pop("refKey", None)
        row["clientFilled"] = bool((row.get("customerName") or "").strip())

    rows.sort(key=lambda x: x["createdAt"], reverse=True)
    return rows


def refresh_cache_and_file() -> None:
    global _cached_rows, _cached_fp, _last_refresh

    with _refresh_lock:
        try:
            fetched = fetch_rows_from_odata()
            if fetched:
                save_rows(fetched)
                _cached_rows = fetched
                _cached_fp = rows_fingerprint(fetched)
                _last_refresh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log(f"refresh success: {len(fetched)} rows")
                return

            log("refresh returned 0 rows, using file fallback")
        except Exception as exc:
            log(f"refresh failed: {exc}")

        fallback = load_rows_from_file()
        _cached_rows = fallback
        _cached_fp = rows_fingerprint(fallback)
        _last_refresh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def cache_is_stale() -> bool:
    if not _last_refresh:
        return True
    try:
        last = datetime.strptime(_last_refresh, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return True
    age = (datetime.now() - last).total_seconds()
    return age >= STALE_REFRESH_AFTER_SECONDS


async def trigger_refresh_if_stale() -> None:
    if not cache_is_stale():
        return
    task = getattr(app.state, "on_demand_refresh_task", None)
    if task and not task.done():
        return
    app.state.on_demand_refresh_task = asyncio.create_task(asyncio.to_thread(refresh_cache_and_file))


async def refresh_loop() -> None:
    while True:
        try:
            await asyncio.to_thread(refresh_cache_and_file)
        except Exception:
            pass
        await asyncio.sleep(REFRESH_SECONDS)


@app.on_event("startup")
async def on_startup() -> None:
    global _cached_rows, _cached_fp, _last_refresh
    _cached_rows = load_rows_from_file()
    _cached_fp = rows_fingerprint(_cached_rows)
    _last_refresh = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log(f"startup cache loaded: {len(_cached_rows)} rows")
    app.state.refresh_task = asyncio.create_task(refresh_loop())


@app.on_event("shutdown")
async def on_shutdown() -> None:
    task = getattr(app.state, "refresh_task", None)
    if task:
        task.cancel()


@app.get("/")
async def root():
    return FileResponse("index.html", media_type="text/html")


@app.get("/healthz")
async def healthz():
    return {
        "ok": True,
        "rows": len(_cached_rows),
        "lastRefresh": _last_refresh,
    }


def format_row_for_client(row: dict) -> dict:
    """Format row for API response: remove ПСУТ- prefix and time from date."""
    formatted = row.copy()
    if "number" in formatted:
        number = str(formatted["number"]).replace("ПСУТ-", "")
        formatted["number"] = number.lstrip("0") or "0"
    if "createdAt" in formatted:
        formatted["createdAt"] = formatted["createdAt"].split(" ")[0]
    return formatted


@app.get("/api/kp/all")
async def get_all_kp():
    await trigger_refresh_if_stale()
    return [format_row_for_client(row) for row in _cached_rows]


@app.websocket("/ws/kp")
async def ws_kp(websocket: WebSocket):
    await websocket.accept()
    previous_fp = ""

    try:
        while True:
            current_fp = _cached_fp
            if current_fp != previous_fp:
                previous_fp = current_fp
                await websocket.send_json(
                    {
                        "type": "rows",
                        "updatedAt": _last_refresh,
                        "rows": [format_row_for_client(row) for row in _cached_rows],
                    }
                )
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        return


app.mount("/", StaticFiles(directory=".", html=True), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
