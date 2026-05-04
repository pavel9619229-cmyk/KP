#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import base64
import json
from datetime import datetime
from html import unescape
from pathlib import Path
import re
import time

import requests
import urllib3

urllib3.disable_warnings()

BASE = "https://aclient.1c-hosting.com/1R88669/1R88669_UT11_bfimz0bdj3/odata/standard.odata"
USERNAME = "павел"
PASSWORD = "1"

ENTITY = "Document_КоммерческоеПредложениеКлиенту"

# UTF-8 Basic Auth для кириллического логина
creds = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode("utf-8")).decode("ascii")
HEADERS = {
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


select_fields = [
    "Number",
    "Date",
    "Статус",
    "ПрочаяДополнительнаяИнформацияТекст",
    "Комментарий",
]

target_start = datetime(2026, 3, 1, 0, 0, 0)
target_end = datetime(2026, 6, 30, 23, 59, 59)

result = []
skip = 0
page_size = 500
pages_scanned = 0

while True:
    params = {
        "$select": ",".join(select_fields),
        "$top": str(page_size),
        "$skip": str(skip),
    }

    url = f"{BASE}/{ENTITY}"
    resp = None
    for _ in range(3):
        resp = requests.get(url, headers=HEADERS, params=params, timeout=90, verify=False)
        if resp.status_code == 200:
            break
        time.sleep(1)

    if resp is None or resp.status_code != 200:
        print(f"HTTP error at skip={skip}: {None if resp is None else resp.status_code}")
        break

    rows = resp.json().get("value", [])
    if not rows:
        break

    pages_scanned += 1

    for row in rows:
        # В текущей публикации русские ключи в JSON могут приходить искаженными,
        # поэтому берем значения по порядку в соответствии с $select.
        values = list(row.values())
        number = values[0] if len(values) > 0 else ""
        dt_raw = values[1] if len(values) > 1 else ""
        status = values[2] if len(values) > 2 else ""
        info_text = values[3] if len(values) > 3 else ""
        comment = values[4] if len(values) > 4 else ""

        try:
            dt = datetime.fromisoformat(str(dt_raw).replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            continue

        if target_start <= dt <= target_end:
            result.append(
                {
                    "number": number,
                    "createdAt": dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "status": status,
                    "additionalInfoFirstLine": first_line(info_text, comment),
                }
            )

    skip += page_size

print(f"Найдено документов за март-апрель 2026: {len(result)}")
print(f"Просканировано страниц: {pages_scanned}")
 
# Краткий табличный вывод в консоль (первые 30)
for i, r in enumerate(result[:30], start=1):
    print(f"{i:>3}. {r['number']:<20} | {r['createdAt']:<19} | {r['status']:<30} | {r['additionalInfoFirstLine']}")

# Сохранение в файл
output_path = Path(__file__).resolve().parent.parent / "data" / "kp_2026_march_april.json"
with output_path.open("w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

print(f"Сохранено: {output_path}")
