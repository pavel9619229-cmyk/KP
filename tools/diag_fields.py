#!/usr/bin/env python3
"""Diagnostic: test with full LIGHT_SELECT_FIELDS (Cyrillic) vs minimal."""
import requests, base64, urllib3
from datetime import datetime
urllib3.disable_warnings()

BASE = "https://aclient.1c-hosting.com/1R88669/1R88669_UT11_bfimz0bdj3/odata/standard.odata"
ENTITY = "Document_КоммерческоеПредложениеКлиенту"

creds = base64.b64encode("павел:1".encode("utf-8")).decode("ascii")
headers = {"Authorization": f"Basic {creds}", "Accept": "application/json"}

FIELDS = ["Number", "Date", "Статус", "ДополнительныеРеквизиты", "Комментарий"]

r = requests.get(f"{BASE}/{ENTITY}/$count", headers=headers, timeout=30, verify=False)
total = int(r.text.strip())
skip = ((total - 1) // 50) * 50
print(f"total={total}, skip={skip}")

params = {
    "$select": "Ref_Key," + ",".join(FIELDS),
    "$top": "50",
    "$skip": str(skip),
}

r2 = requests.get(f"{BASE}/{ENTITY}", headers=headers, params=params, timeout=45, verify=False)
print(f"status={r2.status_code}")

if r2.status_code == 200:
    batch = r2.json().get("value", [])
    print(f"batch_len={len(batch)}")
    if batch:
        item = batch[0]
        print(f"keys={list(item.keys())}")
        print(f"Number={item.get('Number')}")
        print(f"Date={item.get('Date')}")
        has_comment = bool(item.get("Комментарий"))
        has_status = bool(item.get("Статус"))
        print(f"has_comment={has_comment}, has_status={has_status}")
else:
    print(f"body[:500]={r2.text[:500]}")
