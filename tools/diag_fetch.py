#!/usr/bin/env python3
"""Diagnostic: reproduce fetch_rows_from_odata logic locally."""
import requests, base64, urllib3
from datetime import datetime
urllib3.disable_warnings()

BASE = "https://aclient.1c-hosting.com/1R88669/1R88669_UT11_bfimz0bdj3/odata/standard.odata"
ENTITY = "Document_КоммерческоеПредложениеКлиенту"
TARGET_START = datetime(2026, 3, 1, 0, 0, 0)
TARGET_END = datetime(2026, 4, 30, 23, 59, 59)

creds = base64.b64encode("павел:1".encode("utf-8")).decode("ascii")
headers = {"Authorization": f"Basic {creds}", "Accept": "application/json"}

# Step 1: count
r = requests.get(f"{BASE}/{ENTITY}/$count", headers=headers, timeout=30, verify=False)
total = int(r.text.strip())
print(f"total_count={total}")

# Step 2: fetch last page
page_size = 50
skip = ((total - 1) // page_size) * page_size
print(f"starting skip={skip}")

matched = 0
pages = 0
while True:
    pages += 1
    params = {"$select": "Ref_Key,Number,Date", "$top": str(page_size), "$skip": str(skip)}
    r2 = requests.get(f"{BASE}/{ENTITY}", headers=headers, params=params, timeout=45, verify=False)
    if r2.status_code != 200:
        print(f"HTTP {r2.status_code} at skip={skip}")
        break
    batch = r2.json().get("value", [])
    if not batch:
        print(f"empty batch at skip={skip}")
        break

    batch_dates = []
    for item in batch:
        dt_raw = item.get("Date", "")
        num = item.get("Number", "")
        try:
            dt = datetime.fromisoformat(str(dt_raw).replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception as e:
            print(f"  PARSE ERROR: {num} -> {e}, raw={dt_raw}")
            continue
        batch_dates.append(dt)
        if TARGET_START <= dt <= TARGET_END:
            matched += 1
            if pages == 1 and matched <= 3:
                print(f"  MATCH: {num} date={dt}")

    if pages == 1:
        print(f"first batch: len={len(batch)}, dates range: {min(batch_dates)} .. {max(batch_dates)}")

    if batch_dates and max(batch_dates) < TARGET_START:
        print(f"stopping at skip={skip}, max date {max(batch_dates)} < TARGET_START")
        break

    if skip == 0:
        print(f"reached skip=0")
        break

    skip = max(0, skip - page_size)
    if pages % 50 == 0:
        print(f"  page {pages}, skip={skip}, matched so far={matched}")

print(f"\nRESULT: {matched} matched rows in {pages} pages")
