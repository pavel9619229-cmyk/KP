#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Light watcher for near-real-time updates without 1C config changes.
- Polls lightweight fields every POLL_SECONDS.
- If light fingerprint changed, triggers refresh_seed.py.
- Runs heavy stage periodically (every HEAVY_EVERY_CHANGES change cycles).
"""

import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from api_proxy import _build_headers, _fetch_latest_kp_base_batch

STATE_PATH = ROOT / "data" / "kp_watch_state.json"
POLL_SECONDS = int(os.getenv("WATCH_POLL_SECONDS", "60"))
HEAVY_EVERY_CHANGES = int(os.getenv("WATCH_HEAVY_EVERY_CHANGES", "6"))
BASE_BATCH_SIZE = int(os.getenv("WATCH_BASE_BATCH_SIZE", "300"))


def now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{now()}] {msg}", flush=True)


def load_state() -> dict:
    if not STATE_PATH.exists():
        return {"last_fp": "", "changes_since_heavy": 0}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"last_fp": "", "changes_since_heavy": 0}


def save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def build_light_fingerprint(batch: list) -> str:
    parts = []
    for item in batch:
        parts.append(
            "|".join(
                [
                    str(item.get("Ref_Key") or ""),
                    str(item.get("Number") or ""),
                    str(item.get("Date") or ""),
                    str(item.get("Статус") or ""),
                    str(item.get("Комментарий") or ""),
                ]
            )
        )
    joined = "\n".join(parts)
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


def run_refresh(skip_heavy: bool) -> bool:
    cmd = [sys.executable, str(ROOT / "tools" / "refresh_seed.py")]
    if skip_heavy:
        cmd.append("--skip-heavy")
    mode = "FAST" if skip_heavy else "FULL"
    log(f"change detected -> refresh mode={mode}")
    result = subprocess.run(cmd, cwd=str(ROOT))
    return result.returncode == 0


def main() -> None:
    if HEAVY_EVERY_CHANGES < 1:
        raise ValueError("WATCH_HEAVY_EVERY_CHANGES must be >= 1")

    log(
        f"watcher started: poll={POLL_SECONDS}s, heavy_every_changes={HEAVY_EVERY_CHANGES}, batch={BASE_BATCH_SIZE}"
    )
    state = load_state()

    while True:
        try:
            headers = _build_headers()
            _, _, batch = _fetch_latest_kp_base_batch(headers, page_size=BASE_BATCH_SIZE)
            fp = build_light_fingerprint(batch)

            if not state.get("last_fp"):
                state["last_fp"] = fp
                state["changes_since_heavy"] = 0
                save_state(state)
                log("baseline fingerprint saved")
            elif fp != state.get("last_fp"):
                changes = int(state.get("changes_since_heavy", 0))
                do_heavy = changes >= (HEAVY_EVERY_CHANGES - 1)
                ok = run_refresh(skip_heavy=not do_heavy)
                if ok:
                    state["last_fp"] = fp
                    state["changes_since_heavy"] = 0 if do_heavy else (changes + 1)
                    save_state(state)
                    log(
                        f"refresh ok; changes_since_heavy={state['changes_since_heavy']}"
                    )
                else:
                    log("refresh failed; keep previous state")
            else:
                log("no light changes")
        except Exception as exc:
            log(f"watcher error: {type(exc).__name__}: {exc}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
