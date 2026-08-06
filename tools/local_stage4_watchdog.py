#!/usr/bin/env python3
"""Local Windows watchdog for stage4/4 bridge.

Flow:
1) Poll Render queue claim endpoint.
2) If claimed, run local 1C payments-only refresh (4/4 fast mode).
3) Publish confirmed versioned runtime snapshot to GitHub.
4) Report result back to Render queue status endpoint.

Required env vars (in local .env):
- GITHUB_TOKEN
- LOCAL_STAGE4_AGENT_TOKEN

Optional env vars:
- LOCAL_STAGE4_QUEUE_BASE_URL (default: https://onec-kp-realtime.onrender.com)
- LOCAL_STAGE4_AGENT_ID (default: hostname)
- LOCAL_STAGE4_POLL_SECONDS (default: 5)
"""

from __future__ import annotations

import os
import random
import socket
import sys
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


REPO_ROOT = Path(__file__).resolve().parents[1]
_load_dotenv(REPO_ROOT / ".env")

QUEUE_BASE_URL = os.getenv("LOCAL_STAGE4_QUEUE_BASE_URL", "https://onec-kp-realtime.onrender.com").rstrip("/")
AGENT_TOKEN = os.getenv("LOCAL_STAGE4_AGENT_TOKEN", "").strip()
AGENT_ID = os.getenv("LOCAL_STAGE4_AGENT_ID", socket.gethostname()).strip() or "local-agent"
POLL_SECONDS = max(2, int(os.getenv("LOCAL_STAGE4_POLL_SECONDS", "5")))
CLAIM_TIMEOUT_SECONDS = max(5, int(os.getenv("LOCAL_STAGE4_CLAIM_TIMEOUT_SECONDS", "20")))
REPORT_TIMEOUT_SECONDS = max(10, int(os.getenv("LOCAL_STAGE4_REPORT_TIMEOUT_SECONDS", "30")))
REQUEST_RETRIES = max(0, int(os.getenv("LOCAL_STAGE4_REQUEST_RETRIES", "3")))
RETRY_BACKOFF_SECONDS = float(os.getenv("LOCAL_STAGE4_RETRY_BACKOFF_SECONDS", "1.0"))

if not AGENT_TOKEN:
    print("ERROR: LOCAL_STAGE4_AGENT_TOKEN is not set.")
    print("Add LOCAL_STAGE4_AGENT_TOKEN=... to local .env")
    sys.exit(1)

if not os.getenv("GITHUB_TOKEN"):
    print("ERROR: GITHUB_TOKEN is not set.")
    print("Add GITHUB_TOKEN=... to local .env")
    sys.exit(1)

sys.path.insert(0, str(REPO_ROOT))
import api_proxy  # noqa: E402


CLAIM_URL = f"{QUEUE_BASE_URL}/api/kp/local-agent/stage4_4/claim"
REPORT_URL = f"{QUEUE_BASE_URL}/api/kp/local-agent/stage4_4/report"


def _build_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=REQUEST_RETRIES,
        connect=REQUEST_RETRIES,
        read=REQUEST_RETRIES,
        backoff_factor=max(0.0, RETRY_BACKOFF_SECONDS),
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


HTTP = _build_http_session()


def _headers() -> dict[str, str]:
    return {
        "X-Local-Agent-Token": AGENT_TOKEN,
        "X-Local-Agent-Id": AGENT_ID,
        "Content-Type": "application/json",
    }


def _claim_task() -> dict:
    resp = HTTP.post(CLAIM_URL, headers=_headers(), timeout=CLAIM_TIMEOUT_SECONDS)
    resp.raise_for_status()
    return resp.json()


def _report(payload: dict) -> None:
    resp = HTTP.post(REPORT_URL, headers=_headers(), json=payload, timeout=REPORT_TIMEOUT_SECONDS)
    resp.raise_for_status()


def _sleep_with_jitter(base_seconds: int) -> None:
    delay = max(1.0, float(base_seconds))
    delay = delay + random.uniform(0.0, min(2.5, delay * 0.25))
    time.sleep(delay)


def _run_local_stage4(task_id: str) -> dict:
    started = time.time()
    result_payload = {
        "taskId": task_id,
        "ok": False,
        "error": None,
        "paymentReceivedCount": None,
        "invoiceCreatedCount": None,
        "confirmedVersion": None,
        "source": "local-agent",
        "runner": AGENT_ID,
    }

    try:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] task {task_id}: refresh start")
        refresh_result = api_proxy.refresh_payments_only_for_cached_rows(
            f"local-stage4-agent:{AGENT_ID}",
            True,
            True,
        )

        result_payload["paymentReceivedCount"] = refresh_result.get("paymentReceivedCount")
        result_payload["invoiceCreatedCount"] = refresh_result.get("invoiceCreatedCount")

        if not refresh_result.get("ok"):
            result_payload["ok"] = False
            result_payload["error"] = str(refresh_result.get("error") or refresh_result.get("skipped") or "local refresh failed")
            return result_payload

        candidate_rows = api_proxy.load_rows_from_path(Path(api_proxy.RUNTIME_DATA_FILE))
        candidate_meta = api_proxy._read_runtime_meta()
        github_rows, _, github_pointer = api_proxy._publish_confirmed_runtime_snapshot_or_raise(
            candidate_rows,
            candidate_meta,
        )

        result_payload["ok"] = True
        result_payload["confirmedVersion"] = github_pointer.get("version") if github_pointer else None
        result_payload["source"] = "github-current"

        elapsed = time.time() - started
        print(
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] task {task_id}: done in {elapsed:.1f}s, "
            f"rows={len(github_rows)}, version={result_payload['confirmedVersion']}"
        )
        return result_payload
    except Exception as exc:
        result_payload["ok"] = False
        result_payload["error"] = f"{type(exc).__name__}: {exc}"
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] task {task_id}: failed: {result_payload['error']}")
        return result_payload


def main() -> int:
    print(f"Local stage4 watchdog started: base={QUEUE_BASE_URL}, agent={AGENT_ID}, poll={POLL_SECONDS}s")
    while True:
        try:
            claim = _claim_task()
            if not claim.get("claimed"):
                time.sleep(POLL_SECONDS)
                continue

            task_id = str(claim.get("taskId") or "").strip()
            if not task_id:
                time.sleep(POLL_SECONDS)
                continue

            payload = _run_local_stage4(task_id)
            _report(payload)
        except KeyboardInterrupt:
            print("Stopped by user")
            return 0
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else None
            if code == 503:
                print(
                    f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] watchdog HTTP 503: "
                    "Render side likely misses LOCAL_STAGE4_AGENT_TOKEN env or is temporarily unavailable"
                )
            elif code == 401:
                print(
                    f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] watchdog HTTP 401: "
                    "token mismatch between local agent and Render LOCAL_STAGE4_AGENT_TOKEN"
                )
            else:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] watchdog HTTP error: {code} {exc}")
            _sleep_with_jitter(POLL_SECONDS)
        except Exception as exc:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] watchdog loop error: {type(exc).__name__}: {exc}")
            _sleep_with_jitter(POLL_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
