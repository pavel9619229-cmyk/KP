import asyncio
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

from starlette.requests import Request

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / 'api_proxy.py'

spec = importlib.util.spec_from_file_location('api_proxy', MODULE_PATH)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_first_line_refresh_uses_comment_from_doc():
    row = {'refKey': 'abc', 'additionalInfoFirstLine': 'old'}
    doc = {'Комментарий': 'new first line\nsecond line'}
    module.first_line('')
    # Simulate the same transformation used by refresh_comment_first_line_only.
    raw_comment = str(doc.get('Комментарий') or '')
    row['additionalInfoFirstLine'] = module.first_line(raw_comment) or row.get('additionalInfoFirstLine') or ''
    assert row['additionalInfoFirstLine'] == 'new first line'


def test_runtime_write_guard_skips_when_existing_snapshot_is_newer():
    started_at = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
    current_generated_at = datetime(2026, 8, 10, 12, 5, tzinfo=timezone.utc)

    assert module._should_skip_runtime_save(started_at, current_generated_at) is True


def test_stage1_4_blocks_other_runtime_writers():
    previous = dict(module._stage1_4_refresh_state)
    try:
        module._stage1_4_refresh_state["running"] = True
        assert module._stage1_4_blocks_runtime_writer("payments-only-refresh") is True
        assert module._stage1_4_blocks_runtime_writer("fast-partial-refresh") is True
        assert module._stage1_4_blocks_runtime_writer("manual-refresh-1of4:manager") is False
        assert module._stage1_4_blocks_runtime_writer("stage1-4-comment-first-line-refresh") is False
    finally:
        module._stage1_4_refresh_state.clear()
        module._stage1_4_refresh_state.update(previous)


def test_competing_refresh_endpoints_return_conflict_during_stage1_4():
    previous_stage1 = dict(module._stage1_4_refresh_state)
    previous_manual = dict(module._manual_refresh_state)
    previous_payments = dict(module._payments_only_state)
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/",
            "headers": [],
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
            "scheme": "http",
        }
    )

    try:
        module._stage1_4_refresh_state["running"] = True

        manual_response = asyncio.run(module.manual_refresh(request))
        payments_response = asyncio.run(module.payments_only_refresh(request))

        assert manual_response.status_code == 409
        assert payments_response.status_code == 409
        assert json.loads(manual_response.body)["blockers"] == ["stage1/4"]
        assert json.loads(payments_response.body)["blockers"] == ["stage1/4"]
        assert module._manual_refresh_state.get("running") == previous_manual.get("running")
        assert module._payments_only_state.get("running") == previous_payments.get("running")
    finally:
        module._stage1_4_refresh_state.clear()
        module._stage1_4_refresh_state.update(previous_stage1)
        module._manual_refresh_state.clear()
        module._manual_refresh_state.update(previous_manual)
        module._payments_only_state.clear()
        module._payments_only_state.update(previous_payments)


def test_internal_runtime_writers_skip_during_stage1_4():
    previous_stage1 = dict(module._stage1_4_refresh_state)
    previous_rows = list(module._cached_rows)
    previous_payments = dict(module._payments_only_state)
    try:
        module._stage1_4_refresh_state["running"] = True
        module._payments_only_state["running"] = False
        module._cached_rows = [{"number": "1", "refKey": "abc", "paymentReceived": False}]

        assert module.refresh_cached_rows_only()["skipped"] == "stage1-4-exclusive"
        assert module.refresh_payments_only_for_cached_rows()["skipped"] == "stage1-4-exclusive"
        assert module.refresh_payments_for_single_kp_from_seed("1")["skipped"] == "stage1-4-exclusive"
        assert module._persist_payment_match_result_to_cache(
            [{"match": "СОВПАДЕНИЕ", "kpNum": "1"}]
        )["reason"] == "stage1-4-exclusive"
    finally:
        module._stage1_4_refresh_state.clear()
        module._stage1_4_refresh_state.update(previous_stage1)
        module._cached_rows = previous_rows
        module._payments_only_state.clear()
        module._payments_only_state.update(previous_payments)


def test_stage1_4_endpoint_rejects_existing_runtime_writer():
    previous_stage1 = dict(module._stage1_4_refresh_state)
    previous_payments = dict(module._payments_only_state)
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/kp/refresh/stage1_4",
            "headers": [],
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
            "scheme": "http",
        }
    )

    try:
        module._stage1_4_refresh_state["running"] = False
        module._payments_only_state["running"] = True
        response = asyncio.run(module.manual_refresh_stage1_4(request))

        assert response.status_code == 409
        assert json.loads(response.body)["blockers"] == ["payments-only"]
        assert module._stage1_4_refresh_state.get("running") is False
    finally:
        module._stage1_4_refresh_state.clear()
        module._stage1_4_refresh_state.update(previous_stage1)
        module._payments_only_state.clear()
        module._payments_only_state.update(previous_payments)
