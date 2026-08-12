import asyncio
import importlib.util
import inspect
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


def test_stage1_4_does_not_run_a_second_comment_fetch_pass():
    endpoint_source = inspect.getsource(module.manual_refresh_stage1_4)

    assert "refresh_comment_first_line_only(" not in endpoint_source


def test_payment_number_fallback_requires_same_year():
    payment_2025 = {
        "orderRefs": [],
        "purposeNums": ["768"],
        "year": 2025,
    }

    assert module._payment_matches_order_identity("order-2026", "768", 2026, payment_2025) is False
    assert module._payment_matches_order_identity("order-2025", "768", 2025, payment_2025) is True


def test_exact_payment_order_reference_ignores_number_reuse():
    payment = {
        "orderRefs": ["order-2026"],
        "purposeNums": ["768"],
        "year": 2025,
    }

    assert module._payment_matches_order_identity("order-2026", "768", 2026, payment) is True


def test_block3_does_not_match_same_order_number_from_another_year(monkeypatch):
    kp_ref = "kp-2026"
    order = {
        "Ref_Key": "order-2026",
        "Date": "2026-08-07T14:07:11",
        "Number": "ПСУТ-000768",
        "ДокументОснование": kp_ref,
        "ДокументОснование_Type": "StandardODATA.Document_КоммерческоеПредложениеКлиенту",
    }
    payment = {
        "Ref_Key": "payment-2025",
        "Date": "2025-09-17T23:59:59",
        "Number": "ПСУТ-000511",
        "НазначениеПлатежа": "Оплата по счету № 768",
    }

    monkeypatch.setattr(module, "_load_order_cache", lambda: None)
    monkeypatch.setattr(module, "_order_to_kp_cache", {})
    monkeypatch.setattr(module, "_load_payment_seed", lambda: None)
    monkeypatch.setattr(module, "_payment_seed", [])
    monkeypatch.setattr(module, "_collect_tail_pages", lambda *args, **kwargs: ([[order]], True))
    monkeypatch.setattr(
        module,
        "_collect_tail_pages_with_field_fallback",
        lambda *args, **kwargs: ([[payment]], True, module.PAYMENT_MATCH_SELECT_FIELD_CANDIDATES[0]),
    )

    result = module._build_payment_match_table(
        {},
        target_rows=[{"refKey": kp_ref, "number": "ПСУТ-000751"}],
    )

    assert not any(row.get("match") == "СОВПАДЕНИЕ" for row in result["rows"])


def test_authoritative_block3_result_clears_stale_payment_flag(monkeypatch):
    previous_rows = list(module._cached_rows)
    previous_stage1 = dict(module._stage1_4_refresh_state)
    try:
        module._cached_rows = [
            {"refKey": "kp-2026", "number": "ПСУТ-000751", "paymentReceived": True},
        ]
        module._stage1_4_refresh_state["running"] = False
        monkeypatch.setattr(module, "save_rows", lambda *args, **kwargs: True)

        result = module._persist_payment_match_result_to_cache([], authoritative=True)

        assert result["demoted"] == 1
        assert module._cached_rows[0]["paymentReceived"] is False
    finally:
        module._cached_rows = previous_rows
        module._stage1_4_refresh_state.clear()
        module._stage1_4_refresh_state.update(previous_stage1)


def test_complete_group_scan_clears_stale_payment_flag(monkeypatch):
    order_page_sizes = []

    def fake_collect(entity_name, *args, page_size=200, **kwargs):
        if entity_name == "Document_ЗаказКлиента":
            order_page_sizes.append(page_size)
        return [], True

    monkeypatch.setattr(module, "_collect_tail_pages", fake_collect)
    monkeypatch.setattr(
        module,
        "_collect_tail_pages_with_field_fallback",
        lambda *args, **kwargs: ([], True, module.PAYMENT_MATCH_SELECT_FIELD_CANDIDATES[0]),
    )
    monkeypatch.setattr(module, "_load_order_cache", lambda: None)
    monkeypatch.setattr(module, "_order_to_kp_cache", {})

    rows = [{"refKey": "kp-741", "number": "ПСУТ-000741", "paymentReceived": True}]
    result = module._enrich_group_flags_bulk(rows, {}, skip_invoice_scan=True)

    assert result["ordersScanComplete"] is True
    assert result["paymentsScanComplete"] is True
    assert order_page_sizes == [200]
    assert rows[0]["paymentReceived"] is False


def test_group_scan_returns_ready_block3_match_table(monkeypatch):
    order = {
        "Ref_Key": "order-741",
        "Date": "2026-08-01T12:00:00",
        "Number": "ПСУТ-000741",
        "ДокументОснование": "kp-741",
        "ДокументОснование_Type": "StandardODATA.Document_КоммерческоеПредложениеКлиенту",
    }
    payment = {
        "Ref_Key": "payment-1",
        "Date": "2026-08-02T12:00:00",
        "Number": "ПСУТ-000123",
        "НазначениеПлатежа": "Оплата по счету № 741",
    }

    monkeypatch.setattr(module, "_collect_tail_pages", lambda *args, **kwargs: ([[order]], True))
    monkeypatch.setattr(
        module,
        "_collect_tail_pages_with_field_fallback",
        lambda *args, **kwargs: ([[payment]], True, module.PAYMENT_MATCH_SELECT_FIELD_CANDIDATES[0]),
    )
    monkeypatch.setattr(module, "_load_order_cache", lambda: None)
    monkeypatch.setattr(module, "_order_to_kp_cache", {})
    monkeypatch.setattr(module, "_save_order_cache", lambda: None)

    rows = [{"refKey": "kp-741", "number": "ПСУТ-000741", "paymentReceived": False}]
    result = module._enrich_group_flags_bulk(rows, {}, skip_invoice_scan=True)

    assert result["matchTable"] == [{
        "kpNum": "741",
        "orderNum": "741",
        "payNum": "123",
        "purposeNum": "741",
        "match": "СОВПАДЕНИЕ",
    }]
    assert rows[0]["paymentReceived"] is True


def test_block3_page_uses_stage4_4_without_payment_match_rescan():
    page = (ROOT / "admin_block3_match.html").read_text(encoding="utf-8")

    assert "fetch('/api/kp/refresh/stage4_4'" in page
    assert "fetch('/api/kp/refresh/stage4_4/status'" in page
    assert "/api/admin/payment-match-table" not in page


def test_runtime_write_guard_skips_when_existing_snapshot_is_newer():
    started_at = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
    current_generated_at = datetime(2026, 8, 10, 12, 5, tzinfo=timezone.utc)

    assert module._should_skip_runtime_save(started_at, current_generated_at) is True


def test_runtime_pointer_cas_uses_exact_expected_sha(monkeypatch):
    captured = {}

    class Response:
        status_code = 200
        text = ""

    def fake_put(url, headers, json, timeout):
        captured["body"] = json
        return Response()

    monkeypatch.setattr(module, "GITHUB_TOKEN", "token")
    monkeypatch.setattr(module, "GITHUB_REPO", "owner/repo")
    monkeypatch.setattr(module.requests, "put", fake_put)

    result = module._compare_and_swap_github_json("current.json", {"version": 8}, "promote", "sha-7")

    assert result == "updated"
    assert captured["body"]["sha"] == "sha-7"


def test_strict_runtime_never_selects_newer_local_draft(monkeypatch):
    monkeypatch.setattr(module, "RUNTIME_STRICT_GITHUB_POINTER", True)
    local_rows = [{"number": "local"}]
    github_rows = [{"number": "github"}]

    source, rows, _, _ = module._runtime_pick_authoritative_state(
        local_rows,
        {"status": "draft", "generatedAt": "2026-08-10T13:00:00+00:00"},
        {"status": "draft"},
        github_rows,
        {"status": "confirmed", "cycleVersion": 7, "generatedAt": "2026-08-10T12:00:00+00:00"},
        {"status": "confirmed", "version": 7},
    )

    assert source == "github"
    assert rows == github_rows


def test_save_rows_creates_unversioned_local_draft(monkeypatch, tmp_path):
    runtime_path = tmp_path / "runtime.json"
    meta_path = tmp_path / "meta.json"
    pointer_path = tmp_path / "current.json"
    monkeypatch.setattr(module, "RUNTIME_DATA_FILE", str(runtime_path))
    monkeypatch.setattr(module, "RUNTIME_META_FILE", str(meta_path))
    monkeypatch.setattr(module, "RUNTIME_CURRENT_FILE", str(pointer_path))
    monkeypatch.setattr(module, "_stage1_4_blocks_runtime_writer", lambda source: False)

    assert module.save_rows([{"number": "1"}], push_to_github=False) is True

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    assert meta["status"] == "draft"
    assert "cycleVersion" not in meta
    assert pointer["status"] == "draft"
    assert "version" not in pointer
    assert pointer["snapshotId"] == meta["snapshotId"]
    assert pointer["rowsFingerprint"] == meta["rowsFingerprint"]


def test_publish_reallocates_version_after_pointer_cas_conflict(monkeypatch, tmp_path):
    rows = [{"number": "1"}]
    fingerprint = module.rows_fingerprint(rows)
    state = {
        "pointer": {"status": "confirmed", "version": 10, "rowsFingerprint": "old"},
        "sha": "sha-10",
    }
    attempts = []

    def fake_load_with_sha(path):
        return dict(state["pointer"]), state["sha"]

    def fake_cas(path, payload, message, expected_sha):
        attempts.append((payload["version"], expected_sha))
        if len(attempts) == 1:
            state["pointer"] = {"status": "confirmed", "version": 11, "rowsFingerprint": "competitor"}
            state["sha"] = "sha-11"
            return "conflict"
        state["pointer"] = dict(payload)
        state["sha"] = "sha-12"
        return "updated"

    monkeypatch.setattr(module, "_push_json_to_github_path", lambda *args, **kwargs: True)
    monkeypatch.setattr(module, "_load_json_with_sha_from_github_path", fake_load_with_sha)
    monkeypatch.setattr(module, "_compare_and_swap_github_json", fake_cas)
    monkeypatch.setattr(module, "_load_runtime_current_pointer_from_github", lambda: dict(state["pointer"]))
    monkeypatch.setattr(module, "_write_local_confirmed_runtime", lambda *args: None)

    _, confirmed_meta, confirmed_pointer = module._publish_confirmed_runtime_snapshot_or_raise(
        rows,
        {
            "status": "draft",
            "snapshotId": "snapshot-a",
            "rowsFingerprint": fingerprint,
            "generatedAt": "2026-08-10T10:00:00+00:00",
        },
    )

    assert attempts == [(11, "sha-10"), (12, "sha-11")]
    assert confirmed_pointer["version"] == 12
    assert confirmed_meta["cycleVersion"] == 12
    assert confirmed_meta["snapshotId"] == "snapshot-a"


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
