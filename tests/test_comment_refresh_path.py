import importlib.util
from datetime import datetime, timezone
from pathlib import Path

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
