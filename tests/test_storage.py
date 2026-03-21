"""Tests for the local storage backend."""

import tempfile
from datetime import datetime
from pathlib import Path

from sigil.storage.local import LocalStorage
from tests.helpers import make_row


def _count_parquet_files(base: Path) -> int:
    """Count .parquet files recursively under a directory."""
    return len(list(base.rglob("*.parquet")))


class TestLocalStorage:
    def test_append_writes_parquet(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LocalStorage(base_dir=Path(tmpdir))
            rows = [make_row(), make_row(session_id="sess-2")]
            saved = storage.append(iter(rows))

            assert saved == 2
            assert _count_parquet_files(Path(tmpdir)) >= 1

    def test_append_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LocalStorage(base_dir=Path(tmpdir))
            assert storage.append(iter([])) == 0

    def test_append_from_generator(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LocalStorage(base_dir=Path(tmpdir))

            def gen():
                yield make_row(session_id="s1")
                yield make_row(session_id="s2")
                yield make_row(session_id="s3")

            saved = storage.append(gen())
            assert saved == 3

    def test_append_chunked(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LocalStorage(base_dir=Path(tmpdir))
            rows = [make_row(session_id=f"s{i}") for i in range(5)]
            saved = storage.append(iter(rows), chunk_size=2)

            assert saved == 5
            # 5 rows with chunk_size=2 -> 3 flushes (2+2+1)
            assert _count_parquet_files(Path(tmpdir)) >= 3

    def test_max_timestamp_scoped_by_session_system(self):
        """Watermarks should be independent per parser (session_system)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LocalStorage(base_dir=Path(tmpdir))

            early = datetime(2026, 1, 1, 0, 0, 0)
            late = datetime(2026, 3, 15, 0, 0, 0)

            rows = [
                make_row(session_system="claude_code", timestamp=late, session_id="c1"),
                make_row(session_system="openclaw", timestamp=early, session_id="o1"),
            ]
            storage.append(iter(rows))

            # Global watermark (no system filter) returns the latest overall
            global_wm = storage.max_timestamp(device="test-mac")
            assert global_wm is not None
            assert global_wm.replace(tzinfo=None) == late

            # Per-parser watermarks are independent
            claude_wm = storage.max_timestamp(device="test-mac", session_system="claude_code")
            openclaw_wm = storage.max_timestamp(device="test-mac", session_system="openclaw")

            assert claude_wm is not None
            assert claude_wm.replace(tzinfo=None) == late
            assert openclaw_wm is not None
            assert openclaw_wm.replace(tzinfo=None) == early

            # Unknown system returns None
            assert storage.max_timestamp(device="test-mac", session_system="unknown") is None
