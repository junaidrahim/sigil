"""Tests for the local storage backend."""

import tempfile
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
