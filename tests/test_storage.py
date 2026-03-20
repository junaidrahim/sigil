"""Tests for the local storage backend."""

import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

from sigil.models import SessionRow
from sigil.storage.local import LocalStorage


def _make_row(
    session_system: str = "claude_code",
    session_id: str = "sess-1",
    timestamp: Optional[datetime] = None,
    tool_name: Optional[str] = None,
    input_tokens: Optional[int] = 100,
    output_tokens: Optional[int] = 200,
    model: Optional[str] = "claude-opus-4-6",
    **kwargs,
) -> SessionRow:
    """Create a test ``SessionRow`` with sensible defaults."""
    ts = timestamp or datetime(2026, 3, 15, 10, 0, 0)
    return SessionRow(
        row_id=f"row-{id(ts)}",
        session_id=session_id,
        session_system=session_system,
        device="test-mac",
        pushed_at=datetime(2026, 3, 20, 12, 0, 0),
        timestamp=ts,
        entry_type="assistant",
        message_role="assistant",
        message_text="hello",
        content_type="text",
        tool_name=tool_name,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        model=model,
        model_provider="anthropic",
        source_file="/tmp/test.jsonl",
        source_line=1,
        **kwargs,
    )


def _count_parquet_files(base: Path) -> int:
    """Count .parquet files recursively under a directory."""
    return len(list(base.rglob("*.parquet")))


class TestLocalStorage:
    def test_append_writes_parquet(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LocalStorage(base_dir=Path(tmpdir))
            rows = [_make_row(), _make_row(session_id="sess-2")]
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
                yield _make_row(session_id="s1")
                yield _make_row(session_id="s2")
                yield _make_row(session_id="s3")

            saved = storage.append(gen())
            assert saved == 3

    def test_append_chunked(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LocalStorage(base_dir=Path(tmpdir))
            rows = [_make_row(session_id=f"s{i}") for i in range(5)]
            saved = storage.append(iter(rows), chunk_size=2)

            assert saved == 5
            # 5 rows with chunk_size=2 -> 3 flushes (2+2+1)
            assert _count_parquet_files(Path(tmpdir)) >= 3
