"""Tests for the local storage backend."""

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from sigil.models import Message, Session, Snapshot, ToolUse
from sigil.storage.local import LocalStorage


def _make_snapshot(
    source: str = "personal",
    device: str = "test-machine",
    pushed_at: str | None = None,
    n_sessions: int = 1,
) -> Snapshot:
    sessions = []
    for i in range(n_sessions):
        sessions.append(
            Session(
                session_id=f"sess-{i}",
                source=source,
                device=device,
                started_at="2026-03-01T10:00:00",
                ended_at="2026-03-01T11:00:00",
                messages=[Message(role="user", text="hello")],
                tool_uses=[ToolUse(tool_name="Read", count=3)],
                input_tokens=100,
                output_tokens=200,
                total_tokens=300,
            )
        )
    snap = Snapshot(
        source=source,
        device=device,
        sessions=sessions,
    )
    if pushed_at:
        snap.pushed_at = pushed_at
    return snap


class TestLocalStorage:
    def test_save_and_retrieve(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LocalStorage(base_dir=Path(tmpdir))
            snap = _make_snapshot()
            sid = storage.save_snapshot(snap)

            loaded = storage.get_snapshot(sid)
            assert loaded is not None
            assert loaded.snapshot_id == snap.snapshot_id
            assert loaded.session_count == 1
            assert loaded.sessions[0].session_id == "sess-0"
            assert loaded.sessions[0].messages[0].text == "hello"
            assert loaded.sessions[0].tool_uses[0].tool_name == "Read"

    def test_list_all(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LocalStorage(base_dir=Path(tmpdir))
            storage.save_snapshot(_make_snapshot(source="work"))
            storage.save_snapshot(_make_snapshot(source="personal"))

            all_snaps = storage.list_snapshots()
            assert len(all_snaps) == 2

    def test_list_filter_by_source(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LocalStorage(base_dir=Path(tmpdir))
            storage.save_snapshot(_make_snapshot(source="work"))
            storage.save_snapshot(_make_snapshot(source="personal"))

            work = storage.list_snapshots(source="work")
            assert len(work) == 1
            assert work[0].source == "work"

    def test_list_filter_by_time(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LocalStorage(base_dir=Path(tmpdir))
            old = _make_snapshot(pushed_at="2025-01-01T00:00:00")
            new = _make_snapshot(pushed_at="2026-03-15T00:00:00")
            storage.save_snapshot(old)
            storage.save_snapshot(new)

            recent = storage.list_snapshots(since=datetime(2026, 1, 1))
            assert len(recent) == 1

    def test_get_nonexistent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LocalStorage(base_dir=Path(tmpdir))
            assert storage.get_snapshot("nonexistent") is None

    def test_roundtrip_preserves_tokens(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LocalStorage(base_dir=Path(tmpdir))
            snap = _make_snapshot(n_sessions=3)
            storage.save_snapshot(snap)

            loaded = storage.get_snapshot(snap.snapshot_id)
            assert loaded is not None
            for i, session in enumerate(loaded.sessions):
                assert session.input_tokens == 100
                assert session.output_tokens == 200
                assert session.total_tokens == 300
