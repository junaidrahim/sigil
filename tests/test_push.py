"""Tests for push auto-detection and orchestration."""

import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from sigil.push import auto_detect_sources, discover_session_files, push_all
from tests.helpers import write_jsonl


class TestAutoDetect:
    def test_detects_existing_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            claude_dir = Path(tmpdir) / ".claude" / "projects"
            codex_dir = Path(tmpdir) / ".codex" / "sessions"
            claude_dir.mkdir(parents=True)
            codex_dir.mkdir(parents=True)

            with (
                patch("sigil.push.CLAUDE_SESSIONS_DIR", claude_dir),
                patch("sigil.push.CODEX_SESSIONS_DIR", codex_dir),
            ):
                sources = auto_detect_sources()

            assert len(sources) == 2
            systems = {s[0] for s in sources}
            assert "claude_code" in systems
            assert "codex" in systems

    def test_missing_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with (
                patch("sigil.push.CLAUDE_SESSIONS_DIR", Path(tmpdir) / "nope1"),
                patch("sigil.push.CODEX_SESSIONS_DIR", Path(tmpdir) / "nope2"),
            ):
                sources = auto_detect_sources()

            assert sources == []


class TestDiscoverSessionFiles:
    def test_finds_jsonl(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            (base / "proj1").mkdir()
            (base / "proj1" / "session.jsonl").touch()
            (base / "proj2").mkdir()
            (base / "proj2" / "session.jsonl").touch()
            (base / "readme.md").touch()

            files = discover_session_files(base)
            assert len(files) == 2

    def test_empty_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            assert discover_session_files(Path(tmpdir)) == []


class TestPushAll:
    def test_parses_both_systems(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Claude session
            claude_dir = Path(tmpdir) / "claude"
            write_jsonl(
                claude_dir / "proj" / "session.jsonl",
                [
                    {
                        "type": "user",
                        "message": {"role": "user", "content": "hello"},
                        "uuid": "u1",
                        "timestamp": "2026-03-01T10:00:00Z",
                        "sessionId": "cs1",
                    }
                ],
            )

            # Codex session
            codex_dir = Path(tmpdir) / "codex"
            write_jsonl(
                codex_dir / "2026" / "03" / "01" / "rollout.jsonl",
                [
                    {
                        "timestamp": "2026-03-01T10:00:00Z",
                        "type": "session_meta",
                        "payload": {"id": "xs1", "cwd": "/tmp"},
                    },
                    {
                        "timestamp": "2026-03-01T10:01:00Z",
                        "type": "event_msg",
                        "payload": {"type": "user_message", "message": "hi"},
                    },
                ],
            )

            sources = [("claude_code", claude_dir), ("codex", codex_dir)]
            rows = list(push_all("test-mac", sources=sources))

            claude_rows = [r for r in rows if r.session_system == "claude_code"]
            codex_rows = [r for r in rows if r.session_system == "codex"]

            assert len(claude_rows) == 1
            assert len(codex_rows) == 2
            assert claude_rows[0].session_id == "cs1"
            assert codex_rows[0].session_id == "xs1"

    def test_per_parser_watermarks(self):
        """Per-parser watermarks only filter their own system's rows."""
        with tempfile.TemporaryDirectory() as tmpdir:
            claude_dir = Path(tmpdir) / "claude"
            write_jsonl(
                claude_dir / "proj" / "session.jsonl",
                [
                    {
                        "type": "user",
                        "message": {"role": "user", "content": "old"},
                        "uuid": "u1",
                        "timestamp": "2026-03-01T10:00:00Z",
                        "sessionId": "cs1",
                    },
                    {
                        "type": "user",
                        "message": {"role": "user", "content": "new"},
                        "uuid": "u2",
                        "timestamp": "2026-03-10T10:00:00Z",
                        "sessionId": "cs1",
                    },
                ],
            )

            codex_dir = Path(tmpdir) / "codex"
            write_jsonl(
                codex_dir / "2026" / "03" / "01" / "rollout.jsonl",
                [
                    {
                        "timestamp": "2026-03-01T10:00:00Z",
                        "type": "session_meta",
                        "payload": {"id": "xs1", "cwd": "/tmp"},
                    },
                    {
                        "timestamp": "2026-03-01T10:01:00Z",
                        "type": "event_msg",
                        "payload": {"type": "user_message", "message": "hi"},
                    },
                ],
            )

            sources = [("claude_code", claude_dir), ("codex", codex_dir)]

            # Claude watermark at March 5 filters out the March 1 entry
            # but codex has no watermark so all its rows come through
            watermarks = {"claude_code": datetime(2026, 3, 5, 0, 0, 0, tzinfo=UTC)}
            rows = list(push_all("test-mac", sources=sources, watermarks=watermarks))

            claude_rows = [r for r in rows if r.session_system == "claude_code"]
            codex_rows = [r for r in rows if r.session_system == "codex"]

            # Only the March 10 claude row passes the watermark
            assert len(claude_rows) == 1
            assert claude_rows[0].message_text == "new"
            # All codex rows pass (no watermark for codex)
            assert len(codex_rows) == 2
