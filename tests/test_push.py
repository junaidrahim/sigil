"""Tests for push auto-detection and orchestration."""

import json
import tempfile
from pathlib import Path
from typing import Dict, List
from unittest.mock import patch

from sigil.push import auto_detect_sources, discover_session_files, push_all


def _write_jsonl(path: Path, entries: List[Dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")


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
            _write_jsonl(
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
            _write_jsonl(
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
