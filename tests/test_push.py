"""Tests for push/session parsing."""

import json
import tempfile
from pathlib import Path

from sigil.config import SanitizeConfig
from sigil.push import build_snapshot, discover_session_files, parse_session_file


def _write_jsonl(path: Path, entries: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")


class TestDiscoverSessionFiles:
    def test_finds_jsonl_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            (base / "project1").mkdir()
            (base / "project1" / "session.jsonl").touch()
            (base / "project2").mkdir()
            (base / "project2" / "session.jsonl").touch()
            (base / "readme.md").touch()

            files = discover_session_files(base)
            assert len(files) == 2
            assert all(f.suffix == ".jsonl" for f in files)

    def test_empty_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            assert discover_session_files(Path(tmpdir)) == []


class TestParseSessionFile:
    def test_basic_parsing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            entries = [
                {
                    "role": "user",
                    "content": "Hello world",
                    "timestamp": "2026-03-01T10:00:00Z",
                    "usage": {"input_tokens": 10, "output_tokens": 0},
                },
                {
                    "role": "assistant",
                    "content": "Hi there!",
                    "timestamp": "2026-03-01T10:00:05Z",
                    "usage": {"input_tokens": 0, "output_tokens": 20},
                    "model": "claude-sonnet-4-6",
                },
            ]
            _write_jsonl(path, entries)

            config = SanitizeConfig()
            session = parse_session_file(path, "personal", "test-mac", config)

            assert session is not None
            assert session.source == "personal"
            assert session.device == "test-mac"
            assert session.input_tokens == 10
            assert session.output_tokens == 20
            assert session.total_tokens == 30
            assert session.model == "claude-sonnet-4-6"
            assert len(session.messages) == 2

    def test_sanitization_applied(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            entries = [
                {
                    "role": "user",
                    "content": "Check /Users/junaid/work/secret/file.py with key sk-abc123",
                    "timestamp": "2026-03-01T10:00:00Z",
                },
            ]
            _write_jsonl(path, entries)

            config = SanitizeConfig(
                strip_paths=["/Users/junaid/work/"],
                redact_patterns=[r"sk-[a-zA-Z0-9]+"],
                strip_code_blocks=True,
            )
            session = parse_session_file(path, "work", "test-mac", config)

            assert session is not None
            assert "secret" not in session.messages[0].text
            assert "sk-abc123" not in session.messages[0].text

    def test_tool_use_extraction(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            entries = [
                {
                    "role": "assistant",
                    "content": [
                        {"type": "text", "text": "Let me read that."},
                        {"type": "tool_use", "name": "Read", "input": {}},
                    ],
                    "timestamp": "2026-03-01T10:00:00Z",
                },
                {
                    "role": "assistant",
                    "content": [
                        {"type": "tool_use", "name": "Read", "input": {}},
                        {"type": "tool_use", "name": "Edit", "input": {}},
                    ],
                    "timestamp": "2026-03-01T10:00:10Z",
                },
            ]
            _write_jsonl(path, entries)

            config = SanitizeConfig()
            session = parse_session_file(path, "personal", "test-mac", config)

            assert session is not None
            tool_map = {t.tool_name: t.count for t in session.tool_uses}
            assert tool_map["Read"] == 2
            assert tool_map["Edit"] == 1

    def test_empty_file_returns_none(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            path.write_text("")

            config = SanitizeConfig()
            assert parse_session_file(path, "personal", "test", config) is None

    def test_malformed_json_skipped(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            path.write_text(
                'not json\n{"role":"user","content":"valid","timestamp":"2026-03-01T10:00:00Z"}\n'
            )

            config = SanitizeConfig()
            session = parse_session_file(path, "personal", "test", config)
            assert session is not None
            assert len(session.messages) == 1

    def test_project_path_sanitized(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            entries = [
                {
                    "role": "user",
                    "content": "hi",
                    "cwd": "/Users/junaid/work/secret-project",
                    "timestamp": "2026-03-01T10:00:00Z",
                },
            ]
            _write_jsonl(path, entries)

            config = SanitizeConfig(strip_paths=["/Users/junaid/work/"])
            session = parse_session_file(path, "work", "test", config)
            assert session is not None
            assert session.project_path == "[path redacted]"


class TestBuildSnapshot:
    def test_builds_from_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            _write_jsonl(
                base / "proj1" / "session.jsonl",
                [{"role": "user", "content": "hi", "timestamp": "2026-03-01T10:00:00Z"}],
            )
            _write_jsonl(
                base / "proj2" / "session.jsonl",
                [{"role": "user", "content": "bye", "timestamp": "2026-03-02T10:00:00Z"}],
            )

            config = SanitizeConfig()
            snapshot = build_snapshot(base, "personal", "test-mac", config)

            assert snapshot.source == "personal"
            assert snapshot.device == "test-mac"
            assert snapshot.session_count == 2
            assert len(snapshot.sessions) == 2
