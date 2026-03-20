"""Tests for Claude Code and Codex parsers."""

import json
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from sigil.parsers.claude import ClaudeParser
from sigil.parsers.codex import CodexParser


def _write_jsonl(path: Path, entries: List[Dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")


PUSHED_AT = datetime(2026, 3, 20, 12, 0, 0)


def _claude(path: Path) -> ClaudeParser:
    return ClaudeParser(device="mac", pushed_at=PUSHED_AT)


def _codex() -> CodexParser:
    return CodexParser(device="mac", pushed_at=PUSHED_AT)


class TestClaudeParser:
    def test_user_message(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            _write_jsonl(
                path,
                [
                    {
                        "type": "user",
                        "message": {"role": "user", "content": "Hello world"},
                        "uuid": "abc-123",
                        "timestamp": "2026-03-01T10:00:00Z",
                        "sessionId": "sess-1",
                        "cwd": "/Users/junaid/personal/sigil",
                        "version": "2.1.47",
                        "gitBranch": "main",
                    }
                ],
            )

            rows = list(_claude(path).parse_file(path))

            assert len(rows) == 1
            row = rows[0]
            assert row.session_system == "claude_code"
            assert row.entry_type == "user"
            assert row.message_role == "user"
            assert row.message_text == "Hello world"
            assert row.session_id == "sess-1"
            assert row.git_branch == "main"
            assert row.cli_version == "2.1.47"

    def test_assistant_with_tool_use(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            _write_jsonl(
                path,
                [
                    {
                        "type": "assistant",
                        "message": {
                            "role": "assistant",
                            "model": "claude-opus-4-6",
                            "content": [
                                {"type": "text", "text": "Let me read that."},
                                {
                                    "type": "tool_use",
                                    "name": "Read",
                                    "id": "toolu_1",
                                    "input": {"path": "/tmp/f.txt"},
                                },
                            ],
                            "usage": {
                                "input_tokens": 100,
                                "output_tokens": 50,
                                "cache_creation_input_tokens": 200,
                                "cache_read_input_tokens": 300,
                            },
                            "stop_reason": "tool_use",
                        },
                        "uuid": "def-456",
                        "timestamp": "2026-03-01T10:00:05Z",
                        "sessionId": "sess-1",
                        "cwd": "/Users/junaid/personal/sigil",
                        "requestId": "req_abc",
                    }
                ],
            )

            rows = list(_claude(path).parse_file(path))

            assert len(rows) == 1
            row = rows[0]
            assert row.model == "claude-opus-4-6"
            assert row.model_provider == "anthropic"
            assert row.input_tokens == 100
            assert row.output_tokens == 50
            assert row.cache_creation_tokens == 200
            assert row.cache_read_tokens == 300
            assert row.tool_name == "Read"
            assert row.tool_input is not None
            assert "f.txt" in row.tool_input
            assert row.request_id == "req_abc"
            assert row.stop_reason == "tool_use"

    def test_thinking_content_not_stored(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            _write_jsonl(
                path,
                [
                    {
                        "type": "assistant",
                        "message": {
                            "role": "assistant",
                            "content": [
                                {"type": "thinking", "thinking": "Secret reasoning here"},
                            ],
                            "usage": {"input_tokens": 10, "output_tokens": 5},
                        },
                        "uuid": "think-1",
                        "timestamp": "2026-03-01T10:00:00Z",
                        "sessionId": "sess-1",
                    }
                ],
            )

            rows = list(_claude(path).parse_file(path))

            assert len(rows) == 1
            assert rows[0].content_type == "thinking"
            assert rows[0].message_text is None

    def test_file_history_snapshot_skipped(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            _write_jsonl(
                path,
                [
                    {"type": "file-history-snapshot", "messageId": "m1", "snapshot": {}},
                    {
                        "type": "user",
                        "message": {"role": "user", "content": "hi"},
                        "uuid": "u1",
                        "timestamp": "2026-03-01T10:00:00Z",
                        "sessionId": "s1",
                    },
                ],
            )

            rows = list(_claude(path).parse_file(path))
            assert len(rows) == 1
            assert rows[0].entry_type == "user"

    def test_extras_captures_unknown_keys(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            _write_jsonl(
                path,
                [
                    {
                        "type": "user",
                        "message": {"role": "user", "content": "hi"},
                        "uuid": "u1",
                        "timestamp": "2026-03-01T10:00:00Z",
                        "sessionId": "s1",
                        "customField": "custom_value",
                        "anotherField": 42,
                    }
                ],
            )

            rows = list(_claude(path).parse_file(path))
            assert len(rows) == 1
            assert rows[0].extras["customField"] == "custom_value"
            assert rows[0].extras["anotherField"] == 42

    def test_malformed_lines_skipped(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            path.write_text(
                "not json\n"
                '{"type":"user","message":{"role":"user","content":"ok"},"uuid":"u1",'
                '"timestamp":"2026-03-01T10:00:00Z","sessionId":"s1"}\n'
            )

            rows = list(_claude(path).parse_file(path))
            assert len(rows) == 1

    def test_empty_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            path.write_text("")
            rows = list(_claude(path).parse_file(path))
            assert rows == []

    def test_tool_result_entry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            _write_jsonl(
                path,
                [
                    {
                        "type": "user",
                        "parentUuid": "parent-1",
                        "message": {
                            "role": "user",
                            "content": [
                                {
                                    "tool_use_id": "toolu_1",
                                    "type": "tool_result",
                                    "content": [{"type": "text", "text": "file contents here"}],
                                }
                            ],
                        },
                        "uuid": "tr-1",
                        "timestamp": "2026-03-01T10:00:00Z",
                        "sessionId": "s1",
                        "toolUseResult": [{"type": "text", "text": "file contents here"}],
                        "sourceToolAssistantUUID": "parent-1",
                    }
                ],
            )

            rows = list(_claude(path).parse_file(path))
            assert len(rows) == 1
            assert rows[0].content_type == "tool_result"
            assert rows[0].tool_result_text == "file contents here"
            assert rows[0].parent_uuid == "parent-1"


class TestCodexParser:
    def test_session_meta(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "rollout.jsonl"
            _write_jsonl(
                path,
                [
                    {
                        "timestamp": "2026-01-13T07:15:51.156Z",
                        "type": "session_meta",
                        "payload": {
                            "id": "019bb636-00e1-7672-801a-8b1f96bca308",
                            "timestamp": "2026-01-13T07:15:51.137Z",
                            "cwd": "/Users/junaid",
                            "originator": "codex_cli_rs",
                            "cli_version": "0.80.0",
                            "source": "cli",
                            "model_provider": "openai-chat-completions",
                        },
                    }
                ],
            )

            rows = list(_codex().parse_file(path))

            assert len(rows) == 1
            row = rows[0]
            assert row.session_system == "codex"
            assert row.session_id == "019bb636-00e1-7672-801a-8b1f96bca308"
            assert row.cli_version == "0.80.0"
            assert row.model_provider == "openai-chat-completions"
            assert row.entry_type == "session_meta"

    def test_user_message(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "rollout.jsonl"
            _write_jsonl(
                path,
                [
                    {
                        "timestamp": "2026-01-13T07:15:51Z",
                        "type": "session_meta",
                        "payload": {"id": "sess-1", "cwd": "/tmp"},
                    },
                    {
                        "timestamp": "2026-01-13T07:16:11Z",
                        "type": "event_msg",
                        "payload": {"type": "user_message", "message": "hello", "images": []},
                    },
                ],
            )

            rows = list(_codex().parse_file(path))

            assert len(rows) == 2
            user_row = rows[1]
            assert user_row.message_text == "hello"
            assert user_row.message_role == "user"
            assert user_row.session_id == "sess-1"

    def test_turn_context_sets_model(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "rollout.jsonl"
            _write_jsonl(
                path,
                [
                    {
                        "timestamp": "2026-01-13T07:15:51Z",
                        "type": "session_meta",
                        "payload": {"id": "sess-1", "cwd": "/tmp"},
                    },
                    {
                        "timestamp": "2026-01-13T07:16:11Z",
                        "type": "turn_context",
                        "payload": {
                            "cwd": "/tmp",
                            "model": "GPT-5",
                            "approval_policy": "on-request",
                        },
                    },
                    {
                        "timestamp": "2026-01-13T07:16:12Z",
                        "type": "event_msg",
                        "payload": {"type": "user_message", "message": "hi"},
                    },
                ],
            )

            rows = list(_codex().parse_file(path))

            assert len(rows) == 3
            assert rows[2].model == "GPT-5"

    def test_response_item_message(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "rollout.jsonl"
            _write_jsonl(
                path,
                [
                    {
                        "timestamp": "2026-01-13T07:16:15Z",
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": "Hello! How can I help?"}],
                        },
                    }
                ],
            )

            rows = list(_codex().parse_file(path))

            assert len(rows) == 1
            assert rows[0].message_text == "Hello! How can I help?"
            assert rows[0].message_role == "assistant"

    def test_extras_captures_payload_overflow(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "rollout.jsonl"
            _write_jsonl(
                path,
                [
                    {
                        "timestamp": "2026-01-13T07:16:11Z",
                        "type": "turn_context",
                        "payload": {
                            "cwd": "/tmp",
                            "model": "GPT-5",
                            "sandbox_policy": {
                                "type": "workspace-write",
                                "network_access": False,
                            },
                            "truncation_policy": {"mode": "bytes", "limit": 10000},
                        },
                    }
                ],
            )

            rows = list(_codex().parse_file(path))

            assert len(rows) == 1
            payload_extras = rows[0].extras.get("payload", {})
            assert "sandbox_policy" in payload_extras
            assert "truncation_policy" in payload_extras

    def test_empty_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "rollout.jsonl"
            path.write_text("")
            rows = list(_codex().parse_file(path))
            assert rows == []
