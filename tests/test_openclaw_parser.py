"""Tests for the OpenClaw session log parser."""

import tempfile
from datetime import datetime
from pathlib import Path

from sigil.parsers.openclaw import OpenClawParser
from tests.helpers import write_jsonl

PUSHED_AT = datetime(2026, 3, 20, 12, 0, 0)


def _parser() -> OpenClawParser:
    return OpenClawParser(device="mac", pushed_at=PUSHED_AT)


class TestOpenClawParser:
    def test_session_entry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            write_jsonl(
                path,
                [
                    {
                        "type": "session",
                        "version": 3,
                        "id": "abc-123",
                        "timestamp": "2026-02-18T22:35:21.466Z",
                        "cwd": "/Users/junaid/glitch",
                    }
                ],
            )

            rows = list(_parser().parse_file(path))

            assert len(rows) == 1
            row = rows[0]
            assert row.session_system == "openclaw"
            assert row.session_id == "abc-123"
            assert row.entry_type == "session"
            assert row.cwd == "/Users/junaid/glitch"

    def test_model_change_propagates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            write_jsonl(
                path,
                [
                    {
                        "type": "session",
                        "version": 3,
                        "id": "sess-1",
                        "timestamp": "2026-02-18T22:35:21Z",
                        "cwd": "/tmp",
                    },
                    {
                        "type": "model_change",
                        "id": "mc-1",
                        "parentId": None,
                        "timestamp": "2026-02-18T22:35:21Z",
                        "provider": "anthropic",
                        "modelId": "claude-opus-4-6",
                    },
                    {
                        "type": "message",
                        "id": "msg-1",
                        "parentId": "mc-1",
                        "timestamp": "2026-02-18T22:35:22Z",
                        "message": {
                            "role": "user",
                            "content": [{"type": "text", "text": "hello"}],
                        },
                    },
                ],
            )

            rows = list(_parser().parse_file(path))

            assert len(rows) == 3
            # User message should inherit model from model_change
            assert rows[2].model == "claude-opus-4-6"
            assert rows[2].model_provider == "anthropic"

    def test_user_message(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            write_jsonl(
                path,
                [
                    {
                        "type": "message",
                        "id": "msg-1",
                        "parentId": None,
                        "timestamp": "2026-02-18T22:35:22Z",
                        "message": {
                            "role": "user",
                            "content": [{"type": "text", "text": "What is 2+2?"}],
                        },
                    }
                ],
            )

            rows = list(_parser().parse_file(path))

            assert len(rows) == 1
            assert rows[0].message_role == "user"
            assert rows[0].message_text == "What is 2+2?"
            assert rows[0].content_type == "text"

    def test_assistant_message_with_usage(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            write_jsonl(
                path,
                [
                    {
                        "type": "message",
                        "id": "msg-2",
                        "parentId": "msg-1",
                        "timestamp": "2026-02-18T22:35:23Z",
                        "message": {
                            "role": "assistant",
                            "content": [{"type": "text", "text": "4"}],
                            "model": "claude-opus-4-6",
                            "provider": "anthropic",
                            "usage": {
                                "input": 100,
                                "output": 50,
                                "cacheRead": 200,
                                "cacheWrite": 300,
                            },
                            "stopReason": "end_turn",
                        },
                    }
                ],
            )

            rows = list(_parser().parse_file(path))

            assert len(rows) == 1
            row = rows[0]
            assert row.message_role == "assistant"
            assert row.message_text == "4"
            assert row.model == "claude-opus-4-6"
            assert row.model_provider == "anthropic"
            assert row.input_tokens == 100
            assert row.output_tokens == 50
            assert row.cache_read_tokens == 200
            assert row.cache_creation_tokens == 300
            assert row.stop_reason == "end_turn"

    def test_tool_result_message(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            write_jsonl(
                path,
                [
                    {
                        "type": "message",
                        "id": "tr-1",
                        "parentId": "msg-1",
                        "timestamp": "2026-02-18T22:36:00Z",
                        "message": {
                            "role": "toolResult",
                            "toolCallId": "toolu_abc",
                            "toolName": "exec",
                            "content": [
                                {"type": "text", "text": "command output here"}
                            ],
                        },
                    }
                ],
            )

            rows = list(_parser().parse_file(path))

            assert len(rows) == 1
            row = rows[0]
            assert row.content_type == "tool_result"
            assert row.tool_name == "exec"
            assert row.tool_result_text == "command output here"
            assert row.message_role == "tool"

    def test_custom_entry_type(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            write_jsonl(
                path,
                [
                    {
                        "type": "custom",
                        "customType": "model-snapshot",
                        "data": {
                            "timestamp": 1771454121467,
                            "provider": "anthropic",
                            "modelApi": "anthropic-messages",
                            "modelId": "claude-opus-4-6",
                        },
                        "id": "snap-1",
                        "parentId": None,
                        "timestamp": "2026-02-18T22:35:21Z",
                    }
                ],
            )

            rows = list(_parser().parse_file(path))

            assert len(rows) == 1
            assert rows[0].entry_type == "custom:model-snapshot"

    def test_thinking_level_change(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            write_jsonl(
                path,
                [
                    {
                        "type": "thinking_level_change",
                        "id": "tlc-1",
                        "parentId": None,
                        "timestamp": "2026-02-18T22:35:21Z",
                        "thinkingLevel": "low",
                    }
                ],
            )

            rows = list(_parser().parse_file(path))

            assert len(rows) == 1
            assert rows[0].entry_type == "thinking_level_change"

    def test_parent_id_mapped(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            write_jsonl(
                path,
                [
                    {
                        "type": "message",
                        "id": "msg-2",
                        "parentId": "msg-1",
                        "timestamp": "2026-02-18T22:35:22Z",
                        "message": {
                            "role": "assistant",
                            "content": [{"type": "text", "text": "hi"}],
                        },
                    }
                ],
            )

            rows = list(_parser().parse_file(path))

            assert len(rows) == 1
            assert rows[0].parent_uuid == "msg-1"

    def test_empty_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            path.write_text("")
            rows = list(_parser().parse_file(path))
            assert rows == []

    def test_extras_captures_unknown_keys(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            write_jsonl(
                path,
                [
                    {
                        "type": "message",
                        "id": "msg-1",
                        "parentId": None,
                        "timestamp": "2026-02-18T22:35:22Z",
                        "message": {
                            "role": "user",
                            "content": [{"type": "text", "text": "hi"}],
                        },
                        "unknownField": "some_value",
                    }
                ],
            )

            rows = list(_parser().parse_file(path))

            assert len(rows) == 1
            assert rows[0].extras["unknownField"] == "some_value"

    def test_assistant_with_tool_use(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "session.jsonl"
            write_jsonl(
                path,
                [
                    {
                        "type": "message",
                        "id": "msg-1",
                        "parentId": None,
                        "timestamp": "2026-02-18T22:35:22Z",
                        "message": {
                            "role": "assistant",
                            "content": [
                                {"type": "text", "text": "Let me check that."},
                                {
                                    "type": "tool_use",
                                    "name": "Read",
                                    "id": "toolu_1",
                                    "input": {"path": "/tmp/test.txt"},
                                },
                            ],
                            "model": "claude-opus-4-6",
                            "stopReason": "tool_use",
                        },
                    }
                ],
            )

            rows = list(_parser().parse_file(path))

            assert len(rows) == 1
            row = rows[0]
            assert row.content_type == "tool_use"
            assert row.tool_name == "Read"
            assert row.tool_input is not None
            assert "test.txt" in row.tool_input
            assert row.stop_reason == "tool_use"
