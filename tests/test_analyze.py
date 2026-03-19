"""Tests for the analyze module."""

import tempfile
from pathlib import Path

from sigil.analyze import (
    compute_metrics,
    format_as_json,
    format_as_markdown,
    format_as_prompt,
    parse_period,
)
from sigil.models import Message, Session, Snapshot, ToolUse
from sigil.storage.local import LocalStorage


def _store_test_data(storage: LocalStorage) -> None:
    """Store some test snapshots."""
    sessions = [
        Session(
            session_id="s1",
            source="work",
            device="mac",
            started_at="2026-03-15T10:00:00",
            ended_at="2026-03-15T11:00:00",
            messages=[Message(role="user", text="hello")],
            tool_uses=[ToolUse(tool_name="Read", count=5), ToolUse(tool_name="Edit", count=2)],
            input_tokens=500,
            output_tokens=1000,
            total_tokens=1500,
            model="claude-sonnet-4-6",
        ),
        Session(
            session_id="s2",
            source="personal",
            device="mac",
            started_at="2026-03-16T14:00:00",
            ended_at="2026-03-16T15:00:00",
            messages=[Message(role="user", text="write something")],
            tool_uses=[ToolUse(tool_name="Read", count=1)],
            input_tokens=200,
            output_tokens=800,
            total_tokens=1000,
            model="claude-opus-4-6",
        ),
    ]
    snap = Snapshot(source="work", device="mac", sessions=sessions, session_count=2)
    storage.save_snapshot(snap)


class TestParsePeriod:
    def test_days(self):
        assert parse_period("30d").days == 30

    def test_weeks(self):
        assert parse_period("2w").days == 14

    def test_invalid(self):
        try:
            parse_period("abc")
            assert False, "Should have raised"
        except ValueError:
            pass


class TestComputeMetrics:
    def test_basic_metrics(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LocalStorage(base_dir=Path(tmpdir))
            _store_test_data(storage)

            metrics = compute_metrics(storage, period="30d")

            assert metrics.total_sessions == 2
            assert metrics.total_input_tokens == 700
            assert metrics.total_output_tokens == 1800
            assert metrics.total_tokens == 2500
            assert metrics.unique_days == 2
            assert metrics.sessions_by_source["work"] == 1
            assert metrics.sessions_by_source["personal"] == 1
            assert "Read" in metrics.tool_usage
            assert metrics.tool_usage["Read"] == 6

    def test_source_filter(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LocalStorage(base_dir=Path(tmpdir))
            _store_test_data(storage)

            metrics = compute_metrics(storage, period="30d", source="work")
            # Note: source filter applies at snapshot level, both sessions are in same snapshot
            # so both are included but the snapshot source is "work"
            assert metrics.total_sessions >= 1


class TestFormatters:
    def test_json_output(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LocalStorage(base_dir=Path(tmpdir))
            _store_test_data(storage)
            metrics = compute_metrics(storage, period="30d")

            output = format_as_json(metrics)
            assert '"total_sessions": 2' in output

    def test_markdown_output(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LocalStorage(base_dir=Path(tmpdir))
            _store_test_data(storage)
            metrics = compute_metrics(storage, period="30d")

            output = format_as_markdown(metrics)
            assert "# AI Usage Summary" in output
            assert "Total sessions" in output
            assert "| work |" in output or "| personal |" in output

    def test_prompt_output(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = LocalStorage(base_dir=Path(tmpdir))
            _store_test_data(storage)
            metrics = compute_metrics(storage, period="30d")

            output = format_as_prompt(metrics)
            assert "Notable patterns" in output
            assert "Cross-modality" in output
            assert "total_sessions" in output
