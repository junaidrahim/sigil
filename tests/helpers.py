"""Shared test helpers."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from sigil.models import SessionRow


def make_row(
    session_system: str = "claude_code",
    session_id: str = "sess-1",
    timestamp: Optional[datetime] = None,
    tool_name: Optional[str] = None,
    input_tokens: Optional[int] = 100,
    output_tokens: Optional[int] = 200,
    model: Optional[str] = "claude-opus-4-6",
    **kwargs: Any,
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


def write_jsonl(path: Path, entries: List[Dict]) -> None:
    """Write a list of dicts as a JSONL file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")
