"""Data models for sigil."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class Message:
    """A single message in a conversation."""

    role: str
    text: str
    timestamp: str | None = None
    token_count: int | None = None


@dataclass
class ToolUse:
    """A record of a tool being used in a session."""

    tool_name: str
    count: int = 1


@dataclass
class Session:
    """A parsed and sanitized Claude Code session."""

    session_id: str
    source: str  # work, personal, openclaw
    device: str
    started_at: str | None = None
    ended_at: str | None = None
    messages: list[Message] = field(default_factory=list)
    tool_uses: list[ToolUse] = field(default_factory=list)
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    project_path: str | None = None  # sanitized
    model: str | None = None
    raw_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Snapshot:
    """A push snapshot containing sessions and metadata."""

    snapshot_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    pushed_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    source: str = ""
    device: str = ""
    session_count: int = 0
    sessions: list[Session] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.session_count = len(self.sessions)
