"""Push command: read Claude Code sessions, sanitize, and store."""

from __future__ import annotations

import hashlib
import json
import logging
from collections import Counter
from pathlib import Path

from sigil.config import SanitizeConfig
from sigil.models import Message, Session, Snapshot, ToolUse
from sigil.sanitize import sanitize_project_path, sanitize_text

logger = logging.getLogger(__name__)

DEFAULT_SESSIONS_PATH = Path.home() / ".claude" / "projects"


def discover_session_files(base_path: Path) -> list[Path]:
    """Find all JSONL session files under the given path."""
    files = list(base_path.rglob("*.jsonl"))
    logger.info("Discovered %d session files under %s", len(files), base_path)
    return sorted(files)


def parse_session_file(
    path: Path,
    source: str,
    device: str,
    sanitize_config: SanitizeConfig,
) -> Session | None:
    """Parse a single JSONL session file into a Session, applying sanitization."""
    messages: list[Message] = []
    tool_counter: Counter[str] = Counter()
    input_tokens = 0
    output_tokens = 0
    started_at: str | None = None
    ended_at: str | None = None
    model: str | None = None
    project_path: str | None = None

    try:
        lines = path.read_text(errors="replace").strip().splitlines()
    except OSError as e:
        logger.warning("Could not read %s: %s", path, e)
        return None

    if not lines:
        return None

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue

        # Extract timestamp
        ts = entry.get("timestamp") or entry.get("ts")

        if started_at is None and ts:
            started_at = ts
        if ts:
            ended_at = ts

        # Extract model
        if not model and entry.get("model"):
            model = entry["model"]

        # Extract project path from session metadata
        if not project_path:
            project_path = entry.get("cwd") or entry.get("project_path")

        # Extract token usage
        usage = entry.get("usage", {})
        input_tokens += usage.get("input_tokens", 0) or usage.get("prompt_tokens", 0)
        output_tokens += usage.get("output_tokens", 0) or usage.get("completion_tokens", 0)

        # Extract message content
        role = entry.get("role", "")
        content = _extract_content(entry)
        if content:
            sanitized = sanitize_text(content, sanitize_config)
            messages.append(Message(
                role=role,
                text=sanitized,
                timestamp=ts,
                token_count=usage.get("input_tokens") or usage.get("output_tokens"),
            ))

        # Extract tool usage from top-level fields
        tool_name = entry.get("tool_name")
        if not tool_name and isinstance(entry.get("tool"), dict):
            tool_name = entry["tool"].get("name")
        if tool_name:
            tool_counter[tool_name] += 1

        # Extract tool usage from content blocks (only if not already counted above)
        if not tool_name and isinstance(entry.get("content"), list):
            for block in entry["content"]:
                if isinstance(block, dict) and block.get("type") == "tool_use":
                    tool_counter[block.get("name", "unknown")] += 1

    if not messages and not tool_counter:
        return None

    # Generate a stable session ID from the file path
    session_id = hashlib.sha256(str(path).encode()).hexdigest()[:16]

    sanitized_project = sanitize_project_path(
        project_path, sanitize_config.strip_paths
    )

    return Session(
        session_id=session_id,
        source=source,
        device=device,
        started_at=started_at,
        ended_at=ended_at,
        messages=messages,
        tool_uses=[ToolUse(name, count) for name, count in tool_counter.items()],
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=input_tokens + output_tokens,
        project_path=sanitized_project,
        model=model,
    )


def build_snapshot(
    sessions_path: Path,
    source: str,
    device: str,
    sanitize_config: SanitizeConfig,
) -> Snapshot:
    """Build a complete snapshot from session files."""
    files = discover_session_files(sessions_path)
    sessions: list[Session] = []

    for f in files:
        session = parse_session_file(f, source, device, sanitize_config)
        if session:
            sessions.append(session)

    logger.info("Parsed %d sessions from %d files", len(sessions), len(files))

    return Snapshot(
        source=source,
        device=device,
        sessions=sessions,
        session_count=len(sessions),
    )


def _extract_content(entry: dict) -> str:
    """Extract text content from a JSONL entry, handling various formats."""
    content = entry.get("content", "")

    if isinstance(content, str):
        return content

    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, dict):
                if block.get("type") == "text":
                    parts.append(block.get("text", ""))
            elif isinstance(block, str):
                parts.append(block)
        return "\n".join(parts)

    return ""
