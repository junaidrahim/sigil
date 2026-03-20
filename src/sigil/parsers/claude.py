"""Parser for Claude Code session JSONL files.

Claude Code stores sessions at::

    ~/.claude/projects/<encoded-project-path>/<uuid>.jsonl
"""

from typing import Any, Dict, List, Optional, Set, Tuple

import orjson
import xxhash

from sigil.models import SessionRow
from sigil.parsers.base import SessionParser
from sigil.timestamps import parse_timestamp

# Keys we extract into named columns — everything else goes to extras
_KNOWN_KEYS: Set[str] = {
    "type",
    "message",
    "uuid",
    "parentUuid",
    "timestamp",
    "sessionId",
    "cwd",
    "version",
    "gitBranch",
    "requestId",
    "isSidechain",
    "userType",
    "todos",
    "permissionMode",
    "toolUseResult",
    "sourceToolAssistantUUID",
    "snapshot",
    "messageId",
    "isSnapshotUpdate",
    "_source_file",
    "_source_line",
}


class ClaudeParser(SessionParser):
    """Parses Claude Code JSONL entries into ``SessionRow`` instances.

    Stateless — each entry is parsed independently. Claude Code timestamps
    are in unix milliseconds or ISO 8601 strings.
    """

    def parse(self, d: Dict[str, Any]) -> Optional[SessionRow]:
        """Parse a single Claude Code JSONL entry.

        Args:
            d: Raw dict from a JSONL line with injected ``_source_file``
                and ``_source_line``.

        Returns:
            A ``SessionRow``, or ``None`` for internal bookkeeping entries
            (e.g. ``file-history-snapshot``).
        """
        entry_type = d.get("type", "unknown")

        if entry_type == "file-history-snapshot":
            return None

        source_file = d.get("_source_file", "")
        source_line = d.get("_source_line", 0)
        row_id = xxhash.xxh64(f"{source_file}:{source_line}".encode()).hexdigest()

        ts = parse_timestamp(d.get("timestamp"), unix_ms=True)
        session_id = d.get("sessionId", "")

        message = d.get("message", {})
        if not isinstance(message, dict):
            message = {}

        role = message.get("role")
        content = message.get("content")
        model = message.get("model")
        stop_reason = message.get("stop_reason")

        usage = message.get("usage", {}) or {}
        input_tokens = usage.get("input_tokens")
        output_tokens = usage.get("output_tokens")
        cache_creation = usage.get("cache_creation_input_tokens")
        cache_read = usage.get("cache_read_input_tokens")

        msg_text, content_type, tool_name, tool_input = self._extract_content(content)

        tool_result_text: Optional[str] = None
        if d.get("toolUseResult"):
            parts: List[str] = []
            for block in d["toolUseResult"]:
                if isinstance(block, dict) and block.get("text"):
                    parts.append(block["text"])
            if parts:
                tool_result_text = "\n".join(parts)

        extras: Dict[str, Any] = {k: v for k, v in d.items() if k not in _KNOWN_KEYS}

        return SessionRow(
            row_id=row_id,
            session_id=session_id,
            session_system="claude_code",
            device=self.device,
            pushed_at=self.pushed_at,
            timestamp=ts,
            entry_type=entry_type,
            message_role=role,
            message_text=msg_text,
            content_type=content_type,
            tool_name=tool_name,
            tool_input=tool_input,
            tool_result_text=tool_result_text,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_creation_tokens=cache_creation,
            cache_read_tokens=cache_read,
            model=model,
            model_provider="anthropic" if model else None,
            cwd=d.get("cwd"),
            git_branch=d.get("gitBranch"),
            cli_version=d.get("version"),
            parent_uuid=d.get("parentUuid"),
            request_id=d.get("requestId"),
            stop_reason=stop_reason,
            source_file=source_file,
            source_line=source_line,
            extras=extras,
        )

    def _extract_content(
        self,
        content: Any,
    ) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        """Extract structured fields from a Claude message content block.

        Handles string content, and lists of typed blocks (``text``,
        ``tool_use``, ``thinking``, ``tool_result``).

        Args:
            content: The ``message.content`` value — a string, list of
                block dicts, or ``None``.

        Returns:
            A 4-tuple of ``(message_text, content_type, tool_name, tool_input)``.
            Any element may be ``None``.
        """
        if content is None:
            return None, None, None, None

        if isinstance(content, str):
            return content, "text", None, None

        if isinstance(content, list):
            texts: List[str] = []
            content_type: Optional[str] = None
            tool_name: Optional[str] = None
            tool_input: Optional[str] = None

            for block in content:
                if not isinstance(block, dict):
                    continue
                block_type = block.get("type", "")

                if block_type == "text":
                    text = block.get("text", "")
                    if text:
                        texts.append(text)
                    if not content_type:
                        content_type = "text"

                elif block_type == "tool_use":
                    tool_name = block.get("name")
                    raw_input = block.get("input")
                    if raw_input:
                        tool_input = orjson.dumps(raw_input).decode()
                    content_type = "tool_use"

                elif block_type == "thinking":
                    content_type = "thinking"

                elif block_type == "tool_result":
                    result_content = block.get("content", [])
                    if isinstance(result_content, list):
                        for rc in result_content:
                            if isinstance(rc, dict) and rc.get("type") == "text":
                                texts.append(rc.get("text", ""))
                    content_type = "tool_result"

                else:
                    if not content_type:
                        content_type = block_type

            msg_text = "\n".join(texts) if texts else None
            return msg_text, content_type, tool_name, tool_input

        return None, None, None, None
