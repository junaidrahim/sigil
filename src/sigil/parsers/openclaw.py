"""Parser for OpenClaw session JSONL files.

OpenClaw stores sessions at::

    ~/.openclaw/agents/main/sessions/<session-id>.jsonl

The format is similar to Claude Code but with differences in entry
structure, usage field naming, and tool result handling.
"""

from typing import Any, Dict, List, Optional, Set, Tuple

import orjson

from sigil.models import SessionRow
from sigil.parsers.base import SessionParser
from sigil.timestamps import parse_timestamp

# Keys we extract into named columns — everything else goes to extras
_KNOWN_KEYS: Set[str] = {
    "type",
    "customType",
    "data",
    "id",
    "parentId",
    "timestamp",
    "message",
    "version",
    "cwd",
    "provider",
    "modelId",
    "thinkingLevel",
    "_source_file",
    "_source_line",
}


class OpenClawParser(SessionParser):
    """Parses OpenClaw JSONL entries into ``SessionRow`` instances.

    Stateful — maintains session-level context (``session_id``, ``model``,
    ``cwd``, ``provider``) that propagates across entries within a single
    file. A fresh instance should be created per file to avoid state leaking.

    Attributes:
        _session_id: Current session ID from the ``session`` entry.
        _model: Current model from the most recent ``model_change``.
        _provider: Current provider from the most recent ``model_change``.
        _cwd: Working directory from the ``session`` entry.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._session_id = ""
        self._model: Optional[str] = None
        self._provider: Optional[str] = None
        self._cwd: Optional[str] = None

    def parse(self, d: Dict[str, Any]) -> Optional[SessionRow]:
        """Parse a single OpenClaw JSONL entry.

        Updates internal session-level state when ``session`` or
        ``model_change`` entries are encountered, then builds a row
        using the current state.

        Args:
            d: Raw dict from a JSONL line with injected ``_source_file``
                and ``_source_line``.

        Returns:
            A ``SessionRow``, or ``None`` if a timestamp cannot be
            determined.
        """
        entry_type = d.get("type", "unknown")
        ts = parse_timestamp(d.get("timestamp"))

        if ts is None:
            return None

        # Update session-level state
        if entry_type == "session":
            self._session_id = d.get("id", "")
            self._cwd = d.get("cwd")
        elif entry_type == "model_change":
            self._model = d.get("modelId")
            self._provider = d.get("provider")
        elif entry_type == "custom":
            custom_type = d.get("customType", "")
            data = d.get("data", {})
            if custom_type == "model-snapshot" and isinstance(data, dict):
                self._model = data.get("modelId") or self._model
                self._provider = data.get("provider") or self._provider

        message = d.get("message", {})
        if not isinstance(message, dict):
            message = {}

        role = message.get("role")
        content = message.get("content")
        model = message.get("model") or self._model
        stop_reason = message.get("stopReason")

        # Extract usage — OpenClaw uses different field names
        usage = message.get("usage", {}) or {}
        input_tokens = usage.get("input")
        output_tokens = usage.get("output")
        cache_read = usage.get("cacheRead")
        cache_creation = usage.get("cacheWrite")

        # Determine provider
        provider = message.get("provider") or self._provider

        # Handle tool results (role=toolResult)
        tool_result_text: Optional[str] = None
        tool_name_from_result: Optional[str] = None
        if role == "toolResult":
            tool_name_from_result = message.get("toolName")
            result_content = message.get("content", [])
            if isinstance(result_content, list):
                parts: List[str] = []
                for block in result_content:
                    if isinstance(block, dict) and block.get("text"):
                        parts.append(block["text"])
                if parts:
                    tool_result_text = "\n".join(parts)

        # Extract content fields for non-toolResult messages
        msg_text: Optional[str] = None
        content_type: Optional[str] = None
        tool_name: Optional[str] = None
        tool_input: Optional[str] = None

        if role == "toolResult":
            content_type = "tool_result"
            msg_text = tool_result_text
            tool_name = tool_name_from_result
        else:
            msg_text, content_type, tool_name, tool_input = self._extract_content(content)

        # Map entry_type for custom entries
        effective_entry_type = entry_type
        if entry_type == "custom":
            custom_type = d.get("customType", "")
            if custom_type:
                effective_entry_type = f"custom:{custom_type}"

        # Build extras from unknown keys
        extras: Dict[str, Any] = {k: v for k, v in d.items() if k not in _KNOWN_KEYS}

        return self._build_row(
            d,
            session_id=self._session_id,
            session_system="openclaw",
            timestamp=ts,
            entry_type=effective_entry_type,
            message_role=role if role != "toolResult" else "tool",
            message_text=msg_text,
            content_type=content_type,
            tool_name=tool_name,
            tool_input=tool_input,
            tool_result_text=tool_result_text if role == "toolResult" else None,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_creation_tokens=cache_creation,
            cache_read_tokens=cache_read,
            model=model,
            model_provider=provider,
            cwd=self._cwd,
            parent_uuid=d.get("parentId"),
            stop_reason=stop_reason,
            extras=extras,
        )

    def _extract_content(
        self,
        content: Any,
    ) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        """Extract structured fields from a message content block.

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
