"""Parser for OpenAI Codex CLI session JSONL files.

Codex stores sessions at::

    ~/.codex/sessions/YYYY/MM/DD/rollout-<timestamp>-<ulid>.jsonl
"""

from typing import Any, Dict, List, Optional, Set, Tuple

import xxhash

from sigil.models import SessionRow
from sigil.parsers.base import SessionParser
from sigil.timestamps import parse_timestamp

# Top-level keys we handle explicitly
_KNOWN_TOP_KEYS: Set[str] = {"timestamp", "type", "payload", "_source_file", "_source_line"}

# Payload keys handled per entry type (everything else goes to extras)
_HANDLED_PAYLOAD_KEYS: Dict[str, Set[str]] = {
    "session_meta": {
        "id",
        "timestamp",
        "cwd",
        "cli_version",
        "model_provider",
        "originator",
        "source",
    },
    "turn_context": {"cwd", "model"},
    "response_item": {"type", "role", "content", "name", "arguments", "summary"},
    "event_msg": {"type", "message", "images"},
}


class CodexParser(SessionParser):
    """Parses Codex JSONL entries into ``SessionRow`` instances.

    Stateful — maintains session-level context (``session_id``, ``model``,
    ``cwd``, etc.) that propagates across entries within a single file.
    A fresh instance should be created per file to avoid state leaking.

    Attributes:
        _session_id: Current session ID from the most recent ``session_meta``.
        _model: Current model from the most recent ``turn_context``.
        _model_provider: Provider string from ``session_meta``.
        _cli_version: CLI version from ``session_meta``.
        _cwd: Working directory from the most recent meta/context event.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialise parser with empty session-level state.

        Args:
            *args: Positional args forwarded to ``SessionParser.__init__``.
            **kwargs: Keyword args forwarded to ``SessionParser.__init__``.
        """
        super().__init__(*args, **kwargs)
        self._session_id = ""
        self._model: Optional[str] = None
        self._model_provider: Optional[str] = None
        self._cli_version: Optional[str] = None
        self._cwd: Optional[str] = None

    def parse(self, d: Dict[str, Any]) -> Optional[SessionRow]:
        """Parse a single Codex JSONL entry.

        Updates internal session-level state when ``session_meta`` or
        ``turn_context`` entries are encountered, then builds a row using
        the current state.

        Args:
            d: Raw dict from a JSONL line with injected ``_source_file``
                and ``_source_line``.

        Returns:
            A ``SessionRow`` instance. Unlike the Claude parser, Codex
            entries are never skipped.
        """
        source_file = d.get("_source_file", "")
        source_line = d.get("_source_line", 0)
        row_id = xxhash.xxh64(f"{source_file}:{source_line}".encode()).hexdigest()

        entry_type = d.get("type", "unknown")
        payload = d.get("payload", {}) or {}
        ts = parse_timestamp(d.get("timestamp"))

        # Update session-level state from meta/context events
        if entry_type == "session_meta":
            self._session_id = payload.get("id", "")
            self._cli_version = payload.get("cli_version")
            self._cwd = payload.get("cwd")
            self._model_provider = payload.get("model_provider")
        elif entry_type == "turn_context":
            self._model = payload.get("model")
            if payload.get("cwd"):
                self._cwd = payload["cwd"]

        msg_text, content_type, role, tool_name, tool_input = self._extract_from_payload(
            entry_type, payload
        )

        # Build extras
        extras: Dict[str, Any] = {k: v for k, v in d.items() if k not in _KNOWN_TOP_KEYS}
        payload_extras = self._payload_extras(entry_type, payload)
        if payload_extras:
            extras["payload"] = payload_extras

        return SessionRow(
            row_id=row_id,
            session_id=self._session_id,
            session_system="codex",
            device=self.device,
            pushed_at=self.pushed_at,
            timestamp=ts,
            entry_type=entry_type,
            message_role=role,
            message_text=msg_text,
            content_type=content_type,
            tool_name=tool_name,
            tool_input=tool_input,
            tool_result_text=None,
            input_tokens=None,
            output_tokens=None,
            cache_creation_tokens=None,
            cache_read_tokens=None,
            model=self._model,
            model_provider=self._model_provider,
            cwd=self._cwd,
            git_branch=None,
            cli_version=self._cli_version,
            parent_uuid=None,
            request_id=None,
            stop_reason=None,
            source_file=source_file,
            source_line=source_line,
            extras=extras,
        )

    def _extract_from_payload(
        self,
        entry_type: str,
        payload: Dict[str, Any],
    ) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
        """Extract message content from a Codex payload.

        Dispatches based on ``entry_type`` to handle ``response_item``,
        ``event_msg``, ``session_meta``, and ``turn_context`` events.

        Args:
            entry_type: The top-level ``type`` field of the JSONL entry.
            payload: The ``payload`` dict from the JSONL entry.

        Returns:
            A 5-tuple of ``(msg_text, content_type, role, tool_name, tool_input)``.
            Any element may be ``None``.
        """
        if entry_type == "response_item":
            item_type = payload.get("type", "")
            role = payload.get("role")

            if item_type == "message":
                content = payload.get("content", [])
                texts: List[str] = []
                for block in content if isinstance(content, list) else []:
                    if isinstance(block, dict):
                        text = block.get("text", "")
                        if text:
                            texts.append(text)
                block_type: Optional[str] = None
                if isinstance(content, list) and content:
                    first = content[0]
                    if isinstance(first, dict):
                        block_type = first.get("type")
                return (
                    "\n".join(texts) if texts else None,
                    block_type,
                    role,
                    None,
                    None,
                )

            elif item_type == "function_call":
                return (
                    None,
                    "function_call",
                    role,
                    payload.get("name"),
                    payload.get("arguments") or None,
                )

            elif item_type == "reasoning":
                return None, "reasoning", None, None, None

            return None, item_type or None, role, None, None

        elif entry_type == "event_msg":
            msg_type = payload.get("type", "")
            if msg_type == "user_message":
                text = payload.get("message", "")
                return (text if text else None, "text", "user", None, None)
            return None, msg_type or None, None, None, None

        elif entry_type == "session_meta":
            return None, "session_meta", None, None, None

        elif entry_type == "turn_context":
            return None, "turn_context", None, None, None

        return None, None, None, None, None

    def _payload_extras(self, entry_type: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract unhandled payload keys as extras.

        Args:
            entry_type: The top-level ``type`` field of the JSONL entry.
            payload: The ``payload`` dict from the JSONL entry.

        Returns:
            A dict of unhandled keys, or ``None`` if all keys were handled.
        """
        handled = _HANDLED_PAYLOAD_KEYS.get(entry_type, set())
        extras = {k: v for k, v in payload.items() if k not in handled}
        return extras if extras else None
