"""Push command: auto-detect session logs, parse, and store."""

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

from sigil.constants import CLAUDE_SESSIONS_DIR, CODEX_SESSIONS_DIR, OPENCLAW_SESSIONS_DIR
from sigil.models import SessionRow
from sigil.parsers.base import SessionParser
from sigil.parsers.claude import ClaudeParser
from sigil.parsers.codex import CodexParser
from sigil.parsers.openclaw import OpenClawParser

logger = logging.getLogger(__name__)

# System name -> parser class
_PARSERS = {
    "claude_code": ClaudeParser,
    "codex": CodexParser,
    "openclaw": OpenClawParser,
}


def auto_detect_sources() -> List[Tuple[str, Path]]:
    """Detect which session log directories exist on this machine.

    Checks for the presence of known session log directories for each
    supported AI system.

    Returns:
        A list of ``(system_name, directory_path)`` tuples for each
        detected system.
    """
    sources: List[Tuple[str, Path]] = []
    if CLAUDE_SESSIONS_DIR.is_dir():
        sources.append(("claude_code", CLAUDE_SESSIONS_DIR))
    if CODEX_SESSIONS_DIR.is_dir():
        sources.append(("codex", CODEX_SESSIONS_DIR))
    if OPENCLAW_SESSIONS_DIR.is_dir():
        sources.append(("openclaw", OPENCLAW_SESSIONS_DIR))
    return sources


def discover_session_files(base_path: Path) -> List[Path]:
    """Find all JSONL session files under the given path.

    Args:
        base_path: Root directory to search recursively.

    Returns:
        A sorted list of paths to ``.jsonl`` files.
    """
    files = list(base_path.rglob("*.jsonl"))
    logger.info("Discovered %d session files under %s", len(files), base_path)
    return sorted(files)


def push_all(
    device: str,
    sources: Optional[List[Tuple[str, Path]]] = None,
    watermark: Optional[datetime] = None,
    watermarks: Optional[Dict[str, datetime]] = None,
) -> Iterator[SessionRow]:
    """Parse all detected session logs, yielding rows as they're parsed.

    Creates a fresh parser instance per file to avoid state leaking
    between files (relevant for the stateful Codex parser).

    Args:
        device: Machine hostname to tag each row with.
        sources: Explicit list of ``(system_name, path)`` pairs. If
            ``None``, auto-detects from known locations.
        watermark: Legacy single watermark applied to all parsers.
            Ignored when ``watermarks`` is provided.
        watermarks: Per-parser watermarks keyed by system name (e.g.
            ``{"claude_code": <datetime>, "openclaw": <datetime>}``).
            Takes precedence over ``watermark``.

    Yields:
        ``SessionRow`` instances streamed from all discovered session files.
    """
    if sources is None:
        sources = auto_detect_sources()

    pushed_at = datetime.now(UTC)

    for system, base_path in sources:
        parser_cls = _PARSERS.get(system)
        if parser_cls is None:
            logger.warning("Unknown system %s, skipping %s", system, base_path)
            continue

        # Per-parser watermark takes precedence over the legacy single value
        effective_watermark = (
            watermarks.get(system) if watermarks is not None else watermark
        )

        files = discover_session_files(base_path)
        logger.info("Processing %d files from %s (%s)", len(files), system, base_path)

        for path in files:
            parser: SessionParser = parser_cls(
                device=device, pushed_at=pushed_at, watermark=effective_watermark
            )
            yield from parser.parse_file(path)
