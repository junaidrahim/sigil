"""Base parser interface and shared file-reading logic."""

import abc
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

import orjson
import xxhash

from sigil.models import SessionRow

logger = logging.getLogger(__name__)


class SessionParser(abc.ABC):
    """Abstract base class for session log parsers.

    Subclasses implement ``parse()`` to convert a single raw JSONL dict
    into a ``SessionRow``. The base class provides ``parse_file()`` which
    handles file I/O, line-by-line streaming, and watermark filtering.

    Attributes:
        device: Hostname of the machine being parsed.
        pushed_at: Timestamp marking when this push was initiated.
        watermark: High-water mark timestamp. Rows with a timestamp at
            or before this value are skipped. ``None`` means no filtering.
    """

    def __init__(
        self,
        device: str,
        pushed_at: datetime,
        watermark: Optional[datetime] = None,
    ) -> None:
        """Initialise the parser with device, push metadata, and watermark.

        Args:
            device: Machine hostname to tag each row with.
            pushed_at: Timestamp for the current push operation.
            watermark: If set, rows with ``timestamp <= watermark`` are
                skipped. Rows with ``timestamp=None`` always pass through.
        """
        self.device = device
        self.pushed_at = pushed_at
        self.watermark = watermark

    @staticmethod
    def make_row_id(source_file: str, source_line: int) -> str:
        """Generate a deterministic row ID from source file and line number."""
        return xxhash.xxh64(f"{source_file}:{source_line}".encode()).hexdigest()

    def _build_row(self, d: Dict[str, Any], **kwargs: Any) -> SessionRow:
        """Build a ``SessionRow`` with common fields filled from the entry dict.

        Handles ``row_id``, ``device``, ``pushed_at``, ``source_file``,
        ``source_line``, and ``extras``. Callers pass remaining fields
        via ``**kwargs``.
        """
        source_file = d.get("_source_file", "")
        source_line = d.get("_source_line", 0)
        return SessionRow(
            row_id=self.make_row_id(source_file, source_line),
            device=self.device,
            pushed_at=self.pushed_at,
            source_file=source_file,
            source_line=source_line,
            **kwargs,
        )

    @abc.abstractmethod
    def parse(self, d: Dict[str, Any]) -> Optional[SessionRow]:
        """Parse a single JSONL entry dict into a SessionRow.

        Args:
            d: A raw dict from a single JSONL line, with ``_source_file``
                and ``_source_line`` injected by ``parse_file()``.

        Returns:
            A ``SessionRow`` instance, or ``None`` to skip this entry.
        """
        ...

    def parse_file(self, path: Path) -> Iterator[SessionRow]:
        """Stream a JSONL file line-by-line, yielding ``SessionRow`` instances.

        Each line is read in binary mode and parsed with ``orjson``. Malformed
        lines are logged and skipped. The ``_source_file`` and ``_source_line``
        keys are injected into each dict before calling ``parse()``.

        Rows with a ``timestamp`` at or before ``self.watermark`` are filtered
        out. Rows with ``timestamp=None`` are always yielded.

        Args:
            path: Path to the JSONL session file.

        Yields:
            ``SessionRow`` instances for each successfully parsed entry
            that passes the watermark filter.
        """
        try:
            with open(path, "rb") as f:
                for line_num, raw_line in enumerate(f, start=1):
                    raw_line = raw_line.strip()
                    if not raw_line:
                        continue
                    try:
                        entry = orjson.loads(raw_line)
                    except orjson.JSONDecodeError:
                        logger.debug("Skipping malformed JSON at %s:%d", path, line_num)
                        continue

                    entry["_source_file"] = str(path)
                    entry["_source_line"] = line_num
                    row = self.parse(entry)
                    if row is None:
                        continue

                    # Apply watermark filter
                    if self.watermark and row.timestamp <= self.watermark:
                        continue

                    yield row
        except OSError as e:
            logger.warning("Could not read %s: %s", path, e)
