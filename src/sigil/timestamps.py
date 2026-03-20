"""Timestamp parsing utilities using python-dateutil.

All timestamps are normalized to UTC ``datetime`` objects with
``tzinfo=datetime.timezone.utc``.
"""

from datetime import UTC, datetime
from typing import Any, Optional

from dateutil.parser import ParserError
from dateutil.parser import parse as dateutil_parse


def parse_timestamp(raw: Any, unix_ms: bool = False) -> Optional[datetime]:
    """Parse a timestamp from various formats into a UTC datetime.

    All outputs are timezone-aware with ``tzinfo=UTC``. Naive datetimes
    from ``dateutil`` are assumed to be UTC. Timezone-aware datetimes are
    converted to UTC.

    Handles:
        - ``None`` returns ``None``.
        - ``int`` / ``float`` are treated as unix epoch seconds (or
          milliseconds when *unix_ms* is ``True``).
        - ``str`` is parsed via ``dateutil.parser.parse``.

    Args:
        raw: The raw timestamp value to parse.
        unix_ms: If ``True``, treat numeric values as milliseconds since
            the unix epoch instead of seconds.

    Returns:
        A timezone-aware ``datetime`` in UTC, or ``None`` if *raw* is
        ``None`` or cannot be parsed.
    """
    if raw is None:
        return None

    if isinstance(raw, int | float):
        try:
            return datetime.fromtimestamp(raw / 1000 if unix_ms else raw, tz=UTC)
        except (ValueError, OSError, OverflowError):
            return None

    if isinstance(raw, str):
        try:
            dt = dateutil_parse(raw)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)
        except (ParserError, OverflowError):
            return None

    return None
