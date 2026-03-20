"""Abstract storage backend interface."""

import abc
from datetime import datetime
from typing import Any, Iterable, List, Optional

from sigil.constants import DEFAULT_CHUNK_SIZE
from sigil.models import SessionRow


class StorageBackend(abc.ABC):
    """Abstract base class for session row storage backends.

    Provides a concrete ``append()`` that handles chunked iteration.
    Subclasses implement ``_convert_row()``, ``_flush_chunk()``, and
    optionally ``_pre_append()`` for per-append setup.
    """

    def append(self, rows: Iterable[SessionRow], chunk_size: int = DEFAULT_CHUNK_SIZE) -> int:
        """Append rows to storage in chunks.

        Args:
            rows: An iterable (or generator) of ``SessionRow`` instances.
            chunk_size: Number of rows to accumulate before flushing a
                batch to the underlying storage.

        Returns:
            Total number of rows written across all chunks.
        """
        self._pre_append()
        total = 0
        chunk: List[Any] = []

        for row in rows:
            chunk.append(self._convert_row(row))
            if len(chunk) >= chunk_size:
                total += self._flush_chunk(chunk)
                chunk = []

        if chunk:
            total += self._flush_chunk(chunk)

        return total

    def _pre_append(self) -> None:  # noqa: B027
        """Optional setup hook called once at the start of ``append()``."""

    @abc.abstractmethod
    def _convert_row(self, row: SessionRow) -> Any:
        """Convert a ``SessionRow`` to the backend's internal format."""
        ...

    @abc.abstractmethod
    def _flush_chunk(self, chunk: List[Any]) -> int:
        """Write a batch of converted rows, return count written."""
        ...

    @abc.abstractmethod
    def max_timestamp(self, device: Optional[str] = None) -> Optional[datetime]:
        """Return the maximum ``timestamp`` value across stored rows.

        Args:
            device: If provided, only consider rows from this device.

        Returns:
            The latest ``datetime`` found in storage, or ``None`` if
            the store is empty (or has no rows for the given device).
        """
        ...
