"""Abstract storage backend interface."""

import abc
from datetime import datetime
from typing import Iterable, Optional

from sigil.constants import DEFAULT_CHUNK_SIZE
from sigil.models import SessionRow


class StorageBackend(abc.ABC):
    """Abstract base class for session row storage backends.

    Implementations must handle chunked iteration internally — callers
    pass an iterable (including generators) and the backend accumulates
    and flushes in chunks.
    """

    @abc.abstractmethod
    def append(self, rows: Iterable[SessionRow], chunk_size: int = DEFAULT_CHUNK_SIZE) -> int:
        """Append rows to storage in chunks.

        Args:
            rows: An iterable (or generator) of ``SessionRow`` instances.
            chunk_size: Number of rows to accumulate before flushing a
                batch to the underlying storage.

        Returns:
            Total number of rows written across all chunks.
        """
        ...

    @abc.abstractmethod
    def max_timestamp(self) -> Optional[datetime]:
        """Return the maximum ``timestamp`` value across all stored rows.

        Used as a high-water mark for incremental pushes — only rows
        with a timestamp strictly greater than this value will be
        processed on the next push.

        Returns:
            The latest ``datetime`` found in storage, or ``None`` if
            the store is empty.
        """
        ...
