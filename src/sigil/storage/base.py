"""Abstract storage backend interface."""

from __future__ import annotations

import abc
from datetime import datetime

from sigil.models import Snapshot


class StorageBackend(abc.ABC):
    """Interface for snapshot storage backends."""

    @abc.abstractmethod
    def save_snapshot(self, snapshot: Snapshot) -> str:
        """Save a snapshot. Returns the snapshot ID."""
        ...

    @abc.abstractmethod
    def list_snapshots(
        self,
        source: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> list[Snapshot]:
        """List snapshots, optionally filtered by source and time range."""
        ...

    @abc.abstractmethod
    def get_snapshot(self, snapshot_id: str) -> Snapshot | None:
        """Retrieve a single snapshot by ID."""
        ...
