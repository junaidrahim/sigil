"""Local parquet file storage backend."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import daft

from sigil.constants import DEFAULT_CHUNK_SIZE, ROWS_DIR
from sigil.models import SessionRow
from sigil.storage.base import StorageBackend
from sigil.timestamps import parse_timestamp

logger = logging.getLogger(__name__)


class LocalStorage(StorageBackend):
    """Stores rows as parquet files in ``~/.sigil/rows/``.

    Each chunk is flushed as a separate parquet file via daft. Intended
    for local development and testing.

    Attributes:
        base_dir: Root directory where parquet files are written.
    """

    def __init__(self, base_dir: Optional[Path] = None) -> None:
        """Initialise the local storage backend.

        Args:
            base_dir: Directory for parquet output. Defaults to
                ``~/.sigil/rows/``.
        """
        self.base_dir = base_dir or ROWS_DIR
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def append(self, rows: Iterable[SessionRow], chunk_size: int = DEFAULT_CHUNK_SIZE) -> int:
        """Append rows to local parquet storage in chunks.

        Args:
            rows: An iterable (or generator) of ``SessionRow`` instances.
            chunk_size: Number of rows to accumulate before writing a
                parquet file.

        Returns:
            Total number of rows written across all chunks.
        """
        total = 0
        chunk: List[Dict] = []

        for row in rows:
            chunk.append(row.model_dump(mode="json"))
            if len(chunk) >= chunk_size:
                total += self._flush(chunk)
                chunk = []

        if chunk:
            total += self._flush(chunk)

        return total

    def max_timestamp(self) -> Optional[datetime]:
        """Return the maximum ``timestamp`` from stored parquet files.

        Returns:
            The latest ``datetime`` found, or ``None`` if no data exists
            or all timestamps are null.
        """
        parquet_files = list(self.base_dir.rglob("*.parquet"))
        if not parquet_files:
            return None

        df = daft.read_parquet(str(self.base_dir))
        result = (
            df.where(daft.col("timestamp").not_null())
            .agg(daft.col("timestamp").max().alias("max_ts"))
            .collect()
        )

        raw = result.to_pydict()["max_ts"][0]
        if raw is None:
            return None

        return parse_timestamp(raw)

    def _flush(self, records: List[Dict]) -> int:
        """Write a batch of records to parquet.

        Args:
            records: List of dicts (JSON-serializable row data).

        Returns:
            Number of records written.
        """
        df = daft.from_pylist(records)
        df.write_parquet(root_dir=str(self.base_dir), write_mode="append")
        return len(records)
