"""Local parquet file storage backend."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import daft

from sigil.constants import ROWS_DIR
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
        self.base_dir = base_dir or ROWS_DIR
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _convert_row(self, row: SessionRow) -> Dict:
        return row.model_dump(mode="json")

    def _flush_chunk(self, chunk: List[Any]) -> int:
        df = daft.from_pylist(chunk)
        df.write_parquet(root_dir=str(self.base_dir), write_mode="append")
        return len(chunk)

    def max_timestamp(self, device: Optional[str] = None) -> Optional[datetime]:
        parquet_files = list(self.base_dir.rglob("*.parquet"))
        if not parquet_files:
            return None

        df = daft.read_parquet(str(self.base_dir))
        if device:
            df = df.where(daft.col("device") == device)
        result = df.agg(daft.col("timestamp").max().alias("max_ts")).collect()

        raw = result.to_pydict()["max_ts"][0]
        if raw is None:
            return None

        return parse_timestamp(raw)
