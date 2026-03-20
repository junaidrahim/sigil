"""Apache Iceberg storage backend using pyiceberg and daft."""

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import daft
import pyarrow as pa
from pyiceberg.catalog import load_catalog

from sigil.config import IcebergConfig
from sigil.models import SessionRow
from sigil.storage.base import StorageBackend
from sigil.timestamps import parse_timestamp

logger = logging.getLogger(__name__)


class IcebergStorage(StorageBackend):
    """Stores rows in an Apache Iceberg table via pyiceberg + daft.

    Connects to a REST catalog. The table is created automatically on
    first use with the schema and partition spec derived from
    ``SessionRow``.

    Attributes:
        config: The Iceberg catalog connection settings.
        table_identifier: Fully qualified table name (``namespace.table``).
        catalog: The pyiceberg catalog instance.
        table: The pyiceberg table handle.
    """

    def __init__(self, config: IcebergConfig) -> None:
        self.config = config
        namespace, table_name = SessionRow.iceberg_table_id()
        self.table_identifier = f"{namespace}.{table_name}"
        catalog_props: Dict[str, str] = {
            "uri": config.catalog_uri,
            "s3.connect-timeout": "30",
            "s3.request-timeout": "60",
        }
        if config.catalog_token:
            catalog_props["token"] = config.catalog_token
        if config.warehouse:
            catalog_props["warehouse"] = config.warehouse
        self.catalog = load_catalog(config.catalog_name, **catalog_props)
        self._ensure_table(namespace)
        self._arrow_schema_cache: Optional[pa.Schema] = None

    def _ensure_table(self, namespace: str) -> None:
        self.catalog.create_namespace_if_not_exists(namespace)
        self.table = self.catalog.create_table_if_not_exists(
            identifier=self.table_identifier,
            schema=SessionRow.iceberg_schema(),
            partition_spec=SessionRow.partition_spec(),
            properties={
                "write.delete.mode": "merge-on-read",
                "write.update.mode": "merge-on-read",
                "write.merge.mode": "merge-on-read",
            },
        )

    def _pre_append(self) -> None:
        self._arrow_schema_cache = self.table.scan().to_arrow_batch_reader().schema

    def _convert_row(self, row: SessionRow) -> Dict:
        return row.to_storage_dict()

    def _flush_chunk(self, chunk: List[Any], max_retries: int = 3) -> int:
        arrow_table = pa.Table.from_pylist(chunk, schema=self._arrow_schema_cache)
        for attempt in range(1, max_retries + 1):
            try:
                df = daft.from_arrow(arrow_table)
                df.write_iceberg(self.table, mode="append")
                logger.info("Appended %d rows to %s", len(chunk), self.table_identifier)
                return len(chunk)
            except OSError:
                if attempt == max_retries:
                    raise
                wait = 2**attempt
                logger.warning(
                    "Flush attempt %d/%d failed, retrying in %ds...",
                    attempt,
                    max_retries,
                    wait,
                )
                time.sleep(wait)
        return 0  # unreachable, satisfies type checker

    def max_timestamp(self) -> Optional[datetime]:
        df = daft.read_iceberg(self.table)
        result = df.agg(daft.col("timestamp").max().alias("max_ts")).collect()

        raw = result.to_pydict()["max_ts"][0]
        if raw is None:
            return None

        return parse_timestamp(raw)
