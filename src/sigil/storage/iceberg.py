"""Apache Iceberg storage backend using pyiceberg and daft."""

import logging
from datetime import datetime
from typing import Dict, Iterable, List, Optional

import daft
import pyarrow as pa
from pyiceberg.catalog import load_catalog

from sigil.config import IcebergConfig
from sigil.constants import DEFAULT_CHUNK_SIZE
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
        """Initialise the Iceberg storage backend.

        Connects to the REST catalog and ensures the target table exists.

        Args:
            config: Iceberg catalog connection settings.
        """
        self.config = config
        namespace, table_name = SessionRow.iceberg_table_id()
        self.table_identifier = f"{namespace}.{table_name}"
        catalog_props: Dict[str, str] = {"uri": config.catalog_uri}
        if config.catalog_token:
            catalog_props["token"] = config.catalog_token
        if config.warehouse:
            catalog_props["warehouse"] = config.warehouse
        self.catalog = load_catalog(config.catalog_name, **catalog_props)
        self._ensure_table(namespace)

    def _ensure_table(self, namespace: str) -> None:
        """Create the namespace and table if they don't already exist.

        Args:
            namespace: Iceberg namespace to create.
        """
        self.catalog.create_namespace_if_not_exists(namespace)
        self.table = self.catalog.create_table_if_not_exists(
            identifier=self.table_identifier,
            schema=SessionRow.iceberg_schema(),
            partition_spec=SessionRow.partition_spec(),
        )

    def append(self, rows: Iterable[SessionRow], chunk_size: int = DEFAULT_CHUNK_SIZE) -> int:
        """Append rows to the Iceberg table in chunks via daft.

        Args:
            rows: An iterable (or generator) of ``SessionRow`` instances.
            chunk_size: Number of rows to accumulate before flushing a
                batch to Iceberg.

        Returns:
            Total number of rows written across all chunks.
        """
        total = 0
        chunk: List[Dict] = []
        arrow_schema = self._arrow_schema()

        for row in rows:
            chunk.append(row.to_iceberg_dict())
            if len(chunk) >= chunk_size:
                total += self._flush(chunk, arrow_schema)
                chunk = []

        if chunk:
            total += self._flush(chunk, arrow_schema)

        return total

    def max_timestamp(self) -> Optional[datetime]:
        """Return the maximum ``timestamp`` from the Iceberg table.

        Returns:
            The latest ``datetime`` found, or ``None`` if the table is
            empty or all timestamps are null.
        """
        df = daft.read_iceberg(self.table)
        result = (
            df.where(daft.col("timestamp").not_null())
            .agg(daft.col("timestamp").max().alias("max_ts"))
            .collect()
        )

        raw = result.to_pydict()["max_ts"][0]
        if raw is None:
            return None

        return parse_timestamp(raw)

    def _flush(self, records: List[Dict], arrow_schema: pa.Schema) -> int:
        """Write a batch of records to the Iceberg table.

        Args:
            records: List of dicts with ``extras`` already serialized
                as JSON strings.
            arrow_schema: Arrow schema matching the Iceberg table.

        Returns:
            Number of records written.
        """
        arrow_table = pa.Table.from_pylist(records, schema=arrow_schema)
        df = daft.from_arrow(arrow_table)
        df.write_iceberg(self.table, mode="append")
        logger.info("Appended %d rows to %s", len(records), self.table_identifier)
        return len(records)

    def _arrow_schema(self) -> pa.Schema:
        """Derive an Arrow schema from the Iceberg table's current schema.

        Returns:
            A ``pyarrow.Schema`` matching the table's column types.
        """
        return self.table.scan().to_arrow_batch_reader().schema
