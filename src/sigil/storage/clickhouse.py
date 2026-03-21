"""ClickHouse storage backend."""

import logging
from datetime import UTC, datetime
from typing import Any, List, Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from sigil.config import ClickHouseConfig
from sigil.models import SessionRow
from sigil.storage.base import StorageBackend

logger = logging.getLogger(__name__)

_COLUMN_NAMES = [name for name, _ in SessionRow.clickhouse_columns()]


class ClickHouseStorage(StorageBackend):
    """Stores rows in a ClickHouse table.

    Connects via clickhouse-connect with TLS enabled.
    The database and table are created automatically on first use.

    Attributes:
        config: ClickHouse connection settings.
        table: Fully qualified table name.
        client: The clickhouse-connect client instance.
    """

    def __init__(self, config: ClickHouseConfig) -> None:
        self.config = config
        self.table = f"{config.database}.session_logs"
        self.client: Client = clickhouse_connect.get_client(
            host=config.host,
            username=config.user,
            password=config.password,
            secure=True,
        )
        self._ensure_table()

    def _ensure_table(self) -> None:
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.config.database}")
        self.client.command(SessionRow.clickhouse_ddl(self.table))
        logger.info("Ensured table %s exists", self.table)

    def _convert_row(self, row: SessionRow) -> List:
        d = row.to_storage_dict()
        return [d[col] for col in _COLUMN_NAMES]

    def _flush_chunk(self, chunk: List[Any]) -> int:
        self.client.insert(
            self.table,
            data=chunk,
            column_names=_COLUMN_NAMES,
        )
        logger.info("Appended %d rows to %s", len(chunk), self.table)
        return len(chunk)

    def max_timestamp(
        self,
        device: Optional[str] = None,
        session_system: Optional[str] = None,
    ) -> Optional[datetime]:
        query = f"SELECT max(timestamp) AS max_ts FROM {self.table}"
        conditions = []
        params = {}
        if device:
            conditions.append("device = {device:String}")
            params["device"] = device
        if session_system:
            conditions.append("session_system = {session_system:String}")
            params["session_system"] = session_system
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        result = self.client.query(query, parameters=params)
        row = result.first_row
        if not row or row[0] is None:
            return None
        ts = row[0]
        if isinstance(ts, datetime) and ts.tzinfo is None:
            return ts.replace(tzinfo=UTC)
        return ts
