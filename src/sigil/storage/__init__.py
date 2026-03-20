"""Storage backends for sigil."""

from sigil.storage.base import StorageBackend
from sigil.storage.clickhouse import ClickHouseStorage
from sigil.storage.iceberg import IcebergStorage
from sigil.storage.local import LocalStorage

__all__ = ["StorageBackend", "LocalStorage", "IcebergStorage", "ClickHouseStorage"]
