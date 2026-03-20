"""Configuration loading and management.

Resolution order per field:
    1. ``~/.sigil/config.toml`` (central config file)
    2. ``SIGIL_*`` environment variables
    3. Hard-coded defaults
"""

import os
import tomllib
from typing import Any, ClassVar, Dict, Optional, Type

from dotenv import load_dotenv
from pydantic import BaseModel

from sigil.constants import CONFIG_PATH

load_dotenv()


class IcebergConfig(BaseModel):
    """Iceberg REST catalog connection settings.

    Attributes:
        catalog_name: Logical name for the catalog used by pyiceberg.
        catalog_uri: URI of the Iceberg REST catalog.
        warehouse: Warehouse location (e.g. an S3 path).
    """

    _env_map: ClassVar[Dict[str, str]] = {
        "catalog_name": "ICEBERG_CATALOG_NAME",
        "catalog_uri": "ICEBERG_CATALOG_URI",
        "catalog_token": "ICEBERG_CATALOG_TOKEN",
        "warehouse": "ICEBERG_WAREHOUSE",
    }

    catalog_name: str = "default"
    catalog_uri: str = ""
    catalog_token: str = ""
    warehouse: str = ""


class ClickHouseConfig(BaseModel):
    """ClickHouse connection settings.

    Attributes:
        host: ClickHouse server hostname.
        port: ClickHouse native protocol port.
        database: Target database name.
        user: Authentication username.
        password: Authentication password.
        secure: Whether to use TLS for the connection.
    """

    _env_map: ClassVar[Dict[str, str]] = {
        "host": "CLICKHOUSE_HOST",
        "database": "CLICKHOUSE_DATABASE",
        "user": "CLICKHOUSE_USER",
        "password": "CLICKHOUSE_PASSWORD",
    }

    host: str = "localhost"
    database: str = "sigil"
    user: str = "default"
    password: str = ""


class Config(BaseModel):
    """Top-level sigil configuration.

    Attributes:
        iceberg: Iceberg catalog connection settings.
        clickhouse: ClickHouse connection settings.
        storage_backend: Which storage backend to use (``"local"``,
            ``"iceberg"``, or ``"clickhouse"``).
    """

    iceberg: IcebergConfig = IcebergConfig()
    clickhouse: ClickHouseConfig = ClickHouseConfig()
    storage_backend: str = "local"


def _resolve(file_val: Optional[Any], env_key: str, default: str) -> str:
    """Pick the first non-empty value from file, env, or default.

    Args:
        file_val: Value read from the TOML config file (may be ``None``).
        env_key: Suffix of the ``SIGIL_`` prefixed environment variable.
        default: Fallback value when both file and env are empty.

    Returns:
        The resolved string value.
    """
    if file_val is not None and str(file_val) != "":
        return str(file_val)
    env_val = os.environ.get(f"SIGIL_{env_key}", "")
    if env_val:
        return env_val
    return default


def _resolve_model(model_cls: Type[BaseModel], file_data: Dict[str, Any]) -> BaseModel:
    """Resolve all fields of a config model using file data, env vars, and defaults."""
    kwargs: Dict[str, str] = {}
    for field_name, info in model_cls.model_fields.items():
        env_key = model_cls._env_map[field_name]  # type: ignore[attr-defined]
        default = info.default if info.default is not None else ""
        kwargs[field_name] = _resolve(file_data.get(field_name), env_key, str(default))
    return model_cls(**kwargs)


def load_config() -> Config:
    """Load configuration from ``~/.sigil/config.toml`` with env var fallbacks.

    Returns:
        A fully resolved ``Config`` instance.
    """
    data: Dict[str, Any] = {}

    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "rb") as f:
            data = tomllib.load(f)

    iceberg = _resolve_model(IcebergConfig, data.get("iceberg", {}))
    clickhouse = _resolve_model(ClickHouseConfig, data.get("clickhouse", {}))
    storage_backend = _resolve(data.get("storage_backend"), "STORAGE_BACKEND", "local")

    return Config(iceberg=iceberg, clickhouse=clickhouse, storage_backend=storage_backend)
