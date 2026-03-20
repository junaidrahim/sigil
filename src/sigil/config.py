"""Configuration loading and management.

Resolution order per field:
    1. ``~/.sigil/config.toml`` (central config file)
    2. ``SIGIL_*`` environment variables
    3. Hard-coded defaults
"""

import os
import tomllib
from typing import Any, Dict, Optional

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

    catalog_name: str = "default"
    catalog_uri: str = ""
    catalog_token: str = ""
    warehouse: str = ""


class Config(BaseModel):
    """Top-level sigil configuration.

    Attributes:
        iceberg: Iceberg catalog connection settings.
        storage_backend: Which storage backend to use (``"local"`` or
            ``"iceberg"``).
    """

    iceberg: IcebergConfig = IcebergConfig()
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


def load_config() -> Config:
    """Load configuration from ``~/.sigil/config.toml`` with env var fallbacks.

    Returns:
        A fully resolved ``Config`` instance.
    """
    data: Dict[str, Any] = {}

    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "rb") as f:
            data = tomllib.load(f)

    iceberg_data = data.get("iceberg", {})
    iceberg = IcebergConfig(
        catalog_name=_resolve(iceberg_data.get("catalog_name"), "ICEBERG_CATALOG_NAME", "default"),
        catalog_uri=_resolve(iceberg_data.get("catalog_uri"), "ICEBERG_CATALOG_URI", ""),
        catalog_token=_resolve(iceberg_data.get("catalog_token"), "ICEBERG_CATALOG_TOKEN", ""),
        warehouse=_resolve(iceberg_data.get("warehouse"), "ICEBERG_WAREHOUSE", ""),
    )

    storage_backend = _resolve(data.get("storage_backend"), "STORAGE_BACKEND", "local")

    return Config(iceberg=iceberg, storage_backend=storage_backend)
