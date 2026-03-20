"""Tests for config loading with env var fallbacks."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

from sigil.config import load_config


class TestConfigEnvFallback:
    def test_env_storage_backend(self):
        with (
            patch("sigil.config.CONFIG_PATH", Path("/nonexistent/config.toml")),
            patch.dict(os.environ, {"SIGIL_STORAGE_BACKEND": "iceberg"}),
        ):
            config = load_config()
            assert config.storage_backend == "iceberg"

    def test_env_iceberg_config(self):
        env = {
            "SIGIL_ICEBERG_CATALOG_NAME": "prod",
            "SIGIL_ICEBERG_CATALOG_URI": "http://localhost:8181",
            "SIGIL_ICEBERG_WAREHOUSE": "s3://my-bucket/warehouse",
        }
        with (
            patch("sigil.config.CONFIG_PATH", Path("/nonexistent/config.toml")),
            patch.dict(os.environ, env),
        ):
            config = load_config()
            assert config.iceberg.catalog_name == "prod"
            assert config.iceberg.catalog_uri == "http://localhost:8181"
            assert config.iceberg.warehouse == "s3://my-bucket/warehouse"

    def test_file_takes_precedence_over_env(self):
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".toml", delete=False) as f:
            f.write(b'storage_backend = "local"\n')
            config_path = Path(f.name)

        try:
            with (
                patch("sigil.config.CONFIG_PATH", config_path),
                patch.dict(os.environ, {"SIGIL_STORAGE_BACKEND": "iceberg"}),
            ):
                config = load_config()
                assert config.storage_backend == "local"
        finally:
            config_path.unlink()

    def test_defaults_when_no_config_no_env(self):
        clean_env = {k: v for k, v in os.environ.items() if not k.startswith("SIGIL_")}
        with (
            patch("sigil.config.CONFIG_PATH", Path("/nonexistent/config.toml")),
            patch.dict(os.environ, clean_env, clear=True),
        ):
            config = load_config()
            assert config.storage_backend == "local"
            assert config.iceberg.catalog_uri == ""
