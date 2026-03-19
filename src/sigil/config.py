"""Configuration loading and management."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SIGIL_DIR = Path.home() / ".sigil"
CONFIG_PATH = SIGIL_DIR / "config.toml"
DEVICE_CONFIG_PATH = SIGIL_DIR / "device.toml"


@dataclass
class SanitizeConfig:
    """Sanitization settings."""

    strip_paths: list[str] = field(default_factory=list)
    redact_patterns: list[str] = field(default_factory=list)
    strip_code_blocks: bool = True


@dataclass
class Config:
    """Top-level sigil configuration."""

    sanitize: SanitizeConfig = field(default_factory=SanitizeConfig)
    storage_backend: str = "local"
    raw: dict[str, Any] = field(default_factory=dict)


def load_config() -> Config:
    """Load config from ~/.sigil/config.toml, returning defaults if not found."""
    if not CONFIG_PATH.exists():
        return Config()

    with open(CONFIG_PATH, "rb") as f:
        data = tomllib.load(f)

    sanitize_data = data.get("sanitize", {})
    sanitize = SanitizeConfig(
        strip_paths=sanitize_data.get("strip_paths", []),
        redact_patterns=sanitize_data.get("redact_patterns", []),
        strip_code_blocks=sanitize_data.get("strip_code_blocks", True),
    )

    return Config(
        sanitize=sanitize,
        storage_backend=data.get("storage_backend", "local"),
        raw=data,
    )


def load_device_config() -> dict[str, Any]:
    """Load per-device config (remembered source, etc.)."""
    if not DEVICE_CONFIG_PATH.exists():
        return {}

    with open(DEVICE_CONFIG_PATH, "rb") as f:
        return tomllib.load(f)


def save_device_config(data: dict[str, Any]) -> None:
    """Save per-device config as TOML."""
    SIGIL_DIR.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    for key, value in data.items():
        if isinstance(value, str):
            lines.append(f'{key} = "{value}"')
        elif isinstance(value, bool):
            lines.append(f"{key} = {'true' if value else 'false'}")
        else:
            lines.append(f"{key} = {value}")
    DEVICE_CONFIG_PATH.write_text("\n".join(lines) + "\n")
