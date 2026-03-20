"""Shared constants used across the sigil package."""

from pathlib import Path

# sigil data directory
SIGIL_DIR = Path.home() / ".sigil"
CONFIG_PATH = SIGIL_DIR / "config.toml"
ROWS_DIR = SIGIL_DIR / "rows"

# Session log locations
CLAUDE_SESSIONS_DIR = Path.home() / ".claude" / "projects"
CODEX_SESSIONS_DIR = Path.home() / ".codex" / "sessions"

# Storage
DEFAULT_CHUNK_SIZE = 100_000
