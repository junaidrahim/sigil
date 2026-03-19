# Sigil

Personal CLI tool for collecting, storing, and analyzing AI usage data across three modalities: Work Claude Code, Personal Claude Code, and OpenClaw.

## Architecture

- **CLI**: `click` framework, entrypoint in `src/sigil/cli.py`
- **Storage**: Abstract `StorageBackend` interface (`storage/base.py`) with local JSON implementation (`storage/local.py`). R2/Iceberg backend planned but not implemented.
- **Sanitization**: Aggressive-by-default engine in `sanitize.py`. Strips code blocks, redacts file paths and regex patterns. Config lives in `~/.sigil/config.toml`.
- **Models**: Dataclasses in `models.py` — `Session`, `Snapshot`, `Message`, `ToolUse`.

## Commands

- `sigil push` — reads Claude Code JSONL sessions, sanitizes, stores as snapshot
- `sigil analyze` — computes metrics from snapshots, outputs as prompt/json/markdown

## Development

```bash
uv run pytest              # Run tests
uv run sigil --help        # CLI help
uv run sigil push --source personal
uv run sigil analyze --period 30d --format json
```

## Code Style

- Type hints everywhere
- Dataclasses over dicts for structured data
- `click` for CLI, `rich` for terminal output (stderr only)
- Keep dependencies minimal

## Sanitization Philosophy

Aggressive by default. Better to lose data than leak proprietary context. Code blocks are stripped before pattern matching runs. All configurable via `~/.sigil/config.toml`.

## Storage Pattern

All backends implement `StorageBackend` ABC with three methods: `save_snapshot`, `list_snapshots`, `get_snapshot`. Snapshots are append-only with time-travel semantics.
