# Sigil

CLI tool for collecting, storing, and analyzing AI usage data from Claude Code and OpenAI Codex CLI.

## Architecture

- **CLI**: `click` framework, entrypoint in `src/sigil/cli.py`
- **Parsers**: Dedicated parsers per system in `parsers/claude.py` and `parsers/codex.py`. Both extend `SessionParser` base class (`parsers/base.py`) which provides `make_row_id()`, `_build_row()`, and `parse_file()`. Both produce `SessionRow`.
- **Models**: `SessionRow` (Pydantic) is the canonical row schema. Every field from both log formats maps to a named column; overflow goes to `extras` dict. Schema generation for Iceberg and ClickHouse is driven from model fields via `_unwrap_optional()`. See `src/sigil/models.py`.
- **Storage**: Abstract `StorageBackend` base class (`storage/base.py`) with concrete `append()` handling chunked iteration. Three backends: `LocalStorage` (parquet), `IcebergStorage` (REST catalog), `ClickHouseStorage`. Subclasses implement `_convert_row()`, `_flush_chunk()`, and optionally `_pre_append()`.
- **Config**: `~/.sigil/config.toml` with `SIGIL_*` env var fallbacks. Data-driven resolution via `_env_map` on config models. See `src/sigil/config.py`.

## Commands

- `sigil init` — interactive config setup
- `sigil push` — auto-detects `~/.claude/projects/` and `~/.codex/sessions/`, parses all JSONL files, stores as rows (incremental by default via watermark)
- `sigil push --full` — re-push all data ignoring watermark

## Development

```bash
uv run pytest              # Run tests (42 tests)
uv run ruff check src/     # Lint
uv run sigil --help        # CLI help
```

## Code Style

- Type hints everywhere
- Pydantic for the row model
- `click` for CLI, `rich` for terminal output (stderr only)
- Keep dependencies minimal

## Row Schema (SessionRow)

One row per JSONL entry. Key columns: `row_id`, `session_id`, `session_system`, `device`, `pushed_at`, `timestamp`, `entry_type`, `message_role`, `message_text`, `content_type`, `tool_name`, `tool_input`, `tool_result_text`, `input_tokens`, `output_tokens`, `cache_creation_tokens`, `cache_read_tokens`, `model`, `model_provider`, `cwd`, `git_branch`, `cli_version`, `parent_uuid`, `request_id`, `stop_reason`, `source_file`, `source_line`, `extras`.

## Storage Pattern

All backends extend `StorageBackend` ABC. Base class provides concrete `append()` with chunking. Subclasses implement `_convert_row()`, `_flush_chunk()`, `max_timestamp()`. Rows are append-only.

## PyPI

Published as `sigil-ai` on PyPI. Install via `pip install sigil-ai`.
