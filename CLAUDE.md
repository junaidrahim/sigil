# Sigil

Personal CLI tool for collecting, storing, and analyzing AI usage data from Claude Code and OpenAI Codex CLI.

## Architecture

- **CLI**: `click` framework, entrypoint in `src/sigil/cli.py`
- **Parsers**: Dedicated parsers per system in `parsers/claude.py` and `parsers/codex.py`. Both produce `SessionRow` — the universal Pydantic row model.
- **Models**: `SessionRow` (Pydantic) is the canonical Iceberg table row schema. Every field from both log formats maps to a named column; overflow goes to `extras` dict. See `src/sigil/models.py`.
- **Storage**: Abstract `StorageBackend` interface (`storage/base.py`) with local JSON implementation (`storage/local.py`). R2/Iceberg backend planned but not implemented.
- **Sanitization**: Aggressive-by-default engine in `sanitize.py`. Strips code blocks, redacts file paths and regex patterns. Config in `~/.sigil/config.toml`.

## Commands

- `sigil push` — auto-detects `~/.claude/projects/` and `~/.codex/sessions/`, parses all JSONL files, sanitizes, stores as rows
- `sigil analyze` — computes metrics from stored rows, outputs as prompt/json/markdown

## Development

```bash
uv run pytest              # Run tests (64 tests)
uv run sigil --help        # CLI help
uv run sigil push
uv run sigil analyze --period 30d --format json
```

## Code Style

- Type hints everywhere
- Pydantic for the row model, dataclasses for internal structures
- `click` for CLI, `rich` for terminal output (stderr only)
- Keep dependencies minimal

## Row Schema (SessionRow)

One row per JSONL entry. Key columns: `row_id`, `session_id`, `session_system`, `timestamp`, `entry_type`, `message_role`, `message_text`, `content_type`, `tool_name`, `tool_input`, `tool_result_text`, `input_tokens`, `output_tokens`, `cache_creation_tokens`, `cache_read_tokens`, `model`, `model_provider`, `cwd`, `git_branch`, `cli_version`, `parent_uuid`, `request_id`, `stop_reason`, `source_file`, `source_line`, `extras`.

## Sanitization Philosophy

Aggressive by default. Better to lose data than leak proprietary context. Code blocks stripped before pattern matching. Thinking content is never stored. All configurable via `~/.sigil/config.toml`.

## Storage Pattern

All backends implement `StorageBackend` ABC: `save_rows`, `query_rows`, `row_count`. Rows are append-only.
