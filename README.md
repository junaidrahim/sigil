# sigil

Collect, store, and analyze your AI usage data from Claude Code and OpenAI Codex CLI.

## Install

```bash
uv tool install .
```

## Quick Start

```bash
# Interactive config setup
sigil init

# Push session data (auto-detects ~/.claude/projects/ and ~/.codex/sessions/)
sigil push

# Incremental push is the default — use --full to re-push everything
sigil push --full

# Analyze usage
sigil analyze --period 30d
sigil analyze --format json
sigil analyze --format markdown
```

## Storage Backends

Configure via `sigil init` or `~/.sigil/config.toml`. Three backends are supported:

| Backend | Use case | Config key |
|---|---|---|
| `local` (default) | Parquet files in `~/.sigil/rows/` | — |
| `iceberg` | Apache Iceberg via REST catalog | `[iceberg]` |
| `clickhouse` | ClickHouse Cloud/self-hosted | `[clickhouse]` |

Environment variables (`SIGIL_*`) can override any config value. See `.env.template` for the full list.

## Configuration

```toml
# ~/.sigil/config.toml
storage_backend = "clickhouse"

[iceberg]
catalog_name = "default"
catalog_uri = "http://localhost:8181"
warehouse = "s3://bucket/warehouse"

[clickhouse]
host = "localhost"
database = "sigil"
user = "default"
password = ""
```

## Development

```bash
uv run pytest              # Run tests
uv run ruff check src/     # Lint
uv run sigil --help        # CLI help
```

See `sigil --help` for full options.
