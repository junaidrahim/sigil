---
name: sigil
description: >
  Interact with the sigil CLI tool for collecting and analyzing AI usage data
  from Claude Code and OpenAI Codex CLI. Use this skill when the user asks
  about their AI usage patterns, wants to push new session data, wants to
  generate usage reports, or references their "AI stack" or "AI usage". Also
  trigger when the user asks about token consumption, session history, tool
  usage patterns across their AI tools, or wants to update their living essay
  / AI log on their blog. This skill requires the `sigil` CLI to be installed
  and available on PATH.
---

# Sigil — AI Usage Data Tool

Sigil auto-detects and collects session logs from two AI CLI systems:

- **Claude Code** (`~/.claude/projects/`) — Anthropic's CLI agent
- **Codex** (`~/.codex/sessions/`) — OpenAI's CLI agent

All entries are parsed into a universal row format (`SessionRow`) with named columns for every known field and an `extras` column for overflow — no data is lost. Data is sanitized aggressively before storage.

## Pushing Session Data

```bash
# Auto-detect all session logs and push (no flags needed)
sigil push

# Override device name
sigil push --device work-laptop
```

Push auto-detects which systems are installed and parses all JSONL session files it finds.

## Analyzing Usage

```bash
# Generate an analysis prompt (default) — pipe to Claude for narrative
sigil analyze --period 30d | claude -p "analyze my AI usage"

# Raw metrics as JSON
sigil analyze --period 7d --format json

# Markdown summary for Hugo
sigil analyze --period 90d --format markdown

# Filter by system
sigil analyze --system claude_code --period 30d
sigil analyze --system codex --period 7d --format json
```

## Common Workflows

### Push and generate a blog entry
```bash
sigil push
sigil analyze --period 30d | claude -p "Write a reflective blog entry about my AI usage patterns" > ~/blog/content/ai-log/$(date +%Y-%m-%d).md
```

### Compare Claude vs Codex usage
```bash
sigil analyze --period 30d --format json | claude -p "Compare my Claude Code vs Codex usage from this JSON data"
```

### Top tools across all systems
```bash
sigil analyze --period 90d --format json | jq '.tool_usage'
```

## Row Schema

Each JSONL entry becomes one row with these columns: `session_system`, `session_id`, `timestamp`, `entry_type`, `message_role`, `message_text`, `content_type`, `tool_name`, `tool_input`, `tool_result_text`, `input_tokens`, `output_tokens`, `cache_creation_tokens`, `cache_read_tokens`, `model`, `model_provider`, `cwd`, `git_branch`, `cli_version`, and more. Unknown fields go into `extras` (JSON dict).

## Sanitization

Before storage:
1. **Code blocks removed**: Fenced and inline code replaced with placeholders
2. **Paths stripped**: File paths matching configured prefixes fully redacted
3. **Patterns redacted**: Regex matches (API keys, internal domains) replaced
4. **Thinking content**: Never stored

Config: `~/.sigil/config.toml`
```toml
[sanitize]
strip_paths = ["/Users/junaid/work/", "/home/junaid/atlan/"]
redact_patterns = ["atlan\\.com", "sk-[a-zA-Z0-9]+"]
strip_code_blocks = true
```
