---
name: sigil
description: >
  Interact with the sigil CLI tool for collecting and analyzing AI usage data
  across multiple modalities (work Claude, personal Claude, OpenClaw). Use this
  skill when the user asks about their AI usage patterns, wants to push new
  session data, wants to generate usage reports, or references their "AI stack"
  or "AI usage". Also trigger when the user asks about token consumption,
  session history, tool usage patterns across their AI tools, or wants to
  update their living essay / AI log on their blog. This skill requires the
  `sigil` CLI to be installed and available on PATH.
---

# Sigil — AI Usage Data Tool

Sigil collects, stores, and analyzes AI usage data from three sources:

- **work** — Work Claude Code (infrastructure/platform engineering)
- **personal** — Personal Claude Code (writing, thinking, creative work)
- **openclaw** — OpenClaw autonomous AI assistant (local delegation)

Data is sanitized aggressively before storage: code blocks are stripped, file paths matching sensitive patterns are redacted, and configurable regex patterns (API keys, internal URLs) are removed. This means stored data is safe to share publicly (e.g., on a blog).

## Pushing Session Data

```bash
# Push from this device (source remembered after first use)
sigil push --source personal

# Push with explicit device name and custom session path
sigil push --source work --device work-laptop --path ~/.claude/projects

# Push OpenClaw sessions
sigil push --source openclaw --path ~/openclaw/sessions
```

The `--source` flag is required on the first push from a device, then remembered. Sessions are read from `~/.claude/projects/` by default.

## Analyzing Usage

```bash
# Generate an analysis prompt (default) — pipe to Claude for narrative
sigil analyze --period 30d | claude -p "analyze my AI usage"

# Get raw metrics as JSON
sigil analyze --period 7d --format json

# Markdown summary for Hugo data files
sigil analyze --period 90d --format markdown

# Filter by source
sigil analyze --source work --period 30d
sigil analyze --source personal --period 7d --format json
```

## Common Workflows

### Push latest sessions and generate a blog entry
```bash
sigil push --source personal
sigil analyze --period 30d | claude -p "Write a reflective blog entry about my AI usage patterns" > ~/blog/content/ai-log/$(date +%Y-%m-%d).md
```

### Compare work vs personal AI usage this month
```bash
sigil analyze --period 30d --format json | claude -p "Compare my work vs personal AI usage from this JSON data"
```

### What tools am I using most across all modalities?
```bash
sigil analyze --period 90d --format json | jq '.tool_usage'
```

### Quick stats check
```bash
sigil analyze --period 7d --format markdown
```

## How Sanitization Works

Before any data is stored, sigil applies these transformations:
1. **Code block removal**: All fenced (```) and inline (`) code is replaced with placeholders
2. **Path stripping**: File paths matching configured prefixes are replaced with `[path redacted]`
3. **Pattern redaction**: Strings matching configured regexes (API keys, internal domains) are replaced with `[redacted]`

What is preserved: conversation structure, token counts, tool usage, timestamps, topics.

Configuration lives in `~/.sigil/config.toml`:
```toml
[sanitize]
strip_paths = ["/Users/junaid/work/", "/home/junaid/atlan/"]
redact_patterns = ["atlan\\.com", "sk-[a-zA-Z0-9]+"]
strip_code_blocks = true
```
