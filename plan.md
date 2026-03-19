# Claude Code Prompt: Build `sigil` CLI Tool

## Context

I want to build a standalone CLI tool called `sigil` (or suggest a better name if you have one — something short, unix-y). This tool helps me collect, store, and analyze my AI usage data across three modalities:

1. **Work Claude Code** — my work subscription, used for infrastructure/platform engineering
2. **Personal Claude Code** — my personal subscription, used for writing, thinking, creative work
3. **OpenClaw** — an open-source personal AI assistant (https://openclaw.ai) running on my machine, used for autonomous delegation

The end goal is to power a "living essay" page on my Hugo blog (junaid.foo) that shows how my AI usage patterns evolve over time. But this tool is the data layer — collection and analysis. The Hugo rendering is a separate concern.

## Architecture

### Storage

I have a Cloudflare R2 bucket with Apache Iceberg tables. The tool should write data there as append-only versioned records. Each push creates a new snapshot. This gives me time-travel semantics so I can always reprocess historical data.

For now, keep the storage layer abstract behind an interface — start with local JSON files in `~/.sigil/` as the default backend, with R2/Iceberg as a future backend. Don't implement the R2 backend yet, just make sure the abstraction is clean enough that I can add it later.

### Language

Python. Use `click` for the CLI framework. Keep dependencies minimal. Use `uv` for project management.

---

## Commands

### `sigil push`

Pushes Claude Code session data from the current device.

```
sigil push [--source work|personal|openclaw] [--device <name>] [--path <path-to-sessions>]
```

What this does:

1. Reads Claude Code JSONL session files from `~/.claude/projects/` (or a custom path)
2. Tags each session with:
   - `source`: which modality (work/personal/openclaw)
   - `device`: machine identifier (auto-detect hostname, allow override)
   - `pushed_at`: timestamp of this push
3. Sanitizes the data:
   - Strips file paths that match configurable patterns (e.g., anything under company project dirs)
   - Strips code blocks and inline code from conversation content
   - Redacts strings matching configurable regex patterns (API keys, internal URLs, etc.)
   - Keeps: conversation structure, token counts, tool usage, timestamps, topic/intent if present
4. Writes a new snapshot to the storage backend with metadata

The sanitization config should live in `~/.sigil/config.toml`:

```toml
[sanitize]
strip_paths = ["/Users/junaid/work/", "/home/junaid/atlan/"]
redact_patterns = ["atlan\\.com", "sk-[a-zA-Z0-9]+"]
strip_code_blocks = true
```

The `--source` flag should be required on first push from a device but remembered after that (stored in local config per device).

### `sigil analyze`

Generates an analysis prompt that I can pipe into Claude Code or paste into Claude.

```
sigil analyze [--period 30d|7d|90d] [--source work|personal|openclaw|all] [--format prompt|json|markdown]
```

What this does:

1. Reads all snapshots from the storage backend within the specified period
2. Computes aggregate metrics:
   - Total sessions, tokens (input/output), unique days active per source
   - Tool usage breakdown (which tools used how often)
   - Session duration distribution
   - Topic/intent clusters if extractable
   - Friction signals (abandoned sessions, error patterns)
   - Cross-source comparison (how does work vs personal vs openclaw usage differ)
3. With `--format prompt` (default): outputs a structured prompt with the raw metrics embedded, designed to be piped to `claude -p` for narrative analysis. The prompt should ask Claude to:
   - Identify notable patterns and shifts
   - Compare across modalities
   - Surface things I might not notice myself
   - Suggest what to write about in the commentary log
   - Keep the tone reflective, not corporate-dashboard
4. With `--format json`: outputs the raw metrics as JSON
5. With `--format markdown`: outputs a simple markdown summary suitable for inclusion in Hugo data files

Example workflow:
```bash
sigil analyze --period 30d | claude -p "analyze my AI usage" > ~/blog/content/ai-log/2026-03-20.md
```

---

## Project Structure

```
sigil/
├── pyproject.toml
├── README.md
├── CLAUDE.md                    # For Claude Code context
├── src/
│   └── sigil/
│       ├── __init__.py
│       ├── cli.py               # Click CLI entrypoint
│       ├── push.py              # Push command logic
│       ├── analyze.py           # Analyze command logic
│       ├── sanitize.py          # Sanitization/redaction engine
│       ├── storage/
│       │   ├── __init__.py
│       │   ├── base.py          # Abstract storage interface
│       │   └── local.py         # Local JSON file backend
│       ├── models.py            # Data models (dataclasses or pydantic)
│       └── config.py            # Config loading (TOML)
├── skill/
│   └── SKILL.md                 # Agent skill (see below)
└── tests/
    └── ...
```

---

## Skill: `skill/SKILL.md`

Create a skill that any Claude Code agent (or OpenClaw, or any agent with skill support) can use to interact with sigil. The skill should be self-contained and usable by an agent that has access to the CLI.

```yaml
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
```

The skill body should explain:
- What sigil is and what data it tracks
- How to run `sigil push` (including when to use which `--source`)
- How to run `sigil analyze` and what to do with the output
- How to pipe analysis output to Claude for narrative generation
- The sanitization model (so the agent understands what's safe to share publicly)
- Example workflows:
  - "Push my latest sessions and generate a blog entry"
  - "Compare my work vs personal AI usage this month"
  - "What tools am I using most across all modalities?"

---

## CLAUDE.md

Write a CLAUDE.md for the repo that includes:
- Project purpose and architecture overview
- How to run tests (`uv run pytest`)
- Code style: type hints everywhere, dataclasses over dicts, click for CLI
- Storage backend abstraction pattern
- The sanitization philosophy: aggressive by default, configurable
- Note that R2/Iceberg backend is planned but not yet implemented

---

## Implementation Notes

- Don't over-engineer. This is a personal tool, not a product. But keep the abstractions clean because I will extend it.
- The JSONL parsing should be resilient — Claude Code session files can vary in structure. Parse what you can, skip what you can't, log warnings.
- For the analysis prompt generation, think carefully about what metrics are actually interesting to reflect on vs what's just noise. Token counts alone aren't insightful — ratios and comparisons are.
- The sanitization engine is critical. I'd rather lose data than leak proprietary context. Default to stripping aggressively.
- Make the CLI output clean. Use `rich` for terminal formatting if it helps, but don't go overboard.

---

## What to build first

1. Project scaffolding (pyproject.toml, directory structure, CLAUDE.md)
2. Config loading
3. Data models
4. Local storage backend
5. `push` command with sanitization
6. `analyze` command with prompt generation
7. The skill file
8. Tests for sanitization (this is the part that must not have bugs)

Start building. Ask me questions only if something is ambiguous enough that a wrong guess would waste significant effort.