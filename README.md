# sigil

Collect, store, and analyze your AI usage data across modalities.

## Install

```bash
uv tool install .
```

## Usage

```bash
# Push session data
sigil push --source personal

# Analyze usage
sigil analyze --period 30d
sigil analyze --format json
sigil analyze --format markdown
```

See `sigil --help` for full options.
