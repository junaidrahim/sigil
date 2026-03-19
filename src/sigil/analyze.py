"""Analyze command: compute metrics and generate analysis output."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from sigil.models import Snapshot
from sigil.storage.base import StorageBackend


@dataclass
class Metrics:
    """Aggregate usage metrics."""

    period_start: str = ""
    period_end: str = ""
    total_sessions: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_tokens: int = 0
    unique_days: int = 0
    sessions_by_source: dict[str, int] = field(default_factory=dict)
    tokens_by_source: dict[str, dict[str, int]] = field(default_factory=dict)
    tool_usage: dict[str, int] = field(default_factory=dict)
    top_tools_by_source: dict[str, dict[str, int]] = field(default_factory=dict)
    days_active_by_source: dict[str, int] = field(default_factory=dict)
    avg_tokens_per_session: int = 0
    input_output_ratio: float = 0.0
    models_used: dict[str, int] = field(default_factory=dict)
    sessions_per_day: float = 0.0
    busiest_day: str = ""
    busiest_day_sessions: int = 0


def parse_period(period: str) -> timedelta:
    """Parse a period string like '7d', '30d', '90d' into a timedelta."""
    period = period.strip().lower()
    if period.endswith("d"):
        return timedelta(days=int(period[:-1]))
    elif period.endswith("w"):
        return timedelta(weeks=int(period[:-1]))
    raise ValueError(f"Invalid period format: {period}. Use e.g. '7d', '30d', '90d'.")


def compute_metrics(
    storage: StorageBackend,
    period: str = "30d",
    source: str | None = None,
) -> Metrics:
    """Compute aggregate metrics from stored snapshots."""
    delta = parse_period(period)
    now = datetime.utcnow()
    since = now - delta

    src_filter = source if source and source != "all" else None
    snapshots = storage.list_snapshots(source=src_filter, since=since)

    metrics = Metrics(
        period_start=since.isoformat(),
        period_end=now.isoformat(),
    )

    all_days: set[str] = set()
    days_by_source: defaultdict[str, set[str]] = defaultdict(set)
    sessions_per_date: Counter[str] = Counter()
    tool_counter: Counter[str] = Counter()
    tool_by_source: defaultdict[str, Counter[str]] = defaultdict(Counter)
    source_sessions: Counter[str] = Counter()
    source_tokens: defaultdict[str, Counter[str]] = defaultdict(Counter)
    model_counter: Counter[str] = Counter()

    for snap in snapshots:
        for session in snap.sessions:
            metrics.total_sessions += 1
            metrics.total_input_tokens += session.input_tokens
            metrics.total_output_tokens += session.output_tokens
            metrics.total_tokens += session.total_tokens

            source_sessions[session.source] += 1
            source_tokens[session.source]["input"] += session.input_tokens
            source_tokens[session.source]["output"] += session.output_tokens
            source_tokens[session.source]["total"] += session.total_tokens

            if session.model:
                model_counter[session.model] += 1

            # Track active days
            day = _extract_day(session.started_at)
            if day:
                all_days.add(day)
                days_by_source[session.source].add(day)
                sessions_per_date[day] += 1

            # Track tool usage
            for tu in session.tool_uses:
                tool_counter[tu.tool_name] += tu.count
                tool_by_source[session.source][tu.tool_name] += tu.count

    metrics.unique_days = len(all_days)
    metrics.sessions_by_source = dict(source_sessions)
    metrics.tokens_by_source = {s: dict(c) for s, c in source_tokens.items()}
    metrics.tool_usage = dict(tool_counter.most_common(20))
    metrics.top_tools_by_source = {
        s: dict(c.most_common(10)) for s, c in tool_by_source.items()
    }
    metrics.days_active_by_source = {s: len(d) for s, d in days_by_source.items()}
    metrics.models_used = dict(model_counter)

    if metrics.total_sessions > 0:
        metrics.avg_tokens_per_session = metrics.total_tokens // metrics.total_sessions

    if metrics.total_output_tokens > 0:
        metrics.input_output_ratio = round(
            metrics.total_input_tokens / metrics.total_output_tokens, 2
        )

    if metrics.unique_days > 0:
        metrics.sessions_per_day = round(
            metrics.total_sessions / metrics.unique_days, 1
        )

    if sessions_per_date:
        busiest = sessions_per_date.most_common(1)[0]
        metrics.busiest_day = busiest[0]
        metrics.busiest_day_sessions = busiest[1]

    return metrics


def format_as_json(metrics: Metrics) -> str:
    """Format metrics as JSON."""
    from dataclasses import asdict

    return json.dumps(asdict(metrics), indent=2)


def format_as_markdown(metrics: Metrics) -> str:
    """Format metrics as a markdown summary for Hugo data files."""
    lines = [
        f"# AI Usage Summary",
        f"",
        f"**Period:** {metrics.period_start[:10]} to {metrics.period_end[:10]}",
        f"",
        f"## Overview",
        f"",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total sessions | {metrics.total_sessions} |",
        f"| Unique days active | {metrics.unique_days} |",
        f"| Sessions/day | {metrics.sessions_per_day} |",
        f"| Total tokens | {metrics.total_tokens:,} |",
        f"| Avg tokens/session | {metrics.avg_tokens_per_session:,} |",
        f"| Input/output ratio | {metrics.input_output_ratio} |",
        f"",
    ]

    if metrics.sessions_by_source:
        lines.append("## By Source")
        lines.append("")
        lines.append("| Source | Sessions | Days Active | Total Tokens |")
        lines.append("|--------|----------|-------------|--------------|")
        for src in sorted(metrics.sessions_by_source):
            sessions = metrics.sessions_by_source[src]
            days = metrics.days_active_by_source.get(src, 0)
            tokens = metrics.tokens_by_source.get(src, {}).get("total", 0)
            lines.append(f"| {src} | {sessions} | {days} | {tokens:,} |")
        lines.append("")

    if metrics.tool_usage:
        lines.append("## Top Tools")
        lines.append("")
        lines.append("| Tool | Uses |")
        lines.append("|------|------|")
        for tool, count in sorted(metrics.tool_usage.items(), key=lambda x: -x[1])[:10]:
            lines.append(f"| {tool} | {count} |")
        lines.append("")

    return "\n".join(lines)


def format_as_prompt(metrics: Metrics) -> str:
    """Format metrics as a structured prompt for Claude to analyze."""
    metrics_json = format_as_json(metrics)

    return f"""You are analyzing my personal AI usage data across three modalities:
- **Work Claude Code**: infrastructure/platform engineering at my job
- **Personal Claude Code**: writing, thinking, creative projects
- **OpenClaw**: an open-source autonomous AI assistant running locally

Here are my usage metrics for the period {metrics.period_start[:10]} to {metrics.period_end[:10]}:

```json
{metrics_json}
```

Please analyze this data with the following lens:

1. **Notable patterns**: What stands out? What's surprising or notable about the distribution of usage across modalities?

2. **Cross-modality comparison**: How does my work AI usage differ from personal usage? What does the tool usage breakdown reveal about how I use AI differently in each context?

3. **Engagement signals**: Based on sessions per day, token ratios, and tool patterns — am I using AI as a crutch, a collaborator, or a tool? Does this differ by modality?

4. **Blind spots**: What might I not be noticing about my own usage? What questions should I be asking?

5. **Commentary suggestions**: I maintain a living essay about my AI usage on my blog. Based on this data, what would be worth writing about? What would be interesting to readers who are curious about how someone integrates AI across their work and personal life?

Keep the tone reflective and honest, not corporate-dashboard. I'm interested in what the data reveals about my relationship with these tools, not just the numbers.

Write in second person ("you") and be direct. If the data is thin or uninteresting, say so — don't manufacture insights."""


def _extract_day(timestamp: str | None) -> str | None:
    """Extract YYYY-MM-DD from a timestamp string."""
    if not timestamp:
        return None
    try:
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        # Try just taking the first 10 chars
        if len(timestamp) >= 10:
            return timestamp[:10]
        return None
