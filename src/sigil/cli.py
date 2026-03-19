"""CLI entrypoint for sigil."""

from __future__ import annotations

import platform
from pathlib import Path

import click
from rich.console import Console

from sigil.analyze import compute_metrics, format_as_json, format_as_markdown, format_as_prompt
from sigil.config import load_config, load_device_config, save_device_config
from sigil.push import build_snapshot
from sigil.storage.local import LocalStorage

console = Console(stderr=True)


@click.group()
@click.version_option()
def cli() -> None:
    """Sigil: collect, store, and analyze your AI usage data."""


@cli.command()
@click.option(
    "--source",
    type=click.Choice(["work", "personal", "openclaw"]),
    help="Which AI modality this data is from.",
)
@click.option("--device", default=None, help="Device name (defaults to hostname).")
@click.option(
    "--path",
    "sessions_path",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to Claude Code session files.",
)
def push(source: str | None, device: str | None, sessions_path: Path | None) -> None:
    """Push Claude Code session data from this device."""
    config = load_config()
    device_config = load_device_config()

    # Resolve device name
    if not device:
        device = device_config.get("device", platform.node())

    # Resolve source — require on first push, remember after
    if not source:
        source = device_config.get("source")
    if not source:
        console.print(
            "[bold red]Error:[/] --source is required on first push from this device.\n"
            "Use --source work|personal|openclaw"
        )
        raise SystemExit(1)

    # Resolve sessions path
    if not sessions_path:
        sessions_path = Path.home() / ".claude" / "projects"

    if not sessions_path.exists():
        console.print(f"[bold red]Error:[/] Sessions path not found: {sessions_path}")
        raise SystemExit(1)

    console.print(f"[dim]Source:[/]  {source}")
    console.print(f"[dim]Device:[/]  {device}")
    console.print(f"[dim]Path:[/]    {sessions_path}")
    console.print()

    # Build and save snapshot
    snapshot = build_snapshot(sessions_path, source, device, config.sanitize)

    if not snapshot.sessions:
        console.print("[yellow]No sessions found to push.[/]")
        return

    storage = LocalStorage()
    snapshot_id = storage.save_snapshot(snapshot)

    # Remember source and device for next time
    save_device_config({"source": source, "device": device})

    console.print(
        f"[bold green]Pushed {snapshot.session_count} sessions[/] "
        f"(snapshot: {snapshot_id[:8]}...)"
    )


@cli.command()
@click.option(
    "--period",
    default="30d",
    help="Time period to analyze (e.g. 7d, 30d, 90d).",
)
@click.option(
    "--source",
    type=click.Choice(["work", "personal", "openclaw", "all"]),
    default="all",
    help="Filter by source modality.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["prompt", "json", "markdown"]),
    default="prompt",
    help="Output format.",
)
def analyze(period: str, source: str, output_format: str) -> None:
    """Generate usage analysis from stored snapshots."""
    storage = LocalStorage()
    src = source if source != "all" else None
    metrics = compute_metrics(storage, period=period, source=src)

    if metrics.total_sessions == 0:
        console.print("[yellow]No sessions found for the specified period.[/]")
        return

    if output_format == "json":
        click.echo(format_as_json(metrics))
    elif output_format == "markdown":
        click.echo(format_as_markdown(metrics))
    else:
        click.echo(format_as_prompt(metrics))
