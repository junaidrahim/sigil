"""CLI entrypoint for sigil."""

import platform
from typing import Dict, Iterator, Optional

import click
from rich.console import Console
from rich.prompt import Confirm, Prompt

from sigil.config import load_config
from sigil.constants import CONFIG_PATH, SIGIL_DIR
from sigil.models import SessionRow
from sigil.push import auto_detect_sources, push_all
from sigil.storage.base import StorageBackend
from sigil.storage.local import LocalStorage

console = Console(stderr=True)


def _get_storage(backend: str) -> StorageBackend:
    """Instantiate the configured storage backend.

    Args:
        backend: Backend name (``"local"``, ``"iceberg"``, or ``"clickhouse"``).

    Returns:
        A ``StorageBackend`` instance ready to accept rows.
    """
    if backend == "iceberg":
        from sigil.storage.iceberg import IcebergStorage

        config = load_config()
        return IcebergStorage(config.iceberg)
    if backend == "clickhouse":
        from sigil.storage.clickhouse import ClickHouseStorage

        config = load_config()
        return ClickHouseStorage(config.clickhouse)
    return LocalStorage()


class _CountingIterator:
    """Wraps an iterator to count rows by system as they flow through.

    Attributes:
        counts: Dict mapping ``session_system`` to row count, populated
            as rows are yielded.
    """

    def __init__(self, rows: Iterator[SessionRow]) -> None:
        """Initialise the counting wrapper.

        Args:
            rows: Source iterator of ``SessionRow`` instances.
        """
        self._rows = rows
        self.counts: Dict[str, int] = {}

    def __iter__(self) -> Iterator[SessionRow]:
        """Yield rows while updating per-system counts."""
        for row in self._rows:
            self.counts[row.session_system] = self.counts.get(row.session_system, 0) + 1
            yield row


@click.group()
@click.version_option()
def cli() -> None:
    """Sigil: collect, store, and analyze your AI usage data."""


@cli.command()
def init() -> None:
    """Interactively create ~/.sigil/config.toml."""
    if CONFIG_PATH.exists():
        overwrite = Confirm.ask(
            f"[yellow]{CONFIG_PATH} already exists.[/] Overwrite?",
            default=False,
            console=console,
        )
        if not overwrite:
            console.print("[dim]Aborted.[/]")
            return

    console.print("[bold]Sigil configuration[/]\n")

    backend = Prompt.ask(
        "Storage backend",
        choices=["local", "iceberg", "clickhouse"],
        default="local",
        console=console,
    )

    # Build TOML content
    lines = [f'storage_backend = "{backend}"']

    if backend == "iceberg":
        console.print("\n[bold]Iceberg catalog settings[/]\n")
        catalog_name = Prompt.ask("Catalog name", default="default", console=console)
        catalog_uri = Prompt.ask("Catalog URI", default="", console=console)
        catalog_token = Prompt.ask("Catalog token", default="", password=True, console=console)
        warehouse = Prompt.ask("Warehouse path", default="", console=console)
        lines.append("")
        lines.append("[iceberg]")
        lines.append(f'catalog_name = "{catalog_name}"')
        lines.append(f'catalog_uri = "{catalog_uri}"')
        if catalog_token:
            lines.append(f'catalog_token = "{catalog_token}"')
        if warehouse:
            lines.append(f'warehouse = "{warehouse}"')

    if backend == "clickhouse":
        console.print("\n[bold]ClickHouse connection settings[/]\n")
        ch_host = Prompt.ask("Host", default="localhost", console=console)
        ch_database = Prompt.ask("Database", default="sigil", console=console)
        ch_user = Prompt.ask("User", default="default", console=console)
        ch_password = Prompt.ask("Password", default="", password=True, console=console)
        lines.append("")
        lines.append("[clickhouse]")
        lines.append(f'host = "{ch_host}"')
        lines.append(f'database = "{ch_database}"')
        lines.append(f'user = "{ch_user}"')
        if ch_password:
            lines.append(f'password = "{ch_password}"')

    SIGIL_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text("\n".join(lines) + "\n")

    console.print(f"\n[bold green]Wrote {CONFIG_PATH}[/]")


@cli.command()
@click.option("--device", default=None, help="Device name (defaults to hostname).")
@click.option("--full", is_flag=True, help="Ignore watermark and push all data.")
def push(device: Optional[str], full: bool) -> None:
    """Auto-detect and push all session logs from this device.

    By default, only rows newer than the latest timestamp in storage
    are pushed (incremental). Use ``--full`` to re-push everything.
    """
    config = load_config()

    if not device:
        device = platform.node()

    sources = auto_detect_sources()
    if not sources:
        console.print("[yellow]No session log directories found.[/]")
        console.print("[dim]Looked for: ~/.claude/projects/, ~/.codex/sessions/, ~/.openclaw/agents/main/sessions/[/]")
        return

    for system, path in sources:
        console.print(f"[dim]Found:[/]  {system} -> {path}")

    # Query high-water mark from storage
    storage = _get_storage(config.storage_backend)
    watermark = None if full else storage.max_timestamp(device=device)

    if watermark:
        console.print(f"[dim]Watermark:[/]  {watermark.isoformat()}")

    # Stream rows through counting wrapper into storage
    counted = _CountingIterator(push_all(device, sources=sources, watermark=watermark))
    saved = storage.append(counted)

    if saved == 0:
        console.print("[yellow]No new session entries to push.[/]")
        return

    console.print()
    for system, count in sorted(counted.counts.items()):
        console.print(f"  {system}: {count:,} rows")
    console.print(f"\n[bold green]Pushed {saved:,} rows[/]")
