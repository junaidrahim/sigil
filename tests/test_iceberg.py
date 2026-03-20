"""Tests for the Iceberg storage backend.

Uses a SQLite catalog for test isolation (sql-sqlite is a dev-only dependency).
Production uses REST catalogs exclusively.
"""

import tempfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import daft
import pyarrow as pa
from pyiceberg.catalog import load_catalog

from sigil.models import SessionRow


def _make_row(
    session_system: str = "claude_code",
    session_id: str = "sess-1",
    timestamp: Optional[datetime] = None,
    tool_name: Optional[str] = None,
    input_tokens: Optional[int] = 100,
    output_tokens: Optional[int] = 200,
    model: Optional[str] = "claude-opus-4-6",
    **kwargs,
) -> SessionRow:
    ts = timestamp or datetime(2026, 3, 15, 10, 0, 0)
    return SessionRow(
        row_id=f"row-{id(ts)}",
        session_id=session_id,
        session_system=session_system,
        device="test-mac",
        pushed_at=datetime(2026, 3, 20, 12, 0, 0),
        timestamp=ts,
        entry_type="assistant",
        message_role="assistant",
        message_text="hello",
        content_type="text",
        tool_name=tool_name,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        model=model,
        model_provider="anthropic",
        source_file="/tmp/test.jsonl",
        source_line=1,
        **kwargs,
    )


def _create_test_table(tmpdir: str):
    """Create a test Iceberg table using a SQLite catalog (test-only)."""
    warehouse = Path(tmpdir) / "warehouse"
    warehouse.mkdir()
    catalog = load_catalog(
        "test",
        type="sql",
        uri=f"sqlite:///{warehouse}/catalog.db",
        warehouse=f"file://{warehouse}",
    )
    catalog.create_namespace_if_not_exists("sigil_test")
    table = catalog.create_table_if_not_exists(
        identifier="sigil_test.session_logs",
        schema=SessionRow.iceberg_schema(),
        partition_spec=SessionRow.partition_spec(),
    )
    return table


def _append_via_daft(table, rows: List[SessionRow]) -> None:
    """Append rows to an Iceberg table using daft (mirrors IcebergStorage._flush)."""
    records = [row.to_iceberg_dict() for row in rows]
    arrow_schema = table.scan().to_arrow_batch_reader().schema
    arrow_table = pa.Table.from_pylist(records, schema=arrow_schema)
    df = daft.from_arrow(arrow_table)
    df.write_iceberg(table, mode="append")


class TestIcebergAppend:
    def test_append(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            table = _create_test_table(tmpdir)
            _append_via_daft(table, [_make_row(), _make_row(session_id="sess-2")])

            assert table.scan().to_arrow().num_rows == 2

    def test_append_multiple_batches(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            table = _create_test_table(tmpdir)
            _append_via_daft(table, [_make_row(session_id="s1")])
            _append_via_daft(table, [_make_row(session_id="s2")])

            assert table.scan().to_arrow().num_rows == 2

    def test_schema_generated_from_model(self):
        schema = SessionRow.iceberg_schema()
        field_names = [f.name for f in schema.fields]
        assert "row_id" in field_names
        assert "session_id" in field_names
        assert "timestamp" in field_names
        assert "extras" in field_names
        assert "input_tokens" in field_names

    def test_partition_spec(self):
        spec = SessionRow.partition_spec()
        assert len(spec.fields) == 1
        assert spec.fields[0].name == "timestamp_day"

    def test_table_uses_generated_schema(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            table = _create_test_table(tmpdir)
            table_names = {f.name for f in table.schema().fields}
            model_names = {f.name for f in SessionRow.iceberg_schema().fields}
            assert table_names == model_names
