"""Pydantic models for sigil.

``SessionRow`` is the canonical row schema for the Iceberg table. Every field
from Claude Code and Codex logs maps into a named column where possible.
Anything that doesn't fit goes into ``extras`` (serialized as a JSON string
in Iceberg).
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import orjson
from pydantic import BaseModel, Field
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    IntegerType,
    NestedField,
    StringType,
    TimestamptzType,
)

# Pydantic type annotation -> Iceberg type
_PYDANTIC_TO_ICEBERG = {
    str: StringType(),
    int: IntegerType(),
    datetime: TimestamptzType(),
}

# Pydantic type annotation -> ClickHouse type
_PYDANTIC_TO_CLICKHOUSE = {
    str: "String",
    int: "Int64",
    datetime: "DateTime64(3, 'UTC')",
}


def _unwrap_optional(annotation: Any) -> Tuple[Any, bool]:
    """Unwrap ``Optional[X]`` to ``(X, True)``, or return ``(annotation, False)``."""
    args = getattr(annotation, "__args__", ())
    if args and type(None) in args:
        inner = [a for a in args if a is not type(None)][0]
        return inner, True
    origin = getattr(annotation, "__origin__", None)
    if origin is type(None):  # noqa: E721
        return annotation, True
    return annotation, False


class SessionRow(BaseModel):
    """One row per JSONL entry from a session log file.

    This model serves as both the application-level data structure and the
    source of truth for the Iceberg table schema (via ``iceberg_schema()``).

    Attributes:
        row_id: Deterministic xxhash of source file path + line number.
        session_id: Session UUID/ULID from the source system.
        session_system: Source system identifier (``claude_code`` or ``codex``).
        device: Machine hostname that produced this row.
        pushed_at: Timestamp of when this row was pushed to sigil.
        timestamp: Entry timestamp from the source log.
        entry_type: Entry type (``user``, ``assistant``, ``tool_result``, etc.).
        message_role: Message role (``user`` or ``assistant``).
        message_text: Text content of the message.
        content_type: Content block type (``text``, ``tool_use``, ``thinking``, etc.).
        tool_name: Tool name if this entry is a tool_use.
        tool_input: Stringified JSON of tool input parameters.
        tool_result_text: Stringified tool result content.
        input_tokens: Number of input tokens consumed.
        output_tokens: Number of output tokens produced.
        cache_creation_tokens: Tokens used for cache creation.
        cache_read_tokens: Tokens read from cache.
        model: Model identifier string.
        model_provider: Provider name (``anthropic`` or ``openai``).
        cwd: Working directory at time of entry.
        git_branch: Git branch at time of entry.
        cli_version: CLI version string.
        parent_uuid: Parent message UUID for conversation threading.
        request_id: API request ID.
        stop_reason: Reason the model stopped generating.
        source_file: Path to the original JSONL file.
        source_line: Line number in the source file (1-indexed).
        extras: Overflow dict for keys that don't map to a named column.
    """

    # Identity
    row_id: str = Field(description="Deterministic hash of source file path + line number")
    session_id: str = Field(description="Session UUID/ULID from the source system")
    session_system: str = Field(description="Source system: claude_code or codex")
    device: str = Field(description="Machine hostname")
    pushed_at: datetime = Field(description="When this row was pushed to sigil")

    # Timing
    timestamp: datetime = Field(description="Entry timestamp from the source log")

    # Message
    entry_type: str = Field(
        description="Entry type: user, assistant, tool_result, event, session_meta, etc."
    )
    message_role: Optional[str] = Field(default=None, description="Message role: user or assistant")
    message_text: Optional[str] = Field(default=None, description="Text content")
    content_type: Optional[str] = Field(
        default=None,
        description="Content block type: text, tool_use, thinking, reasoning, etc.",
    )

    # Tool usage
    tool_name: Optional[str] = Field(
        default=None, description="Tool name if this entry is a tool_use"
    )
    tool_input: Optional[str] = Field(default=None, description="Stringified tool input JSON")
    tool_result_text: Optional[str] = Field(
        default=None, description="Stringified tool result content"
    )

    # Tokens
    input_tokens: Optional[int] = Field(default=None)
    output_tokens: Optional[int] = Field(default=None)
    cache_creation_tokens: Optional[int] = Field(default=None)
    cache_read_tokens: Optional[int] = Field(default=None)

    # Model
    model: Optional[str] = Field(default=None, description="Model identifier")
    model_provider: Optional[str] = Field(default=None, description="Provider: anthropic or openai")

    # Context
    cwd: Optional[str] = Field(default=None, description="Working directory")
    git_branch: Optional[str] = Field(default=None)
    cli_version: Optional[str] = Field(default=None, description="CLI version string")

    # Session-level metadata
    parent_uuid: Optional[str] = Field(
        default=None, description="Parent message UUID for threading"
    )
    request_id: Optional[str] = Field(default=None, description="API request ID")
    stop_reason: Optional[str] = Field(default=None)

    # Source file info
    source_file: str = Field(description="Path to the original JSONL file")
    source_line: int = Field(description="Line number in the source file (1-indexed)")

    # Overflow — everything else (stored as JSON string in Iceberg)
    extras: Dict[str, Any] = Field(
        default_factory=dict,
        description="All keys from the raw entry that don't map to a named column",
    )

    @classmethod
    def iceberg_schema(cls) -> Schema:
        """Generate an Iceberg ``Schema`` from this model's field definitions.

        Iterates over all Pydantic model fields, unwraps ``Optional`` types,
        and maps Python types to Iceberg types using ``_PYDANTIC_TO_ICEBERG``.
        Complex types (``Dict``, ``Any``) fall back to ``StringType``.

        Returns:
            A ``pyiceberg.schema.Schema`` with one ``NestedField`` per model field.
        """
        fields: List[NestedField] = []
        for field_id, (name, info) in enumerate(cls.model_fields.items(), start=1):
            annotation, is_optional = _unwrap_optional(info.annotation)
            iceberg_type = _PYDANTIC_TO_ICEBERG.get(annotation, StringType())

            fields.append(
                NestedField(
                    field_id=field_id,
                    name=name,
                    field_type=iceberg_type,
                    required=not is_optional,
                )
            )
        return Schema(*fields)

    @classmethod
    def partition_spec(cls, timestamp_field: str = "timestamp") -> PartitionSpec:
        """Generate a day-based partition spec on a timestamp field.

        Args:
            timestamp_field: Name of the field to partition by.

        Returns:
            A ``PartitionSpec`` with a single ``DayTransform`` field.
        """
        schema = cls.iceberg_schema()
        source_field = schema.find_field(timestamp_field)
        return PartitionSpec(
            PartitionField(
                source_id=source_field.field_id,
                field_id=1000,
                transform=DayTransform(),
                name=f"{timestamp_field}_day",
            )
        )

    def to_storage_dict(self) -> Dict[str, Any]:
        """Convert this row to a dict suitable for storage ingestion.

        The ``extras`` field is serialized from a Python dict to a JSON string,
        since storage backends store it as a string column.

        Returns:
            A flat dict with all fields, ``extras`` as a JSON string.
        """
        d = self.model_dump()
        d["extras"] = orjson.dumps(d["extras"]).decode()
        return d

    @classmethod
    def iceberg_table_id(cls) -> Tuple[str, str]:
        """Return the default Iceberg table identifier.

        Returns:
            A ``(namespace, table_name)`` tuple.
        """
        return ("sigil", "session_logs")

    @classmethod
    def clickhouse_columns(cls) -> List[Tuple[str, str]]:
        """Return ``(column_name, clickhouse_type)`` pairs for all fields.

        Derives column types from model field annotations using
        ``_PYDANTIC_TO_CLICKHOUSE``. ``Optional`` fields are wrapped in
        ``Nullable()``. Complex types fall back to ``String``.

        Returns:
            A list of ``(name, type_string)`` tuples.
        """
        columns: List[Tuple[str, str]] = []
        for name, info in cls.model_fields.items():
            annotation, is_optional = _unwrap_optional(info.annotation)
            ch_type = _PYDANTIC_TO_CLICKHOUSE.get(annotation, "String")
            if is_optional:
                ch_type = f"Nullable({ch_type})"
            columns.append((name, ch_type))
        return columns

    @classmethod
    def clickhouse_ddl(cls, table: str) -> str:
        """Generate a ClickHouse CREATE TABLE statement.

        Uses ``MergeTree()`` engine ordered by
        ``(session_system, timestamp, row_id)`` and partitioned by month.

        Args:
            table: Fully qualified table name (e.g. ``sigil.session_logs``).

        Returns:
            A complete ``CREATE TABLE IF NOT EXISTS`` DDL string.
        """
        col_defs = ", ".join(f"{name} {typ}" for name, typ in cls.clickhouse_columns())
        return (
            f"CREATE TABLE IF NOT EXISTS {table} "
            f"({col_defs}) "
            f"ENGINE = MergeTree() "
            f"ORDER BY (session_system, timestamp, row_id) "
            f"PARTITION BY toYYYYMM(timestamp)"
        )
