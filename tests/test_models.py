"""Tests for SessionRow schema generation."""

from pyiceberg.types import IntegerType, StringType, TimestamptzType

from sigil.models import SessionRow


class TestIcebergSchemaGeneration:
    def test_all_model_fields_present(self):
        schema = SessionRow.iceberg_schema()
        schema_names = {f.name for f in schema.fields}
        model_names = set(SessionRow.model_fields.keys())
        assert schema_names == model_names

    def test_str_fields_map_to_string_type(self):
        schema = SessionRow.iceberg_schema()
        row_id_field = schema.find_field("row_id")
        assert isinstance(row_id_field.field_type, StringType)

    def test_int_fields_map_to_integer_type(self):
        schema = SessionRow.iceberg_schema()
        source_line = schema.find_field("source_line")
        assert isinstance(source_line.field_type, IntegerType)

    def test_datetime_fields_map_to_timestamptz(self):
        schema = SessionRow.iceberg_schema()
        pushed_at = schema.find_field("pushed_at")
        assert isinstance(pushed_at.field_type, TimestamptzType)

    def test_optional_fields_are_not_required(self):
        schema = SessionRow.iceberg_schema()
        model = schema.find_field("model")
        assert not model.required

    def test_timestamp_is_required(self):
        schema = SessionRow.iceberg_schema()
        timestamp = schema.find_field("timestamp")
        assert timestamp.required

    def test_required_fields_are_required(self):
        schema = SessionRow.iceberg_schema()
        row_id = schema.find_field("row_id")
        assert row_id.required

        entry_type = schema.find_field("entry_type")
        assert entry_type.required

    def test_extras_is_string_type(self):
        """extras is Dict[str, Any] in Pydantic but serialized as JSON string in Iceberg."""
        schema = SessionRow.iceberg_schema()
        extras = schema.find_field("extras")
        assert isinstance(extras.field_type, StringType)
        assert extras.required

    def test_partition_spec_targets_timestamp(self):
        spec = SessionRow.partition_spec()
        assert len(spec.fields) == 1
        pf = spec.fields[0]
        assert pf.name == "timestamp_day"

        # Verify source_id matches the timestamp field_id in the schema
        schema = SessionRow.iceberg_schema()
        ts_field = schema.find_field("timestamp")
        assert pf.source_id == ts_field.field_id

    def test_to_storage_dict_serializes_extras(self):
        row = SessionRow(
            row_id="r1",
            session_id="s1",
            session_system="claude_code",
            device="mac",
            pushed_at="2026-03-20T12:00:00",
            timestamp="2026-03-20T12:00:00",
            entry_type="user",
            source_file="/tmp/test.jsonl",
            source_line=1,
            extras={"key": "value", "nested": {"a": 1}},
        )
        d = row.to_storage_dict()
        assert isinstance(d["extras"], str)
        assert '"key"' in d["extras"]
        assert '"nested"' in d["extras"]
