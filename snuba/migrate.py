"""
Simple schema migration tool. Only intended for local development environment.
"""

import logging
import re

from snuba.clickhouse.columns import (
    Array,
    ColumnType,
    Date,
    DateTime,
    Enum,
    FixedString,
    Float,
    IPv4,
    IPv6,
    LowCardinality,
    Materialized,
    Nullable,
    String,
    UInt,
    UUID,
    WithCodecs,
    WithDefault,
)
from snuba.datasets.schemas.tables import TableSchema

logger = logging.getLogger("snuba.migrate")


def _run_schema(conn, schema):
    if not isinstance(schema, TableSchema):
        return
    clickhouse_table = schema.get_local_table_name()

    STRIP_CAST_RE = re.compile(r"^CAST\((.*), (.*)\)$", re.IGNORECASE)
    FIXED_STRING_RE = re.compile(r"^FixedString\((\d+)\)$")
    ENUM_RE = re.compile(r"^Enum\d+\((.*)\)$")
    ENUM_VALUES_RE = re.compile(r"'(.+)' = (\d+)$")
    MODIFIER_COL_RE = re.compile(r"^(Array|LowCardinality|Nullable)\((.+)\)$")

    def strip_cast(default_expr: str) -> str:
        match = STRIP_CAST_RE.match(default_expr)
        if match:
            default_expr = match.groups()[0]
        return default_expr

    def get_column_type(column_type: str) -> ColumnType:
        basic_col_types = {
            "Date": Date(),
            "DateTime": DateTime(),
            "Float32": Float(32),
            "Float64": Float(64),
            "IPv4": IPv4(),
            "IPv6": IPv6(),
            "String": String(),
            "UUID": UUID(),
            "UInt8": UInt(8),
            "UInt16": UInt(16),
            "UInt32": UInt(32),
            "UInt64": UInt(64),
        }

        if column_type in basic_col_types:
            return basic_col_types[column_type]

        match = FIXED_STRING_RE.match(column_type)
        if match:
            size = match.groups()[0]
            return FixedString(int(size))

        match = ENUM_RE.match(column_type)
        if match:
            values = []
            for value_str in match.groups()[0].split(", "):
                (name, val) = ENUM_VALUES_RE.match(value_str).groups()
                values.append((name, int(val)))
            return Enum(values)

        modifier_col_types = {
            "Array": Array,
            "LowCardinality": LowCardinality,
            "Nullable": Nullable,
        }

        match = MODIFIER_COL_RE.match(column_type)

        if match:
            (func, inner_type) = match.groups()
            return modifier_col_types[func](get_column_type(inner_type))

        raise ValueError("Could not parse {}".format(column_type))

    def get_column(
        column_type: str, default_type: str, default_expr: str, codec_expr: str
    ) -> ColumnType:
        column = get_column_type(column_type)

        if default_type == "MATERIALIZED":
            column = Materialized(column, strip_cast(default_expr))
        elif default_type == "DEFAULT":
            column = WithDefault(column, strip_cast(default_expr))

        if codec_expr:
            column = WithCodecs(column, codec_expr.split(", "))

        return column

    def get_schema():
        return {
            column_name: get_column(column_type, default_type, default_expr, codec_expr)
            for column_name, column_type, default_type, default_expr, _comment, codec_expr in [
                cols[:6]
                for cols in conn.execute("DESCRIBE TABLE %s" % clickhouse_table)
            ]
        }

    local_schema = get_schema()

    migrations = schema.get_migration_statements()(clickhouse_table, local_schema)
    for statement in migrations:
        logger.info(f"Executing migration: {statement}")
        conn.execute(statement)

    # Refresh after alters
    refreshed_schema = get_schema()

    # Warn user about any *other* schema diffs
    differences = schema.get_column_differences(refreshed_schema)

    for difference in differences:
        logger.warn(difference)


def run(conn, dataset):
    schemas = []
    if dataset.get_table_writer():
        schemas.append(dataset.get_table_writer().get_schema())
    schemas.append(dataset.get_dataset_schemas().get_read_schema())

    for schema in schemas:
        _run_schema(conn, schema)
