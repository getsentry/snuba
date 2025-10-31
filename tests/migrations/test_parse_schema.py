from typing import Tuple

import pytest

from snuba.clickhouse.columns import (
    JSON,
    UUID,
    AggregateFunction,
    Array,
    Bool,
    ColumnType,
    Date,
    DateTime,
    DateTime64,
    Enum,
    FixedString,
    Float,
    IPv4,
    IPv6,
    String,
)
from snuba.clickhouse.columns import Tuple as TupleCol
from snuba.clickhouse.columns import UInt
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.parse_schema import _get_column

test_data = [
    # Basic types
    (("Date", "", "", ""), Date()),
    (("DateTime", "", "", ""), DateTime()),
    (
        ("Enum8('success' = 0, 'error' = 1)", "", "", ""),
        Enum([("success", 0), ("error", 1)]),
    ),
    (("FixedString(32)", "", "", ""), FixedString(32)),
    (("Float32", "", "", ""), Float(32)),
    (("IPv4", "", "", ""), IPv4()),
    (("IPv6", "", "", ""), IPv6()),
    (("String", "", "", ""), String()),
    (("UInt32", "", "", ""), UInt(32)),
    (("UInt128", "", "", ""), UInt(128)),
    (("UUID", "", "", ""), UUID()),
    (("Bool", "", "", ""), Bool()),
    # Aggregate functions
    (
        ("AggregateFunction(uniq, UInt8)", "", "", ""),
        AggregateFunction("uniq", [UInt(8)]),
    ),
    (
        ("AggregateFunction(countIf, UUID, UInt8)", "", "", ""),
        AggregateFunction("countIf", [UUID(), UInt(8)]),
    ),
    (
        ("AggregateFunction(quantileIf(0.5, 0.9), UInt32, UInt8)", "", "", ""),
        AggregateFunction("quantileIf(0.5, 0.9)", [UInt(32), UInt(8)]),
    ),
    # Array
    (("Array(String)", "", "", ""), Array(String())),
    (("Array(DateTime)", "", "", ""), Array(DateTime())),
    (("Array(UInt64)", "", "", ""), Array(UInt(64))),
    (("Array(Nullable(UUID))", "", "", ""), Array(UUID(Modifiers(nullable=True)))),
    (
        ("Array(Array(Nullable(UUID)))", "", "", ""),
        Array(Array(UUID(Modifiers(nullable=True)))),
    ),
    # Nullable
    (("Nullable(String)", "", "", ""), String(Modifiers(nullable=True))),
    (
        ("Nullable(FixedString(8))", "", "", ""),
        FixedString(8, Modifiers(nullable=True)),
    ),
    (("Nullable(Date)", "", "", ""), Date(Modifiers(nullable=True))),
    # Low cardinality
    (("LowCardinality(String)", "", "", ""), String(Modifiers(low_cardinality=True))),
    (
        ("LowCardinality(Nullable(String))", "", "", ""),
        String(Modifiers(nullable=True, low_cardinality=True)),
    ),
    # Materialized
    (
        ("Date", "MATERIALIZED", "toDate(col1)", ""),
        (Date(Modifiers(materialized="toDate(col1)"))),
    ),
    (
        ("UInt64", "MATERIALIZED", "CAST(cityHash64(col1), 'UInt64')", ""),
        (UInt(64, Modifiers(materialized="cityHash64(col1)"))),
    ),
    # Default value
    (
        ("LowCardinality(String)", "DEFAULT", "a", ""),
        (String(Modifiers(low_cardinality=True, default="a"))),
    ),
    (("UInt8", "DEFAULT", "2", ""), (UInt(8, Modifiers(default="2")))),
    # With codecs
    (("UUID", "", "", "NONE"), (UUID(Modifiers(codecs=["NONE"])))),
    (
        ("DateTime", "", "", "DoubleDelta, LZ4"),
        (DateTime(Modifiers(codecs=["DoubleDelta", "LZ4"]))),
    ),
    # DateTime64
    (
        ("DateTime64", "", "", ""),
        DateTime64(3),
    ),
    (
        ("DateTime64(6)", "", "", ""),
        DateTime64(6),
    ),
    (
        ("DateTime64(9, 'America/New_York')", "", "", ""),
        DateTime64(9, "America/New_York"),
    ),
    (
        ("Tuple(String, UUID, String, String)", "", "", ""),
        TupleCol(types=((String(), UUID(), String(), String()))),
    ),
    (
        ("Tuple(String)", "", "", ""),
        TupleCol(types=((String(),))),
    ),
    (
        ("Tuple(UInt64, String)", "", "", ""),
        TupleCol(
            types=(
                (
                    UInt(64),
                    String(),
                )
            )
        ),
    ),
    # JSON type
    (("JSON", "", "", ""), JSON()),
    (("JSON('a.b' String)", "", "", ""), JSON(type_hints={"a.b": String()})),
    (
        (
            "JSON(max_dynamic_paths=10, max_dynamic_types=10, 'a.b' String, 'c.d' DateTime, SKIP 'a.c', SKIP REGEXP 'b.*')",
            "",
            "",
            "",
        ),
        JSON(
            max_dynamic_paths=10,
            max_dynamic_types=10,
            type_hints={"a.b": String(), "c.d": DateTime()},
            skip_paths=["a.c"],
            skip_regexp=["b.*"],
        ),
    ),
]


@pytest.mark.parametrize("input, expected_output", test_data)
def test_parse_column(
    input: Tuple[str, str, str, str],
    expected_output: Tuple[Tuple[str, str, str, str, str], ColumnType[Modifiers]],
) -> None:
    (input_name, input_type, default_expr, codec_expr) = input
    assert _get_column(input_name, input_type, default_expr, codec_expr) == expected_output
