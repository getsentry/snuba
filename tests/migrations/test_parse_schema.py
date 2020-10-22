import pytest

from snuba.clickhouse.columns import (
    AggregateFunction,
    Array,
    Date,
    DateTime,
    Enum,
    FixedString,
    Float,
    IPv4,
    IPv6,
    Nullable,
    String,
    UInt,
    UUID,
)
from snuba.migrations.columns import (
    LowCardinality,
    Materialized,
    WithCodecs,
    WithDefault,
)
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
    (("UUID", "", "", ""), UUID()),
    # Aggregate functions
    (
        ("AggregateFunction(uniq, UInt8)", "", "", ""),
        AggregateFunction("uniq", UInt(8)),
    ),
    (
        ("AggregateFunction(countIf, UUID, UInt8)", "", "", ""),
        AggregateFunction("countIf", UUID(), UInt(8)),
    ),
    (
        ("AggregateFunction(quantileIf(0.5, 0.9), UInt32, UInt8)", "", "", ""),
        AggregateFunction("quantileIf(0.5, 0.9)", UInt(32), UInt(8)),
    ),
    # Array
    (("Array(String)", "", "", ""), Array(String())),
    (("Array(DateTime)", "", "", ""), Array(DateTime())),
    (("Array(UInt64)", "", "", ""), Array(UInt(64))),
    (("Array(Nullable(UUID))", "", "", ""), Array(UUID([Nullable()]))),
    (("Array(Array(Nullable(UUID)))", "", "", ""), Array(Array(UUID([Nullable()])))),
    # Nullable
    (("Nullable(String)", "", "", ""), String([Nullable()])),
    (("Nullable(FixedString(8))", "", "", ""), FixedString(8, [Nullable()])),
    (("Nullable(Date)", "", "", ""), Date([Nullable()])),
    # Low cardinality
    (("LowCardinality(String)", "", "", ""), String([LowCardinality()])),
    (
        ("LowCardinality(Nullable(String))", "", "", ""),
        String([Nullable(), LowCardinality()]),
    ),
    # Materialized
    (
        ("Date", "MATERIALIZED", "toDate(col1)", ""),
        (Date([Materialized("toDate(col1)")])),
    ),
    (
        ("UInt64", "MATERIALIZED", "CAST(cityHash64(col1), 'UInt64')", ""),
        (UInt(64, [Materialized("cityHash64(col1)")])),
    ),
    # Default value
    (
        ("LowCardinality(String)", "DEFAULT", "a", ""),
        (String([LowCardinality(), WithDefault("a")])),
    ),
    (("UInt8", "DEFAULT", "2", ""), (UInt(8, [WithDefault("2")]))),
    # With codecs
    (("UUID", "", "", "NONE"), (UUID([WithCodecs(["NONE"])]))),
    (
        ("DateTime", "", "", "DoubleDelta, LZ4"),
        (DateTime([WithCodecs(["DoubleDelta", "LZ4"])])),
    ),
]


@pytest.mark.parametrize("input, expected_output", test_data)
def test_parse_column(input, expected_output):
    (input_name, input_type, default_expr, codec_expr) = input
    assert (
        _get_column(input_name, input_type, default_expr, codec_expr) == expected_output
    )
