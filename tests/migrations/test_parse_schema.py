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
    (("Array(Nullable(UUID))", "", "", ""), Array(Nullable(UUID()))),
    (("Array(Array(Nullable(UUID)))", "", "", ""), Array(Array(Nullable(UUID())))),
    # Nullable
    (("Nullable(String)", "", "", ""), Nullable(String())),
    (("Nullable(FixedString(8))", "", "", ""), Nullable(FixedString(8))),
    (("Nullable(Date)", "", "", ""), Nullable(Date())),
    # Low cardinality
    (("LowCardinality(String)", "", "", ""), LowCardinality(String())),
    (
        ("LowCardinality(Nullable(String))", "", "", ""),
        LowCardinality(Nullable(String())),
    ),
    # Materialized
    (
        ("Date", "MATERIALIZED", "toDate(col1)", ""),
        Materialized(Date(), "toDate(col1)"),
    ),
    (
        ("UInt64", "MATERIALIZED", "CAST(cityHash64(col1), 'UInt64')", ""),
        Materialized(UInt(64), "cityHash64(col1)"),
    ),
    # Default value
    (
        ("LowCardinality(String)", "DEFAULT", "a", ""),
        WithDefault(LowCardinality(String()), "a"),
    ),
    (("UInt8", "DEFAULT", "2", ""), WithDefault(UInt(8), "2")),
    # With codecs
    (("UUID", "", "", "NONE"), WithCodecs(UUID(), ["NONE"])),
    (
        ("DateTime", "", "", "DoubleDelta, LZ4"),
        WithCodecs(DateTime(), ["DoubleDelta", "LZ4"]),
    ),
]


@pytest.mark.parametrize("input, expected_output", test_data)
def test_parse_column(input, expected_output):
    (input_name, input_type, default_expr, codec_expr) = input
    assert (
        _get_column(input_name, input_type, default_expr, codec_expr) == expected_output
    )
