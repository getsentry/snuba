from copy import deepcopy

import pytest
from snuba.clickhouse.columns import (
    UUID,
    AggregateFunction,
    Array,
    ColumnType,
    Date,
    DateTime,
    Enum,
    FixedString,
    Float,
    IPv4,
    IPv6,
    Nested,
    ReadOnly,
)
from snuba.clickhouse.columns import SchemaModifiers as Modifier
from snuba.clickhouse.columns import String, UInt

TEST_CASES = [
    pytest.param(
        String(Modifier(nullable=True)),
        String(),
        String(),
        "Nullable(String)",
        id="strings",
    ),
    pytest.param(
        UUID(Modifier(readonly=True)),
        UUID(),
        UUID(Modifier(nullable=True)),
        "UUID",
        id="UUIDs",
    ),
    pytest.param(IPv4(None), IPv4(), IPv4(Modifier(nullable=True)), "IPv4", id="IPs",),
    pytest.param(IPv6(None), IPv6(), IPv6(Modifier(nullable=True)), "IPv6", id="IPs",),
    pytest.param(
        FixedString(32, Modifier(nullable=True)),
        FixedString(32),
        FixedString(64, Modifier(nullable=True)),
        "Nullable(FixedString(32))",
        id="fixed strings",
    ),
    pytest.param(
        UInt(8, Modifier(nullable=True)),
        UInt(8),
        UInt(16, Modifier(nullable=True)),
        "Nullable(UInt8)",
        id="integers",
    ),
    pytest.param(
        Float(64, Modifier(nullable=True)),
        Float(64),
        Float(32, Modifier(nullable=True)),
        "Nullable(Float64)",
        id="floats",
    ),
    pytest.param(Date(), Date(), Date(Modifier(nullable=True)), "Date", id="dates",),
    pytest.param(
        DateTime(),
        DateTime(),
        DateTime(Modifier(nullable=True)),
        "DateTime",
        id="datetimes",
    ),
    pytest.param(
        Array(String(Modifier(nullable=True))),
        Array(String()),
        Array(String()),
        "Array(Nullable(String))",
        id="arrays",
    ),
    pytest.param(
        Nested(
            [("key", String()), ("val", String(Modifier(nullable=True)))],
            Modifier(nullable=True),
        ),
        Nested([("key", String()), ("val", String())]),
        Nested([("key", String()), ("val", String())], Modifier(nullable=True)),
        "Nullable(Nested(key String, val Nullable(String)))",
        id="nested",
    ),
    pytest.param(
        AggregateFunction("uniqIf", [UInt(8), UInt(32)], Modifier(nullable=True)),
        AggregateFunction("uniqIf", [UInt(8), UInt(32)]),
        AggregateFunction("uniqIf", [UInt(8)], Modifier(nullable=True)),
        "Nullable(AggregateFunction(uniqIf, UInt8, UInt32))",
        id="aggregated",
    ),
    pytest.param(
        Enum([("a", 1), ("b", 2)], Modifier(nullable=True)),
        Enum([("a", 1), ("b", 2)]),
        Enum([("a", 1), ("b", 2)]),
        "Nullable(Enum('a' = 1, 'b' = 2))",
        id="enums",
    ),
]


@pytest.mark.parametrize("col_type, raw_type, different_type, for_schema", TEST_CASES)
def test_methods(
    col_type: ColumnType,
    raw_type: ColumnType,
    different_type: ColumnType,
    for_schema: str,
) -> None:
    assert col_type == deepcopy(col_type)
    assert col_type != different_type
    # Test it is not equal to a type of different class.
    assert col_type != ColumnType(Modifier(readonly=True))
    assert col_type.for_schema() == for_schema

    assert col_type.get_raw() == raw_type

    modified = col_type.set_modifiers(col_type.get_modifiers())
    assert modified is not col_type
    assert modified == col_type

    assert col_type.set_modifiers(Modifier(readonly=True)).has_modifier(ReadOnly)
