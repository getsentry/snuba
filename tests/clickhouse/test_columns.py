from copy import deepcopy
from typing import cast

import pytest

from snuba.clickhouse.columns import (
    UUID,
    AggregateFunction,
    Array,
    Bool,
    Column,
    ColumnType,
    Date,
    DateTime,
    DateTime64,
    Enum,
    FixedString,
    Float,
    IPv4,
    IPv6,
    Map,
    Nested,
    ReadOnly,
)
from snuba.clickhouse.columns import SchemaModifiers as Modifier
from snuba.clickhouse.columns import SimpleAggregateFunction, String, UInt
from snuba.utils.schemas import JSON

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
    pytest.param(
        IPv4(None),
        IPv4(),
        IPv4(Modifier(nullable=True)),
        "IPv4",
        id="IPs",
    ),
    pytest.param(
        IPv6(None),
        IPv6(),
        IPv6(Modifier(nullable=True)),
        "IPv6",
        id="IPs",
    ),
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
    pytest.param(
        Date(),
        Date(),
        Date(Modifier(nullable=True)),
        "Date",
        id="dates",
    ),
    pytest.param(
        DateTime(),
        DateTime(),
        DateTime(Modifier(nullable=True)),
        "DateTime",
        id="datetimes",
    ),
    pytest.param(
        DateTime64(3, "America/New_York"),
        DateTime64(3, "America/New_York"),
        DateTime64(9, modifiers=Modifier(nullable=True)),
        "DateTime64(3, 'America/New_York')",
        id="datetime64s_tz",
    ),
    pytest.param(
        DateTime64(3),
        DateTime64(3),
        DateTime64(9, modifiers=Modifier(nullable=True)),
        "DateTime64(3)",
        id="datetime64s_notz",
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
        cast(
            Column[Modifier],
            Nested([("key", String()), ("val", String())], Modifier(nullable=True)),
        ),
        "Nullable(Nested(key String, val Nullable(String)))",
        id="nested",
    ),
    pytest.param(
        Map(
            key=String(),
            value=Array(String()),
        ),
        Map(
            key=String(),
            value=Array(String()),
        ),
        cast(
            Column[Modifier],
            Map(key=String(), value=UInt(64)),
        ),
        "Map(String, Array(String))",
        id="map",
    ),
    pytest.param(
        cast(
            Column[Modifier],
            AggregateFunction("uniqIf", [UInt(8), UInt(32)], Modifier(nullable=True)),
        ),
        AggregateFunction("uniqIf", [UInt(8), UInt(32)]),
        cast(
            Column[Modifier],
            AggregateFunction("uniqIf", [UInt(8)], Modifier(nullable=True)),
        ),
        "Nullable(AggregateFunction(uniqIf, UInt8, UInt32))",
        id="aggregated",
    ),
    pytest.param(
        cast(
            Column[Modifier],
            SimpleAggregateFunction("sum", [UInt(8), UInt(32)], Modifier(nullable=True)),
        ),
        SimpleAggregateFunction("sum", [UInt(8), UInt(32)]),
        cast(
            Column[Modifier],
            SimpleAggregateFunction("sum", [UInt(8), UInt(8)], Modifier(nullable=True)),
        ),
        "Nullable(SimpleAggregateFunction(sum, UInt8, UInt32))",
        id="simple-aggregated",
    ),
    pytest.param(
        Enum([("a", 1), ("b", 2)], Modifier(nullable=True)),
        Enum([("a", 1), ("b", 2)]),
        Enum([("a", 1), ("b", 2)]),
        "Nullable(Enum('a' = 1, 'b' = 2))",
        id="enums",
    ),
    pytest.param(
        Bool(Modifier(nullable=True)),
        Bool(),
        Bool(),
        "Nullable(Bool)",
        id="bools",
    ),
    pytest.param(
        JSON[Modifier](
            max_dynamic_paths=10,
            max_dynamic_types=10,
            type_hints={"a.b": String()},
            skip_paths=["a.c"],
            skip_regexp=["b.*"],
            modifiers=Modifier(nullable=True),
        ),
        JSON(
            max_dynamic_paths=10,
            max_dynamic_types=10,
            type_hints={"a.b": String()},
            skip_paths=["a.c"],
            skip_regexp=["b.*"],
        ),
        JSON(
            max_dynamic_paths=10,
            max_dynamic_types=10,
            type_hints={"a.b": String()},
            skip_paths=["a.c"],
            skip_regexp=["b.*", "c.*"],
        ),
        "Nullable(JSON(max_dynamic_paths=10, max_dynamic_types=10, a.b String, SKIP a.c, SKIP REGEXP 'b.*', SKIP REGEXP 'c.*'))",
    ),
]


@pytest.mark.parametrize("col_type, raw_type, different_type, for_schema", TEST_CASES)
def test_methods(
    col_type: ColumnType[Modifier],
    raw_type: ColumnType[Modifier],
    different_type: ColumnType[Modifier],
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
