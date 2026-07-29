"""Query shape of the v2 co-occurring-attributes source.

No ClickHouse needed: these cover the pure per-storage decisions. End-to-end behaviour
against real data is in tests/web/rpc/v1/test_endpoint_trace_item_attribute_names_v2.py.
"""

import pytest
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.query.expressions import Column, FunctionCall
from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs.v2 import (
    CO_OCCURRING_ATTRS_V2_STORAGE_KEY,
    V2,
)


def test_reads_the_v2_storage() -> None:
    assert V2.storage_key == CO_OCCURRING_ATTRS_V2_STORAGE_KEY


def test_data_source_points_at_the_storage() -> None:
    assert V2.data_source.key == CO_OCCURRING_ATTRS_V2_STORAGE_KEY


def test_count_sums_the_occurrence_column() -> None:
    """Rows carry a `count` the SummingMergeTree accumulates, so it is summed."""
    expression = V2.count_expression()
    assert isinstance(expression, FunctionCall)
    assert expression.function_name == "sum"
    assert [p.column_name for p in expression.parameters if isinstance(p, Column)] == ["count"]


@pytest.mark.parametrize(
    ("attr_type", "expected"),
    [
        (AttributeKey.Type.TYPE_STRING, ("attributes_string", "TYPE_STRING")),
        (AttributeKey.Type.TYPE_BOOLEAN, ("attributes_bool", "TYPE_BOOLEAN")),
        # Unlike v1, this storage has a dedicated int key array, so int keys keep their
        # real type instead of surfacing as TYPE_DOUBLE.
        (AttributeKey.Type.TYPE_INT, ("attributes_int", "TYPE_INT")),
        (AttributeKey.Type.TYPE_DOUBLE, ("attributes_float", "TYPE_DOUBLE")),
        (AttributeKey.Type.TYPE_FLOAT, ("attributes_float", "TYPE_FLOAT")),
        (AttributeKey.Type.TYPE_ARRAY_STRING, ("attributes_array_string", "TYPE_ARRAY_STRING")),
        (AttributeKey.Type.TYPE_ARRAY_INT, ("attributes_array_int", "TYPE_ARRAY_INT")),
        (AttributeKey.Type.TYPE_ARRAY_DOUBLE, ("attributes_array_float", "TYPE_ARRAY_DOUBLE")),
        (AttributeKey.Type.TYPE_ARRAY_BOOL, ("attributes_array_bool", "TYPE_ARRAY_BOOL")),
    ],
)
def test_each_type_reads_its_own_array(
    attr_type: AttributeKey.Type.ValueType, expected: tuple[str, str]
) -> None:
    assert list(V2.typed_key_arrays(attr_type)) == [expected]


def test_untyped_array_surfaces_all_four_element_types() -> None:
    """The deprecated untyped TYPE_ARRAY has no element type, so it reads all four."""
    assert list(V2.typed_key_arrays(AttributeKey.Type.TYPE_ARRAY)) == [
        ("attributes_array_string", "TYPE_ARRAY_STRING"),
        ("attributes_array_int", "TYPE_ARRAY_INT"),
        ("attributes_array_float", "TYPE_ARRAY_DOUBLE"),
        ("attributes_array_bool", "TYPE_ARRAY_BOOL"),
    ]


def test_unspecified_covers_every_type_without_duplicating_ints() -> None:
    """attributes_int is left out of TYPE_UNSPECIFIED on purpose: int keys are
    double-written to a float bucket, so including both arrays would emit them twice."""
    columns = [col for col, _ in V2.typed_key_arrays(AttributeKey.Type.TYPE_UNSPECIFIED)]
    assert "attributes_int" not in columns
    assert columns == [
        "attributes_string",
        "attributes_float",
        "attributes_bool",
        "attributes_array_string",
        "attributes_array_int",
        "attributes_array_float",
        "attributes_array_bool",
    ]
    # no column is read twice, so no key can be emitted twice
    assert len(set(columns)) == len(columns)


def test_key_array_columns_matches_typed_key_arrays() -> None:
    for attr_type in (
        AttributeKey.Type.TYPE_STRING,
        AttributeKey.Type.TYPE_INT,
        AttributeKey.Type.TYPE_ARRAY_INT,
        AttributeKey.Type.TYPE_UNSPECIFIED,
    ):
        assert V2.key_array_columns(attr_type) == [col for col, _ in V2.typed_key_arrays(attr_type)]
