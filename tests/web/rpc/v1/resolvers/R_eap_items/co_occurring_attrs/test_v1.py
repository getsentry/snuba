"""Query shape of the v1 co-occurring-attributes source.

Pure per-storage decisions, no ClickHouse. End-to-end coverage is in
tests/web/rpc/v1/test_endpoint_trace_item_attribute_names.py.
"""

import pytest
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.query.expressions import FunctionCall
from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs.v1 import (
    CO_OCCURRING_ATTRS_STORAGE_KEY,
    V1,
)

ALL_SCALAR_ARRAYS = [
    ("attributes_string", "TYPE_STRING"),
    ("attributes_float", "TYPE_DOUBLE"),
    ("attributes_bool", "TYPE_BOOLEAN"),
]


def test_reads_the_v1_storage() -> None:
    assert V1.storage_key == CO_OCCURRING_ATTRS_STORAGE_KEY


def test_data_source_points_at_the_storage() -> None:
    assert V1.data_source.key == CO_OCCURRING_ATTRS_STORAGE_KEY


def test_count_counts_rows() -> None:
    """Rows are distinct attribute-key sets, so frequency is a row count."""
    expression = V1.count_expression()
    assert isinstance(expression, FunctionCall)
    assert expression.function_name == "count"
    assert expression.parameters == ()


@pytest.mark.parametrize(
    ("attr_type", "expected"),
    [
        (AttributeKey.Type.TYPE_STRING, [("attributes_string", "TYPE_STRING")]),
        (AttributeKey.Type.TYPE_BOOLEAN, [("attributes_bool", "TYPE_BOOLEAN")]),
        # No int key array of its own: int keys are visible via the float bucket they are
        # double-written to on eap_items, so they are reported as TYPE_DOUBLE.
        (AttributeKey.Type.TYPE_INT, [("attributes_float", "TYPE_DOUBLE")]),
        (AttributeKey.Type.TYPE_DOUBLE, [("attributes_float", "TYPE_DOUBLE")]),
        # TYPE_FLOAT is a backwards-compatible alias: same column, requested type name.
        (AttributeKey.Type.TYPE_FLOAT, [("attributes_float", "TYPE_FLOAT")]),
    ],
)
def test_scalar_types_read_one_array(
    attr_type: AttributeKey.Type.ValueType, expected: list[tuple[str, str]]
) -> None:
    assert list(V1.typed_key_arrays(attr_type)) == expected


def test_unspecified_reads_every_array() -> None:
    assert list(V1.typed_key_arrays(AttributeKey.Type.TYPE_UNSPECIFIED)) == ALL_SCALAR_ARRAYS


@pytest.mark.parametrize(
    "attr_type",
    [
        AttributeKey.Type.TYPE_ARRAY,
        AttributeKey.Type.TYPE_ARRAY_STRING,
        AttributeKey.Type.TYPE_ARRAY_INT,
        AttributeKey.Type.TYPE_ARRAY_DOUBLE,
        AttributeKey.Type.TYPE_ARRAY_BOOL,
    ],
)
def test_array_types_fall_back_to_every_scalar_array(
    attr_type: AttributeKey.Type.ValueType,
) -> None:
    """No array-typed keys here, so these degrade to reading all three scalar arrays."""
    assert list(V1.typed_key_arrays(attr_type)) == ALL_SCALAR_ARRAYS


def test_key_array_columns_matches_typed_key_arrays() -> None:
    for attr_type in (
        AttributeKey.Type.TYPE_STRING,
        AttributeKey.Type.TYPE_INT,
        AttributeKey.Type.TYPE_UNSPECIFIED,
    ):
        assert V1.key_array_columns(attr_type) == [col for col, _ in V1.typed_key_arrays(attr_type)]


def test_does_not_record_last_seen() -> None:
    """No last_seen column, so the endpoint must not select or order by it."""
    assert not V1.has_last_seen


def test_last_seen_expression_raises() -> None:
    """Guarding on has_last_seen is the contract; calling anyway is a programming error."""
    with pytest.raises(NotImplementedError, match="does not record last_seen"):
        V1.last_seen_expression()
