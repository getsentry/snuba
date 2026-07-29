"""Unit tests for the co-occurring-attributes source selection.

These need no ClickHouse: they cover the pure decision logic (which storage, which key
arrays, which aggregate) that the endpoint delegates to. End-to-end behaviour against real
data lives in tests/web/rpc/v1/test_endpoint_trace_item_attribute_names{,_v2}.py.
"""

from datetime import UTC, datetime, timedelta

import pytest
from sentry_options.testing import override_options
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeNamesRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.query.expressions import Column, FunctionCall
from snuba.web.rpc.v1.resolvers.R_eap_items import co_occurring_attrs
from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs import (
    CO_OCCURRING_ATTRS_STORAGE_KEY,
    CO_OCCURRING_ATTRS_V2_OPTION,
    CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_DEFAULT,
    CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION,
    CO_OCCURRING_ATTRS_V2_STORAGE_KEY,
    V1,
    V2,
)

V2_START = datetime.fromtimestamp(CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_DEFAULT, UTC)


def _request(
    start: datetime,
    attr_type: AttributeKey.Type.ValueType = AttributeKey.Type.TYPE_STRING,
) -> TraceItemAttributeNamesRequest:
    req = TraceItemAttributeNamesRequest(meta=RequestMeta(project_ids=[1], organization_id=1))
    req.meta.start_timestamp.FromDatetime(start)
    req.meta.end_timestamp.FromDatetime(start + timedelta(hours=1))
    req.type = attr_type
    return req


class TestStorageKeys:
    def test_sources_point_at_their_storages(self) -> None:
        assert V1.storage_key == CO_OCCURRING_ATTRS_STORAGE_KEY
        assert V2.storage_key == CO_OCCURRING_ATTRS_V2_STORAGE_KEY

    def test_only_v2_has_per_type_key_arrays(self) -> None:
        assert not V1.per_type_key_arrays
        assert V2.per_type_key_arrays


class TestCountExpression:
    def test_v1_counts_rows(self) -> None:
        """v1 has one row per distinct attribute set, so frequency is a row count."""
        expression = V1.count_expression()
        assert isinstance(expression, FunctionCall)
        assert expression.function_name == "count"
        assert expression.parameters == ()

    def test_v2_sums_the_occurrence_column(self) -> None:
        """v2 rows carry a `count` the SummingMergeTree accumulates, so it is summed."""
        expression = V2.count_expression()
        assert isinstance(expression, FunctionCall)
        assert expression.function_name == "sum"
        assert [p.column_name for p in expression.parameters if isinstance(p, Column)] == ["count"]


class TestTypedKeyArrays:
    @pytest.mark.parametrize(
        ("attr_type", "expected"),
        [
            (AttributeKey.Type.TYPE_STRING, [("attributes_string", "TYPE_STRING")]),
            (AttributeKey.Type.TYPE_BOOLEAN, [("attributes_bool", "TYPE_BOOLEAN")]),
            # v1 stores no int keys of its own; they are visible via the float bucket they
            # are double-written to, so they come back typed TYPE_DOUBLE.
            (AttributeKey.Type.TYPE_INT, [("attributes_float", "TYPE_DOUBLE")]),
            (AttributeKey.Type.TYPE_DOUBLE, [("attributes_float", "TYPE_DOUBLE")]),
            # TYPE_FLOAT is a backwards-compatible alias: same column, requested type name.
            (AttributeKey.Type.TYPE_FLOAT, [("attributes_float", "TYPE_FLOAT")]),
        ],
    )
    def test_v1_scalar_types(
        self, attr_type: AttributeKey.Type.ValueType, expected: list[tuple[str, str]]
    ) -> None:
        assert list(V1.typed_key_arrays(attr_type)) == expected

    @pytest.mark.parametrize(
        "attr_type",
        [
            AttributeKey.Type.TYPE_ARRAY,
            AttributeKey.Type.TYPE_ARRAY_STRING,
            AttributeKey.Type.TYPE_ARRAY_INT,
        ],
    )
    def test_v1_array_types_fall_back_to_every_scalar_array(
        self, attr_type: AttributeKey.Type.ValueType
    ) -> None:
        """v1 stores no array-typed keys, so these degrade to the historical behaviour of
        reading all three scalar arrays rather than returning nothing."""
        assert list(V1.typed_key_arrays(attr_type)) == [
            ("attributes_string", "TYPE_STRING"),
            ("attributes_float", "TYPE_DOUBLE"),
            ("attributes_bool", "TYPE_BOOLEAN"),
        ]

    @pytest.mark.parametrize(
        ("attr_type", "expected"),
        [
            (AttributeKey.Type.TYPE_STRING, [("attributes_string", "TYPE_STRING")]),
            (AttributeKey.Type.TYPE_BOOLEAN, [("attributes_bool", "TYPE_BOOLEAN")]),
            # v2 has a dedicated int key array, so int keys keep their real type.
            (AttributeKey.Type.TYPE_INT, [("attributes_int", "TYPE_INT")]),
            (AttributeKey.Type.TYPE_DOUBLE, [("attributes_float", "TYPE_DOUBLE")]),
            (AttributeKey.Type.TYPE_FLOAT, [("attributes_float", "TYPE_FLOAT")]),
            (
                AttributeKey.Type.TYPE_ARRAY_STRING,
                [("attributes_array_string", "TYPE_ARRAY_STRING")],
            ),
            (AttributeKey.Type.TYPE_ARRAY_INT, [("attributes_array_int", "TYPE_ARRAY_INT")]),
            (
                AttributeKey.Type.TYPE_ARRAY_DOUBLE,
                [("attributes_array_float", "TYPE_ARRAY_DOUBLE")],
            ),
            (AttributeKey.Type.TYPE_ARRAY_BOOL, [("attributes_array_bool", "TYPE_ARRAY_BOOL")]),
        ],
    )
    def test_v2_reads_one_array_per_type(
        self, attr_type: AttributeKey.Type.ValueType, expected: list[tuple[str, str]]
    ) -> None:
        assert list(V2.typed_key_arrays(attr_type)) == expected

    def test_v2_untyped_array_surfaces_all_four_element_types(self) -> None:
        assert list(V2.typed_key_arrays(AttributeKey.Type.TYPE_ARRAY)) == [
            ("attributes_array_string", "TYPE_ARRAY_STRING"),
            ("attributes_array_int", "TYPE_ARRAY_INT"),
            ("attributes_array_float", "TYPE_ARRAY_DOUBLE"),
            ("attributes_array_bool", "TYPE_ARRAY_BOOL"),
        ]

    def test_v2_unspecified_covers_every_type_without_duplicating_ints(self) -> None:
        """attributes_int is left out of TYPE_UNSPECIFIED on purpose: int keys are
        double-written to a float bucket, so including both would emit them twice."""
        arrays = list(V2.typed_key_arrays(AttributeKey.Type.TYPE_UNSPECIFIED))
        columns = [col for col, _ in arrays]
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
        # every key is tagged with exactly one type, and no column repeats
        assert len(set(columns)) == len(columns)

    def test_key_array_columns_matches_typed_key_arrays(self) -> None:
        for source in (V1, V2):
            for attr_type in (
                AttributeKey.Type.TYPE_STRING,
                AttributeKey.Type.TYPE_INT,
                AttributeKey.Type.TYPE_UNSPECIFIED,
            ):
                assert source.key_array_columns(attr_type) == [
                    col for col, _ in source.typed_key_arrays(attr_type)
                ]


@pytest.mark.redis_db
class TestForRequest:
    def test_flag_off_reads_v1(self) -> None:
        with override_options("snuba", {CO_OCCURRING_ATTRS_V2_OPTION: False}):
            assert co_occurring_attrs.for_request(_request(V2_START)) is V1

    def test_flag_off_reads_v1_even_well_inside_the_window(self) -> None:
        with override_options("snuba", {CO_OCCURRING_ATTRS_V2_OPTION: False}):
            assert co_occurring_attrs.for_request(_request(V2_START + timedelta(days=90))) is V1

    def test_flag_on_inside_window_reads_v2(self) -> None:
        with override_options("snuba", {CO_OCCURRING_ATTRS_V2_OPTION: True}):
            assert co_occurring_attrs.for_request(_request(V2_START)) is V2

    def test_flag_on_before_window_falls_back_to_v1(self) -> None:
        with override_options("snuba", {CO_OCCURRING_ATTRS_V2_OPTION: True}):
            assert co_occurring_attrs.for_request(_request(V2_START - timedelta(seconds=1))) is V1

    def test_gate_compares_the_rounded_bucket(self) -> None:
        """The gate must compare the bucket the query reads, not the raw start timestamp.

        With a mid-week cutoff, a request starting after that instant still reads from the
        Monday before it — a bucket v2 never populated — so it must fall back to v1.
        """
        wednesday = V2_START + timedelta(days=2)
        with override_options(
            "snuba",
            {
                CO_OCCURRING_ATTRS_V2_OPTION: True,
                CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION: int(wednesday.timestamp()),
            },
        ):
            assert co_occurring_attrs.for_request(_request(wednesday + timedelta(hours=1))) is V1

    def test_start_timestamp_option_widens_the_window(self) -> None:
        old = _request(V2_START - timedelta(days=365))
        with override_options("snuba", {CO_OCCURRING_ATTRS_V2_OPTION: True}):
            assert co_occurring_attrs.for_request(old) is V1
        with override_options(
            "snuba",
            {
                CO_OCCURRING_ATTRS_V2_OPTION: True,
                CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION: 0,
            },
        ):
            assert co_occurring_attrs.for_request(old) is V2

    def test_default_cutoff_is_a_monday(self) -> None:
        """`date` is bucketed with toMonday() and the query rounds down to the previous
        Monday, so a mid-week cutoff would admit requests that read a v1-only bucket."""
        assert V2_START.weekday() == 0
        assert (V2_START.hour, V2_START.minute, V2_START.second) == (0, 0, 0)
