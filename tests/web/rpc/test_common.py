from datetime import datetime, timedelta

import pytest
from google.protobuf import json_format, struct_pb2
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AnyAttributeFilter,
    TraceItemFilter,
)

from snuba import settings
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, in_cond, literal, literals_array, not_cond, or_cond
from snuba.query.expressions import Argument, Expression, Lambda
from snuba.web.rpc.common.common import (
    _any_attribute_filter_to_expression,
    next_monday,
    prev_monday,
    trace_item_filters_to_expression,
    use_sampling_factor,
)
from snuba.web.rpc.common.exceptions import (
    BadSnubaRPCRequestException,
    RPCAllocationPolicyException,
    convert_rpc_exception_to_proto,
)
from tests.conftest import SnubaSetConfig


class TestCommon:
    def test_timestamp_rounding(self) -> None:
        start = datetime(2025, 3, 10)
        end = datetime(2025, 3, 17)

        tmp = start.replace()
        for _ in range(7):
            assert prev_monday(tmp) == start
            assert next_monday(tmp) == end
            tmp += timedelta(days=1)

    @pytest.mark.redis_db
    def test_use_sampling_factor(self, snuba_set_config: SnubaSetConfig) -> None:
        assert use_sampling_factor(
            RequestMeta(
                start_timestamp=Timestamp(seconds=settings.USE_SAMPLING_FACTOR_TIMESTAMP_SECONDS)
            )
        )
        assert not use_sampling_factor(
            RequestMeta(
                start_timestamp=Timestamp(
                    seconds=settings.USE_SAMPLING_FACTOR_TIMESTAMP_SECONDS - 1
                )
            )
        )
        snuba_set_config("use_sampling_factor_timestamp_seconds", 10)
        assert use_sampling_factor(RequestMeta(start_timestamp=Timestamp(seconds=10)))
        assert not use_sampling_factor(RequestMeta(start_timestamp=Timestamp(seconds=9)))


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_convert_rpc_exception_to_proto_packs_details() -> None:
    routing_decision_log_dict = {
        "can_run": False,
        "is_throttled": True,
        "strategy": "OutcomesBasedRoutingStrategy",
        "source_request_id": "21df09b9-41a8-4529-9fc7-d75c851adbfe",
        "extra_info": {
            "sampling_in_storage_estimation_time_overhead": {
                "type": "timing",
                "value": 10,
                "tags": None,
            }
        },
        "clickhouse_settings": {"max_threads": 0},
        "result_info": {},
        "routed_tier": "TIER_1",
        "allocation_policies_recommendations": {
            "RejectionPolicy": {
                "can_run": False,
                "max_threads": 0,
                "explanation": {
                    "reason": "policy rejects all queries",
                    "storage_key": "doesntmatter",
                },
                "is_throttled": True,
                "throttle_threshold": 1000000000000,
                "rejection_threshold": 1000000000000,
                "quota_used": 0,
                "quota_unit": "no_units",
                "suggestion": "no_suggestion",
                "max_bytes_to_read": 0,
            }
        },
    }

    exc = RPCAllocationPolicyException(
        "Query cannot be run due to routing strategy deciding it cannot run, most likely due to allocation policies",
        routing_decision_log_dict,
    )

    proto = convert_rpc_exception_to_proto(exc)
    assert isinstance(proto, ErrorProto)
    assert proto.code == 429
    assert (
        proto.message
        == "Query cannot be run due to routing strategy deciding it cannot run, most likely due to allocation policies"
    )

    s = struct_pb2.Struct()
    assert proto.details[0].Unpack(s)
    unpacked = json_format.MessageToDict(s)
    assert unpacked == routing_decision_log_dict


def _dummy_attr_key_to_expression(attr_key: AttributeKey) -> Expression:
    """Dummy for tests that don't use attribute_key_to_expression."""
    return column(attr_key.name)


class TestAnyAttributeFilter:
    def test_equals_string_default_type(self) -> None:
        """Default attribute_types (empty) should search attributes_string."""
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_EQUALS,
            value=AttributeValue(val_str="error"),
        )
        result = _any_attribute_filter_to_expression(filt)
        x = Argument(None, "x")
        expected = f.arrayExists(
            Lambda(None, ("x",), f.equals(x, literal("error"))),
            f.mapValues(column("attributes_string")),
        )
        assert result == expected

    def test_equals_ignore_case(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_EQUALS,
            value=AttributeValue(val_str="Error"),
            ignore_case=True,
        )
        result = _any_attribute_filter_to_expression(filt)
        x = Argument(None, "x")
        expected = f.arrayExists(
            Lambda(None, ("x",), f.equals(f.lower(x), f.lower(literal("Error")))),
            f.mapValues(column("attributes_string")),
        )
        assert result == expected

    def test_like_string(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_LIKE,
            value=AttributeValue(val_str="%error%"),
        )
        result = _any_attribute_filter_to_expression(filt)
        x = Argument(None, "x")
        expected = f.arrayExists(
            Lambda(None, ("x",), f.like(x, literal("%error%"))),
            f.mapValues(column("attributes_string")),
        )
        assert result == expected

    def test_ilike_string(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_LIKE,
            value=AttributeValue(val_str="%error%"),
            ignore_case=True,
        )
        result = _any_attribute_filter_to_expression(filt)
        x = Argument(None, "x")
        expected = f.arrayExists(
            Lambda(None, ("x",), f.ilike(x, literal("%error%"))),
            f.mapValues(column("attributes_string")),
        )
        assert result == expected

    def test_in_string_array(self) -> None:
        from sentry_protos.snuba.v1.trace_item_attribute_pb2 import StrArray

        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_IN,
            value=AttributeValue(val_str_array=StrArray(values=["a", "b", "c"])),
        )
        result = _any_attribute_filter_to_expression(filt)
        x = Argument(None, "x")
        arr = literals_array(None, [literal("a"), literal("b"), literal("c")])
        expected = f.arrayExists(
            Lambda(None, ("x",), in_cond(x, arr)),
            f.mapValues(column("attributes_string")),
        )
        assert result == expected

    def test_in_ignore_case(self) -> None:
        from sentry_protos.snuba.v1.trace_item_attribute_pb2 import StrArray

        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_IN,
            value=AttributeValue(val_str_array=StrArray(values=["A", "B"])),
            ignore_case=True,
        )
        result = _any_attribute_filter_to_expression(filt)
        x = Argument(None, "x")
        arr = literals_array(None, [literal("a"), literal("b")])
        expected = f.arrayExists(
            Lambda(None, ("x",), in_cond(f.lower(x), arr)),
            f.mapValues(column("attributes_string")),
        )
        assert result == expected

    def test_not_equals_negates(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_NOT_EQUALS,
            value=AttributeValue(val_str="error"),
        )
        result = _any_attribute_filter_to_expression(filt)
        x = Argument(None, "x")
        positive = f.arrayExists(
            Lambda(None, ("x",), f.equals(x, literal("error"))),
            f.mapValues(column("attributes_string")),
        )
        expected = not_cond(positive)
        assert result == expected

    def test_not_like_negates(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_NOT_LIKE,
            value=AttributeValue(val_str="%error%"),
        )
        result = _any_attribute_filter_to_expression(filt)
        x = Argument(None, "x")
        positive = f.arrayExists(
            Lambda(None, ("x",), f.like(x, literal("%error%"))),
            f.mapValues(column("attributes_string")),
        )
        expected = not_cond(positive)
        assert result == expected

    def test_not_in_negates(self) -> None:
        from sentry_protos.snuba.v1.trace_item_attribute_pb2 import StrArray

        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_NOT_IN,
            value=AttributeValue(val_str_array=StrArray(values=["a", "b"])),
        )
        result = _any_attribute_filter_to_expression(filt)
        x = Argument(None, "x")
        arr = literals_array(None, [literal("a"), literal("b")])
        positive = f.arrayExists(
            Lambda(None, ("x",), in_cond(x, arr)),
            f.mapValues(column("attributes_string")),
        )
        expected = not_cond(positive)
        assert result == expected

    def test_custom_attribute_types_float(self) -> None:
        """Searching float attributes should use attributes_float column."""
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_EQUALS,
            value=AttributeValue(val_double=3.14),
            attribute_types=[AttributeKey.Type.TYPE_FLOAT],
        )
        result = _any_attribute_filter_to_expression(filt)
        x = Argument(None, "x")
        expected = f.arrayExists(
            Lambda(None, ("x",), f.equals(x, literal(3.14))),
            f.mapValues(column("attributes_float")),
        )
        assert result == expected

    def test_multiple_attribute_types_deduplicates(self) -> None:
        """TYPE_INT, TYPE_FLOAT, TYPE_DOUBLE all map to attributes_float, so should deduplicate."""
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_EQUALS,
            value=AttributeValue(val_float=1.0),
            attribute_types=[
                AttributeKey.Type.TYPE_INT,
                AttributeKey.Type.TYPE_FLOAT,
                AttributeKey.Type.TYPE_DOUBLE,
            ],
        )
        result = _any_attribute_filter_to_expression(filt)
        # All three map to attributes_float, so only one column
        x = Argument(None, "x")
        expected = f.arrayExists(
            Lambda(None, ("x",), f.equals(x, literal(1.0))),
            f.mapValues(column("attributes_float")),
        )
        assert result == expected

    def test_multiple_distinct_attribute_types_or(self) -> None:
        """Searching string + float should OR across both columns."""
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_EQUALS,
            value=AttributeValue(val_str="42"),
            attribute_types=[
                AttributeKey.Type.TYPE_STRING,
                AttributeKey.Type.TYPE_FLOAT,
            ],
        )
        result = _any_attribute_filter_to_expression(filt)
        x = Argument(None, "x")
        lam = Lambda(None, ("x",), f.equals(x, literal("42")))
        expected = or_cond(
            f.arrayExists(lam, f.mapValues(column("attributes_string"))),
            f.arrayExists(lam, f.mapValues(column("attributes_float"))),
        )
        assert result == expected

    def test_like_on_non_string_type_raises(self) -> None:
        """LIKE on float-only types should raise an error."""
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_LIKE,
            value=AttributeValue(val_str="%error%"),
            attribute_types=[AttributeKey.Type.TYPE_FLOAT],
        )
        with pytest.raises(BadSnubaRPCRequestException, match="LIKE/NOT_LIKE"):
            _any_attribute_filter_to_expression(filt)

    def test_not_like_on_non_string_type_raises(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_NOT_LIKE,
            value=AttributeValue(val_str="%error%"),
            attribute_types=[AttributeKey.Type.TYPE_FLOAT],
        )
        with pytest.raises(BadSnubaRPCRequestException, match="LIKE/NOT_LIKE"):
            _any_attribute_filter_to_expression(filt)

    def test_no_value_raises(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_EQUALS,
        )
        with pytest.raises(BadSnubaRPCRequestException, match="does not have a value"):
            _any_attribute_filter_to_expression(filt)

    def test_via_trace_item_filter(self) -> None:
        """Test that trace_item_filters_to_expression dispatches to any_attribute_filter."""
        item_filter = TraceItemFilter(
            any_attribute_filter=AnyAttributeFilter(
                op=AnyAttributeFilter.OP_EQUALS,
                value=AttributeValue(val_str="test"),
            )
        )
        result = trace_item_filters_to_expression(item_filter, _dummy_attr_key_to_expression)
        x = Argument(None, "x")
        expected = f.arrayExists(
            Lambda(None, ("x",), f.equals(x, literal("test"))),
            f.mapValues(column("attributes_string")),
        )
        assert result == expected

    def test_boolean_attribute_type(self) -> None:
        """Searching boolean attributes should use attributes_bool column."""
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_EQUALS,
            value=AttributeValue(val_bool=True),
            attribute_types=[AttributeKey.Type.TYPE_BOOLEAN],
        )
        result = _any_attribute_filter_to_expression(filt)
        x = Argument(None, "x")
        expected = f.arrayExists(
            Lambda(None, ("x",), f.equals(x, literal(True))),
            f.mapValues(column("attributes_bool")),
        )
        assert result == expected
