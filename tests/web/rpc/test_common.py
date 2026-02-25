from datetime import datetime, timedelta

import pytest
from google.protobuf import json_format, struct_pb2
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import ComparisonFilter, TraceItemFilter

from snuba import settings
from snuba.query.expressions import DangerousRawSQL, FunctionCall, Lambda
from snuba.web.rpc.common.common import (
    attribute_key_to_expression,
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


class TestTraceItemFiltersArrayLike:
    def _make_like_filter(
        self,
        attr_name: str,
        attr_type: AttributeKey.Type.ValueType,
        pattern: str,
        op: ComparisonFilter.Op.ValueType = ComparisonFilter.OP_LIKE,
        ignore_case: bool = False,
    ) -> TraceItemFilter:
        return TraceItemFilter(
            comparison_filter=ComparisonFilter(
                key=AttributeKey(type=attr_type, name=attr_name),
                op=op,
                value=AttributeValue(val_str=pattern),
                ignore_case=ignore_case,
            )
        )

    def test_like_on_array_key(self) -> None:
        item_filter = self._make_like_filter("my_tags", AttributeKey.Type.TYPE_ARRAY, "%error%")
        result = trace_item_filters_to_expression(item_filter, attribute_key_to_expression)
        assert isinstance(result, FunctionCall)
        assert result.function_name == "arrayExists"
        # First param is a Lambda with like
        lam = result.parameters[0]
        assert isinstance(lam, Lambda)
        assert lam.parameters == ("x",)
        assert isinstance(lam.transformation, FunctionCall)
        assert lam.transformation.function_name == "like"
        # Second param is the array expression (DangerousRawSQL)
        assert isinstance(result.parameters[1], DangerousRawSQL)

    def test_like_on_array_key_ignore_case(self) -> None:
        item_filter = self._make_like_filter(
            "my_tags", AttributeKey.Type.TYPE_ARRAY, "%error%", ignore_case=True
        )
        result = trace_item_filters_to_expression(item_filter, attribute_key_to_expression)
        assert isinstance(result, FunctionCall)
        assert result.function_name == "arrayExists"
        lam = result.parameters[0]
        assert isinstance(lam, Lambda)
        assert isinstance(lam.transformation, FunctionCall)
        assert lam.transformation.function_name == "ilike"

    def test_not_like_on_array_key(self) -> None:
        item_filter = self._make_like_filter(
            "my_tags",
            AttributeKey.Type.TYPE_ARRAY,
            "%error%",
            op=ComparisonFilter.OP_NOT_LIKE,
        )
        result = trace_item_filters_to_expression(item_filter, attribute_key_to_expression)
        # Result should be NOT(arrayExists(...))
        assert isinstance(result, FunctionCall)
        assert result.function_name == "not"
        inner = result.parameters[0]
        assert isinstance(inner, FunctionCall)
        assert inner.function_name == "arrayExists"
        lam = inner.parameters[0]
        assert isinstance(lam, Lambda)
        assert isinstance(lam.transformation, FunctionCall)
        assert lam.transformation.function_name == "like"

    def test_not_like_on_array_key_ignore_case(self) -> None:
        item_filter = self._make_like_filter(
            "my_tags",
            AttributeKey.Type.TYPE_ARRAY,
            "%error%",
            op=ComparisonFilter.OP_NOT_LIKE,
            ignore_case=True,
        )
        result = trace_item_filters_to_expression(item_filter, attribute_key_to_expression)
        assert isinstance(result, FunctionCall)
        assert result.function_name == "not"
        inner = result.parameters[0]
        assert isinstance(inner, FunctionCall)
        assert inner.function_name == "arrayExists"
        lam = inner.parameters[0]
        assert isinstance(lam, Lambda)
        assert isinstance(lam.transformation, FunctionCall)
        assert lam.transformation.function_name == "ilike"

    def test_like_on_array_key_non_string_value_raises(self) -> None:
        item_filter = TraceItemFilter(
            comparison_filter=ComparisonFilter(
                key=AttributeKey(type=AttributeKey.Type.TYPE_ARRAY, name="my_tags"),
                op=ComparisonFilter.OP_LIKE,
                value=AttributeValue(val_int=42),
            )
        )
        with pytest.raises(
            BadSnubaRPCRequestException,
            match="LIKE/NOT_LIKE on array keys requires a string pattern",
        ):
            trace_item_filters_to_expression(item_filter, attribute_key_to_expression)

    def test_equals_on_array_key_raises(self) -> None:
        item_filter = TraceItemFilter(
            comparison_filter=ComparisonFilter(
                key=AttributeKey(type=AttributeKey.Type.TYPE_ARRAY, name="my_tags"),
                op=ComparisonFilter.OP_EQUALS,
                value=AttributeValue(val_str="something"),
            )
        )
        with pytest.raises(
            BadSnubaRPCRequestException,
            match="only LIKE and NOT_LIKE comparisons are supported on array keys",
        ):
            trace_item_filters_to_expression(item_filter, attribute_key_to_expression)

    def test_like_on_int_key_raises(self) -> None:
        item_filter = self._make_like_filter("my_int", AttributeKey.Type.TYPE_INT, "%something%")
        with pytest.raises(
            BadSnubaRPCRequestException,
            match="LIKE comparison is only supported on string and array keys",
        ):
            trace_item_filters_to_expression(item_filter, attribute_key_to_expression)

    def test_not_like_on_int_key_raises(self) -> None:
        item_filter = self._make_like_filter(
            "my_int",
            AttributeKey.Type.TYPE_INT,
            "%something%",
            op=ComparisonFilter.OP_NOT_LIKE,
        )
        with pytest.raises(
            BadSnubaRPCRequestException,
            match="NOT LIKE comparison is only supported on string and array keys",
        ):
            trace_item_filters_to_expression(item_filter, attribute_key_to_expression)


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
