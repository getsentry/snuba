import uuid
from datetime import datetime, timedelta, timezone

import pytest
from google.protobuf import json_format, struct_pb2
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1.request_common_pb2 import (
    RequestMeta,
    TraceItemType,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeKey,
    AttributeValue,
    IntArray,
    StrArray,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AnyAttributeFilter,
    TraceItemFilter,
)
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue

from snuba import settings
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.common.common import (
    _any_attribute_filter_to_expression,
    next_monday,
    prev_monday,
    use_sampling_factor,
)
from snuba.web.rpc.common.exceptions import (
    BadSnubaRPCRequestException,
    RPCAllocationPolicyException,
    convert_rpc_exception_to_proto,
)
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.conftest import SnubaSetConfig
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import gen_item_message


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


class TestAnyAttributeFilter:
    def test_like_on_non_string_type_raises(self) -> None:
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

    def test_ignore_case_on_non_string_equals_raises(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_EQUALS,
            value=AttributeValue(val_int=42),
            ignore_case=True,
        )
        with pytest.raises(
            BadSnubaRPCRequestException, match="Cannot ignore case on non-string values"
        ):
            _any_attribute_filter_to_expression(filt)

    def test_ignore_case_on_non_string_in_raises(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_IN,
            value=AttributeValue(val_int_array=IntArray(values=[1, 2, 3])),
            ignore_case=True,
        )
        with pytest.raises(
            BadSnubaRPCRequestException, match="Cannot ignore case on non-string values"
        ):
            _any_attribute_filter_to_expression(filt)

    def test_in_with_scalar_value_raises(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_IN,
            value=AttributeValue(val_str="hello"),
        )
        with pytest.raises(
            BadSnubaRPCRequestException, match="IN/NOT_IN operations require an array value type"
        ):
            _any_attribute_filter_to_expression(filt)

    def test_not_in_with_scalar_value_raises(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_NOT_IN,
            value=AttributeValue(val_int=42),
        )
        with pytest.raises(
            BadSnubaRPCRequestException, match="IN/NOT_IN operations require an array value type"
        ):
            _any_attribute_filter_to_expression(filt)


@pytest.mark.eap
@pytest.mark.redis_db
class TestAnyAttributeFilterIntegration:
    """Integration tests that insert spans into ClickHouse and query them
    with AnyAttributeFilter via EndpointTraceItemTable."""

    UNIQUE_VALUE = f"needle-{uuid.uuid4().hex[:8]}"

    @pytest.fixture(autouse=True)
    def setup(self, eap: None, redis_db: None) -> None:
        self.base_time = datetime.now(tz=timezone.utc).replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(hours=1)
        self.start_ts = Timestamp(seconds=int((self.base_time - timedelta(hours=1)).timestamp()))
        self.end_ts = Timestamp(seconds=int((self.base_time + timedelta(hours=2)).timestamp()))

        # Span 0: the target — has the unique needle value on "haystack" attr
        # Span 1: a decoy with a different value
        # Span 2: another decoy with no "haystack" attribute at all
        messages = [
            gen_item_message(
                start_timestamp=self.base_time,
                attributes={
                    "haystack": AnyValue(string_value=self.UNIQUE_VALUE),
                    "color": AnyValue(string_value="red"),
                },
            ),
            gen_item_message(
                start_timestamp=self.base_time + timedelta(minutes=1),
                attributes={
                    "haystack": AnyValue(string_value="decoy-value"),
                    "color": AnyValue(string_value="blue"),
                },
            ),
            gen_item_message(
                start_timestamp=self.base_time + timedelta(minutes=2),
                attributes={
                    "color": AnyValue(string_value="green"),
                },
            ),
        ]
        storage = get_storage(StorageKey("eap_items"))
        write_raw_unprocessed_events(storage, messages)  # type: ignore

    def _execute(self, filt: TraceItemFilter) -> list[str]:
        """Run a TraceItemTable query with the given filter, returning
        the matched 'color' attribute values as a sorted list."""
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="test",
                referrer="test",
                start_timestamp=self.start_ts,
                end_timestamp=self.end_ts,
                request_id=uuid.uuid4().hex,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=filt,
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color")),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color"))
                )
            ],
            limit=100,
        )
        response = EndpointTraceItemTable().execute(message)
        if not response.column_values:
            return []
        return sorted(r.val_str for r in response.column_values[0].results)

    def test_equals_finds_target_span(self) -> None:
        """OP_EQUALS on a unique string value should return only the target span."""
        colors = self._execute(
            TraceItemFilter(
                any_attribute_filter=AnyAttributeFilter(
                    op=AnyAttributeFilter.OP_EQUALS,
                    value=AttributeValue(val_str=self.UNIQUE_VALUE),
                )
            )
        )
        assert colors == ["red"]

    def test_like_finds_target_span(self) -> None:
        """OP_LIKE with a pattern matching the unique value finds the target."""
        colors = self._execute(
            TraceItemFilter(
                any_attribute_filter=AnyAttributeFilter(
                    op=AnyAttributeFilter.OP_LIKE,
                    value=AttributeValue(val_str=f"%{self.UNIQUE_VALUE}%"),
                )
            )
        )
        assert colors == ["red"]

    def test_not_equals_excludes_target_span(self) -> None:
        """OP_NOT_EQUALS on the unique value should return the other two spans."""
        colors = self._execute(
            TraceItemFilter(
                any_attribute_filter=AnyAttributeFilter(
                    op=AnyAttributeFilter.OP_NOT_EQUALS,
                    value=AttributeValue(val_str=self.UNIQUE_VALUE),
                )
            )
        )
        assert colors == ["blue", "green"]

    def test_in_finds_target_span(self) -> None:
        """OP_IN with an array containing the unique value finds the target."""
        colors = self._execute(
            TraceItemFilter(
                any_attribute_filter=AnyAttributeFilter(
                    op=AnyAttributeFilter.OP_IN,
                    value=AttributeValue(
                        val_str_array=StrArray(values=[self.UNIQUE_VALUE, "no-match"])
                    ),
                )
            )
        )
        assert colors == ["red"]

    def test_equals_ignore_case(self) -> None:
        """OP_EQUALS with ignore_case matches regardless of casing."""
        colors = self._execute(
            TraceItemFilter(
                any_attribute_filter=AnyAttributeFilter(
                    op=AnyAttributeFilter.OP_EQUALS,
                    value=AttributeValue(val_str=self.UNIQUE_VALUE.upper()),
                    ignore_case=True,
                )
            )
        )
        assert colors == ["red"]

    def test_no_match_returns_empty(self) -> None:
        """Searching for a value that doesn't exist returns nothing."""
        colors = self._execute(
            TraceItemFilter(
                any_attribute_filter=AnyAttributeFilter(
                    op=AnyAttributeFilter.OP_EQUALS,
                    value=AttributeValue(val_str="value-that-does-not-exist"),
                )
            )
        )
        assert colors == []
