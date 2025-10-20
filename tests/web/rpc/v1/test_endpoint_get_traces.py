import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_get_traces_pb2 import (
    GetTracesRequest,
    GetTracesResponse,
    TraceAttribute,
)
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1.request_common_pb2 import (
    PageToken,
    RequestMeta,
    ResponseMeta,
    TraceItemType,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, TraceItem

from snuba import state
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.endpoint_get_traces import EndpointGetTraces
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import (
    comparison_filter,
    create_cross_item_test_data,
    create_request_meta,
    gen_item_message,
    or_filter,
    write_cross_item_data_to_storage,
)

_TRACE_IDS = [uuid.uuid4().hex for _ in range(10)]
_BASE_TIME = datetime.now(tz=timezone.utc).replace(
    minute=0,
    second=0,
    microsecond=0,
) - timedelta(hours=2)
_SPAN_COUNT = 120
_REQUEST_ID = uuid.uuid4().hex
_SPANS = [
    gen_item_message(
        start_timestamp=_BASE_TIME + timedelta(minutes=i),
        trace_id=_TRACE_IDS[i % len(_TRACE_IDS)],
        attributes={
            "sentry.op": AnyValue(string_value="navigation" if i < len(_TRACE_IDS) else "db"),
            "sentry.raw_description": AnyValue(
                string_value=(
                    "root"
                    if i < len(_TRACE_IDS)
                    else f"child {i % len(_TRACE_IDS) + 1} of {_SPAN_COUNT // len(_TRACE_IDS) - 1}"
                )
            ),
            "is_segment": AnyValue(bool_value=i < len(_TRACE_IDS)),
            "sentry.segment_id": AnyValue(
                string_value=_TRACE_IDS[i % len(_TRACE_IDS)][:16],
            ),
            "sentry.parent_span_id": AnyValue(string_value="" if i < len(_TRACE_IDS) else "1" * 16),
        },
    )
    for i in range(_SPAN_COUNT)
]
_ADDITIONAL_TRACE_IDS = [uuid.uuid4().hex for _ in range(_SPAN_COUNT)]
_ADDITIONAL_SPANS = [
    gen_item_message(
        start_timestamp=_BASE_TIME + timedelta(hours=1, minutes=i),
        trace_id=_ADDITIONAL_TRACE_IDS[i],
        attributes={
            "span_op": AnyValue(string_value="lcp"),
            "span_name": AnyValue(string_value="standalone"),
            "is_segment": AnyValue(bool_value=False),
        },
    )
    for i in range(_SPAN_COUNT)
]


@pytest.fixture(autouse=False)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    items_storage = get_storage(StorageKey("eap_items"))
    state.set_config("enable_trace_sampling", True)

    write_raw_unprocessed_events(items_storage, _SPANS)  # type: ignore
    write_raw_unprocessed_events(items_storage, _ADDITIONAL_SPANS)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestGetTraces(BaseApiTest):
    def test_without_data(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = GetTracesRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=ts,
                request_id=_REQUEST_ID,
            ),
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_TRACE_ID,
                )
            ],
            limit=10,
        )
        response = self.app.post("/rpc/EndpointGetTraces/v1", data=message.SerializeToString())
        error_proto = ErrorProto()
        if response.status_code != 200:
            error_proto.ParseFromString(response.data)
        assert response.status_code == 200, error_proto

    def test_with_data(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        three_hours_later = int((_BASE_TIME + timedelta(hours=3)).timestamp())
        (
            start_timestamp_per_trace_id,
            trace_id_per_start_timestamp,
        ) = generate_trace_id_timestamp_data(_SPANS + _ADDITIONAL_SPANS)
        message = GetTracesRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=Timestamp(seconds=three_hours_later),
                request_id=_REQUEST_ID,
            ),
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_TRACE_ID,
                ),
            ],
        )
        response = EndpointGetTraces().execute(message)
        expected_response = GetTracesResponse(
            traces=[
                GetTracesResponse.Trace(
                    attributes=[
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_TRACE_ID,
                            type=AttributeKey.TYPE_STRING,
                            value=AttributeValue(
                                val_str=trace_id_per_start_timestamp[start_timestamp],
                            ),
                        ),
                    ],
                )
                for start_timestamp in reversed(sorted(trace_id_per_start_timestamp.keys()))
            ],
            page_token=PageToken(offset=len(_TRACE_IDS + _ADDITIONAL_TRACE_IDS)),
            meta=ResponseMeta(request_id=_REQUEST_ID),
        )
        assert MessageToDict(response) == MessageToDict(expected_response)

    def test_with_data_and_limit(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        ten_hours_later = int((_BASE_TIME + timedelta(hours=10)).timestamp())
        message = GetTracesRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=Timestamp(seconds=ten_hours_later),
                request_id=_REQUEST_ID,
            ),
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_TRACE_ID,
                ),
            ],
            limit=1,
        )
        response = EndpointGetTraces().execute(message)
        spans = generate_spans(_SPANS + _ADDITIONAL_SPANS)
        last_span = spans[0]
        trace_ids = [span.trace_id for span in spans]
        print(trace_ids)
        for span in spans:
            if span.timestamp.seconds >= last_span.timestamp.seconds:
                last_span = span
        expected_response = GetTracesResponse(
            traces=[
                GetTracesResponse.Trace(
                    attributes=[
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_TRACE_ID,
                            type=AttributeKey.Type.TYPE_STRING,
                            value=AttributeValue(
                                val_str=last_span.trace_id,
                            ),
                        )
                    ],
                )
            ],
            page_token=PageToken(offset=1),
            meta=ResponseMeta(request_id=_REQUEST_ID),
        )
        assert MessageToDict(response) == MessageToDict(expected_response)

    def test_with_data_and_filter(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        three_hours_later = int((_BASE_TIME + timedelta(hours=3)).timestamp())
        message = GetTracesRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=Timestamp(seconds=three_hours_later),
                request_id=_REQUEST_ID,
            ),
            filters=[
                GetTracesRequest.TraceFilter(
                    item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    filter=TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                name="sentry.trace_id",
                                type=AttributeKey.TYPE_STRING,
                            ),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(
                                val_str=_TRACE_IDS[0],
                            ),
                        ),
                    ),
                ),
            ],
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_TRACE_ID,
                ),
            ],
        )
        response = EndpointGetTraces().execute(message)
        expected_response = GetTracesResponse(
            traces=[
                GetTracesResponse.Trace(
                    attributes=[
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_TRACE_ID,
                            type=AttributeKey.Type.TYPE_STRING,
                            value=AttributeValue(
                                val_str=_TRACE_IDS[0],
                            ),
                        )
                    ],
                )
            ],
            page_token=PageToken(offset=1),
            meta=ResponseMeta(request_id=_REQUEST_ID),
        )
        assert MessageToDict(response) == MessageToDict(expected_response)

    def test_with_data_and_aggregated_fields_all_keys(self, setup_teardown: Any) -> None:
        start_timestamp = Timestamp(seconds=int((_BASE_TIME - timedelta(hours=10)).timestamp()))
        end_timestamp = Timestamp(seconds=int((_BASE_TIME + timedelta(hours=10)).timestamp()))
        (
            start_timestamp_per_trace_id,
            trace_id_per_start_timestamp,
        ) = generate_trace_id_timestamp_data(_SPANS)
        message = GetTracesRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                request_id=_REQUEST_ID,
            ),
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_TRACE_ID,
                    type=AttributeKey.TYPE_STRING,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_START_TIMESTAMP,
                    type=AttributeKey.TYPE_DOUBLE,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_TOTAL_ITEM_COUNT,
                    type=AttributeKey.TYPE_INT,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_FILTERED_ITEM_COUNT,
                    type=AttributeKey.TYPE_INT,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_ROOT_SPAN_NAME,
                    type=AttributeKey.TYPE_STRING,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_ROOT_SPAN_DURATION_MS,
                    type=AttributeKey.TYPE_INT,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_ROOT_SPAN_PROJECT_ID,
                    type=AttributeKey.TYPE_INT,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_EARLIEST_SPAN_NAME,
                    type=AttributeKey.TYPE_STRING,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_EARLIEST_SPAN_PROJECT_ID,
                    type=AttributeKey.TYPE_INT,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_EARLIEST_SPAN_DURATION_MS,
                    type=AttributeKey.TYPE_INT,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_EARLIEST_FRONTEND_SPAN,
                    type=AttributeKey.TYPE_STRING,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_EARLIEST_FRONTEND_SPAN_PROJECT_ID,
                    type=AttributeKey.TYPE_INT,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_EARLIEST_FRONTEND_SPAN_DURATION_MS,
                    type=AttributeKey.TYPE_INT,
                ),
            ],
            filters=[
                GetTracesRequest.TraceFilter(
                    item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    filter=TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                name="sentry.op",
                                type=AttributeKey.TYPE_STRING,
                            ),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_str="db"),
                        ),
                    ),
                ),
            ],
        )
        response = EndpointGetTraces().execute(message)
        expected_response = GetTracesResponse(
            traces=[
                GetTracesResponse.Trace(
                    attributes=[
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_TRACE_ID,
                            type=AttributeKey.TYPE_STRING,
                            value=AttributeValue(
                                val_str=trace_id_per_start_timestamp[start_timestamp],
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_START_TIMESTAMP,
                            type=AttributeKey.TYPE_DOUBLE,
                            value=AttributeValue(
                                val_double=start_timestamp_per_trace_id[
                                    trace_id_per_start_timestamp[start_timestamp]
                                ],
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_TOTAL_ITEM_COUNT,
                            type=AttributeKey.TYPE_INT,
                            value=AttributeValue(
                                val_int=_SPAN_COUNT // len(_TRACE_IDS),
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_FILTERED_ITEM_COUNT,
                            type=AttributeKey.TYPE_INT,
                            value=AttributeValue(
                                val_int=(_SPAN_COUNT // len(_TRACE_IDS)) - 1,
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_ROOT_SPAN_NAME,
                            type=AttributeKey.TYPE_STRING,
                            value=AttributeValue(
                                val_str="root",
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_ROOT_SPAN_DURATION_MS,
                            type=AttributeKey.TYPE_INT,
                            value=AttributeValue(
                                val_int=152,
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_ROOT_SPAN_PROJECT_ID,
                            type=AttributeKey.TYPE_INT,
                            value=AttributeValue(
                                val_int=1,
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_EARLIEST_SPAN_NAME,
                            type=AttributeKey.TYPE_STRING,
                            value=AttributeValue(
                                val_str="root",
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_EARLIEST_SPAN_PROJECT_ID,
                            type=AttributeKey.TYPE_INT,
                            value=AttributeValue(
                                val_int=1,
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_EARLIEST_SPAN_DURATION_MS,
                            type=AttributeKey.TYPE_INT,
                            value=AttributeValue(
                                val_int=152,
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_EARLIEST_FRONTEND_SPAN,
                            type=AttributeKey.TYPE_STRING,
                            value=AttributeValue(
                                val_str="root",
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_EARLIEST_FRONTEND_SPAN_PROJECT_ID,
                            type=AttributeKey.TYPE_INT,
                            value=AttributeValue(
                                val_int=1,
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_EARLIEST_FRONTEND_SPAN_DURATION_MS,
                            type=AttributeKey.TYPE_INT,
                            value=AttributeValue(
                                val_int=152,
                            ),
                        ),
                    ],
                )
                for start_timestamp in reversed(sorted(trace_id_per_start_timestamp.keys()))
            ],
            page_token=PageToken(offset=len(_TRACE_IDS)),
            meta=ResponseMeta(request_id=_REQUEST_ID),
        )
        assert MessageToDict(response) == MessageToDict(expected_response)

    def test_with_data_and_aggregated_fields(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        three_hours_later = int((_BASE_TIME + timedelta(hours=3)).timestamp())
        (
            start_timestamp_per_trace_id,
            trace_id_per_start_timestamp,
        ) = generate_trace_id_timestamp_data(_SPANS)
        message = GetTracesRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=Timestamp(seconds=three_hours_later),
                request_id=_REQUEST_ID,
            ),
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_START_TIMESTAMP,
                    type=AttributeKey.TYPE_DOUBLE,
                ),
            ],
            filters=[
                GetTracesRequest.TraceFilter(
                    item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    filter=TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                name="sentry.op",
                                type=AttributeKey.TYPE_STRING,
                            ),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_str="db"),
                        ),
                    ),
                ),
            ],
        )
        response = EndpointGetTraces().execute(message)
        expected_response = GetTracesResponse(
            traces=[
                GetTracesResponse.Trace(
                    attributes=[
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_START_TIMESTAMP,
                            type=AttributeKey.TYPE_DOUBLE,
                            value=AttributeValue(
                                val_double=start_timestamp_per_trace_id[
                                    trace_id_per_start_timestamp[start_timestamp]
                                ],
                            ),
                        ),
                    ],
                )
                for start_timestamp in reversed(sorted(trace_id_per_start_timestamp.keys()))
            ],
            page_token=PageToken(offset=len(_TRACE_IDS)),
            meta=ResponseMeta(request_id=_REQUEST_ID),
        )
        assert MessageToDict(response) == MessageToDict(expected_response)

    def test_with_data_and_aggregated_fields_ignore_case(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        three_hours_later = int((_BASE_TIME + timedelta(hours=3)).timestamp())
        (
            start_timestamp_per_trace_id,
            trace_id_per_start_timestamp,
        ) = generate_trace_id_timestamp_data(_SPANS)
        message = GetTracesRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=Timestamp(seconds=three_hours_later),
                request_id=_REQUEST_ID,
            ),
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_START_TIMESTAMP,
                    type=AttributeKey.TYPE_DOUBLE,
                ),
            ],
            filters=[
                GetTracesRequest.TraceFilter(
                    item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    filter=TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                name="sentry.op",
                                type=AttributeKey.TYPE_STRING,
                            ),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_str="DB"),
                            ignore_case=True,
                        ),
                    ),
                ),
            ],
        )
        response = EndpointGetTraces().execute(message)
        expected_response = GetTracesResponse(
            traces=[
                GetTracesResponse.Trace(
                    attributes=[
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_START_TIMESTAMP,
                            type=AttributeKey.TYPE_DOUBLE,
                            value=AttributeValue(
                                val_double=start_timestamp_per_trace_id[
                                    trace_id_per_start_timestamp[start_timestamp]
                                ],
                            ),
                        ),
                    ],
                )
                for start_timestamp in reversed(sorted(trace_id_per_start_timestamp.keys()))
            ],
            page_token=PageToken(offset=len(_TRACE_IDS)),
            meta=ResponseMeta(request_id=_REQUEST_ID),
        )
        assert MessageToDict(response) == MessageToDict(expected_response)

    def test_with_data_and_aggregated_fields_ignore_case_on_non_strings_error(
        self, setup_teardown: Any
    ) -> None:
        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        three_hours_later = int((_BASE_TIME + timedelta(hours=3)).timestamp())
        message = GetTracesRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=Timestamp(seconds=three_hours_later),
                request_id=_REQUEST_ID,
            ),
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_START_TIMESTAMP,
                    type=AttributeKey.TYPE_DOUBLE,
                ),
            ],
            filters=[
                GetTracesRequest.TraceFilter(
                    item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    filter=TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                name="my.float.field",
                                type=AttributeKey.TYPE_DOUBLE,
                            ),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_double=0.123),
                            ignore_case=True,
                        ),
                    ),
                ),
            ],
        )
        with pytest.raises(
            BadSnubaRPCRequestException, match="Cannot ignore case on non-string values"
        ):
            EndpointGetTraces().execute(message)

    def test_with_data_and_cross_event_query(self) -> None:
        trace_ids, all_items, start_time, end_time = create_cross_item_test_data()
        write_cross_item_data_to_storage(all_items)

        filters = [
            trace_filter(
                comparison_filter("span.attr1", "val1"),
                TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            trace_filter(
                comparison_filter("log.attr2", "val2"),
                TraceItemType.TRACE_ITEM_TYPE_LOG,
            ),
            trace_filter(
                or_filter(
                    [
                        comparison_filter("error.attr3", "val3"),
                        comparison_filter("error.attr4", "val4"),
                    ]
                ),
                TraceItemType.TRACE_ITEM_TYPE_ERROR,
            ),
        ]

        message = GetTracesRequest(
            meta=create_request_meta(start_time, end_time),
            attributes=[
                TraceAttribute(key=TraceAttribute.Key.KEY_TRACE_ID),
            ],
            filters=filters,
        )

        response = EndpointGetTraces().execute(message)

        assert len(response.traces) == 3

        returned_trace_ids = set()
        for trace in response.traces:
            returned_trace_ids.add(trace.attributes[0].value.val_str)

        # Only the first 3 traces should match all filter conditions
        expected_trace_ids = set(trace_ids[:3])
        assert (
            returned_trace_ids == expected_trace_ids
        ), f"Expected {expected_trace_ids}, got {returned_trace_ids}"

    def test_cross_item_filtered_count_with_span_restriction(self) -> None:
        trace_ids, all_items, start_time, end_time = create_cross_item_test_data()
        write_cross_item_data_to_storage(all_items)

        filters = [
            trace_filter(
                comparison_filter("span.attr1", "val1"),
                TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            trace_filter(
                comparison_filter("log.attr2", "val2"),
                TraceItemType.TRACE_ITEM_TYPE_LOG,
            ),
        ]

        message = GetTracesRequest(
            meta=create_request_meta(start_time, end_time, TraceItemType.TRACE_ITEM_TYPE_SPAN),
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_FILTERED_ITEM_COUNT,
                    type=AttributeKey.TYPE_INT,
                ),
            ],
            filters=filters,
        )

        response = EndpointGetTraces().execute(message)

        assert len(response.traces) == 3
        for trace in response.traces:
            count_attr = trace.attributes[0]

            assert (
                count_attr.value.val_int == 1
            ), f"Expected count of 1 span per trace, got {count_attr.value.val_int}"

    def test_cross_item_filtered_count_without_restriction(self) -> None:
        trace_ids, all_items, start_time, end_time = create_cross_item_test_data()
        write_cross_item_data_to_storage(all_items)

        filters = [
            trace_filter(
                comparison_filter("span.attr1", "val1"),
                TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            trace_filter(
                comparison_filter("log.attr2", "val2"),
                TraceItemType.TRACE_ITEM_TYPE_LOG,
            ),
        ]

        # Request without meta type restriction - should count all matching items
        message = GetTracesRequest(
            meta=create_request_meta(start_time, end_time),
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_FILTERED_ITEM_COUNT,
                    type=AttributeKey.TYPE_INT,
                ),
            ],
            filters=filters,
        )

        response = EndpointGetTraces().execute(message)

        assert len(response.traces) == 3
        for trace in response.traces:
            count_attr = trace.attributes[0]
            assert (
                count_attr.value.val_int == 2
            ), f"Expected count of 2 items per trace (1 span + 1 log), got {count_attr.value.val_int}"

    def test_multiple_item_types_start_timestamp(self) -> None:
        trace_ids, all_items, start_time, end_time = create_cross_item_test_data()
        write_cross_item_data_to_storage(all_items)

        message = GetTracesRequest(
            meta=create_request_meta(start_time, end_time),
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_START_TIMESTAMP,
                    type=AttributeKey.TYPE_DOUBLE,
                ),
            ],
        )

        response = EndpointGetTraces().execute(message)

        assert len(response.traces) == 6
        for i, trace in enumerate(response.traces):
            # Traces are returned in descending order of start timestamp
            trace_index = 5 - i
            expected_timestamp = (start_time + timedelta(minutes=trace_index * 10)).timestamp()
            assert trace.attributes[0].value.val_double == expected_timestamp

    def test_default_start_timestamp(self) -> None:
        spans = [
            TraceItem(
                organization_id=1,
                project_id=1,
                item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                timestamp=Timestamp(seconds=int(_BASE_TIME.timestamp())),
                trace_id=uuid.uuid4().hex,
                item_id=uuid.uuid4().int.to_bytes(16, byteorder="little"),
                received=Timestamp(seconds=int(_BASE_TIME.timestamp())),
                retention_days=90,
                server_sample_rate=1.0,
                attributes={},
            ).SerializeToString()
        ]
        write_cross_item_data_to_storage(spans)

        message = GetTracesRequest(
            meta=create_request_meta(_BASE_TIME, _BASE_TIME + timedelta(hours=1)),
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_START_TIMESTAMP,
                    type=AttributeKey.TYPE_DOUBLE,
                ),
            ],
        )
        response = EndpointGetTraces().execute(message)

        assert len(response.traces) == 1
        assert response.traces[0].attributes[0].value.val_double == _BASE_TIME.timestamp()


def generate_spans(spans_data: list[bytes]) -> list[TraceItem]:
    spans: list[TraceItem] = []
    for payload in spans_data:
        span = TraceItem()
        span.ParseFromString(payload)
        spans.append(span)
    return spans


def generate_trace_id_timestamp_data(
    spans_data: list[bytes],
) -> tuple[dict[str, float], dict[float, str]]:
    start_timestamp_per_trace_id: dict[str, float] = defaultdict(lambda: 2 * 1e10)
    for payload in spans_data:
        s = TraceItem()
        s.ParseFromString(payload)
        start_timestamp_per_trace_id[s.trace_id] = min(
            start_timestamp_per_trace_id[s.trace_id],
            s.attributes["sentry.start_timestamp_precise"].double_value,
        )
    trace_id_per_start_timestamp: dict[float, str] = {
        timestamp: trace_id for trace_id, timestamp in start_timestamp_per_trace_id.items()
    }
    return start_timestamp_per_trace_id, trace_id_per_start_timestamp


def trace_filter(
    filter: TraceItemFilter,
    item_type: TraceItemType.ValueType,
) -> GetTracesRequest.TraceFilter:
    """Create a trace filter for the specified field and item type."""
    return GetTracesRequest.TraceFilter(
        item_type=item_type,
        filter=filter,
    )
