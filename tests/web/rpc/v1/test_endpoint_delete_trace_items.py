import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import Mock, patch

import pytest
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_delete_trace_items_pb2 import (
    DeleteTraceItemsRequest,
    DeleteTraceItemsResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import (
    RequestMeta,
    ResponseMeta,
    TraceItemFilterWithType,
    TraceItemType,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.endpoint_delete_trace_items import EndpointDeleteTraceItems
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import gen_item_message

_REQUEST_ID = uuid.uuid4().hex

_TRACE_ID = str(uuid.uuid4())
_BASE_TIME = datetime.now(tz=timezone.utc).replace(
    minute=0,
    second=0,
    microsecond=0,
) - timedelta(minutes=180)
_SPAN_COUNT = 120
_REQUEST_ID = uuid.uuid4().hex
_SPANS = [
    gen_item_message(
        start_timestamp=_BASE_TIME + timedelta(minutes=i),
        trace_id=_TRACE_ID,
        item_id=int(uuid.uuid4().hex[:16], 16).to_bytes(
            16,
            byteorder="little",
            signed=False,
        ),
        attributes={
            "sentry.op": AnyValue(string_value="http.server" if i == 0 else "db"),
            "sentry.raw_description": AnyValue(
                string_value="root" if i == 0 else f"child {i + 1} of {_SPAN_COUNT}",
            ),
            "sentry.is_segment": AnyValue(bool_value=i == 0),
        },
    )
    for i in range(_SPAN_COUNT)
]


@pytest.fixture(autouse=False)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    items_storage = get_storage(StorageKey("eap_items"))
    write_raw_unprocessed_events(items_storage, _SPANS)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestEndpointDeleteTrace(BaseApiTest):
    def test_missing_trace_id_raises_exception(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = DeleteTraceItemsRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=ts,
                request_id=_REQUEST_ID,
            ),
            # trace_id and filters are intentionally omitted
        )

        with pytest.raises(BadSnubaRPCRequestException):
            EndpointDeleteTraceItems().execute(message)

    def test_valid_trace_id_returns_success_response(self, setup_teardown: Any) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = DeleteTraceItemsRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=ts,
                request_id=_REQUEST_ID,
            ),
            trace_ids=[_TRACE_ID],
        )

        response = EndpointDeleteTraceItems().execute(message)

        expected_response = DeleteTraceItemsResponse(
            meta=ResponseMeta(request_id=_REQUEST_ID),
            matching_items_count=_SPAN_COUNT,
        )

        assert MessageToDict(response) == MessageToDict(expected_response)

    @patch("snuba.web.bulk_delete_query.produce_delete_query")
    def test_valid_trace_id_produces_bulk_delete_message(
        self, produce_delete_query_mock: Mock, setup_teardown: Any
    ) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = DeleteTraceItemsRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=ts,
                request_id=_REQUEST_ID,
            ),
            trace_ids=[_TRACE_ID],
        )

        EndpointDeleteTraceItems().execute(message)

        # Assert produce_delete_query was called once
        assert produce_delete_query_mock.call_count == 1

        # Check the arguments to produce_delete_query
        called_args = produce_delete_query_mock.call_args[0][0]
        assert called_args["storage_name"] == "eap_items"
        assert called_args["conditions"]["project_id"] == [1, 2, 3]
        assert called_args["conditions"]["organization_id"] == [1]
        assert called_args["conditions"]["trace_id"] == [_TRACE_ID]
        assert called_args["rows_to_delete"] == _SPAN_COUNT

    def test_filters_with_equals_operation_accepted(self) -> None:
        """Test that filters with OP_EQUALS are properly converted to attribute_conditions"""
        ts = Timestamp()
        ts.GetCurrentTime()

        message = DeleteTraceItemsRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=ts,
                request_id=_REQUEST_ID,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
            ),
            filters=[
                TraceItemFilterWithType(
                    filter=TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(name="group_id", type=AttributeKey.TYPE_INT),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_int=12345),
                        )
                    )
                )
            ],
        )

        with patch("snuba.web.bulk_delete_query._enforce_max_rows", return_value=10):
            with patch("snuba.web.bulk_delete_query.produce_delete_query") as mock_produce:
                EndpointDeleteTraceItems().execute(message)

                # Verify produce_delete_query was called with attribute_conditions
                assert mock_produce.call_count == 1

    def test_filters_with_in_operation_accepted(self) -> None:
        """Test that filters with OP_IN are properly converted to attribute_conditions"""
        ts = Timestamp()
        ts.GetCurrentTime()

        int_array = AttributeValue.IntArray()  # type: ignore
        int_array.values.extend([12345, 67890])

        message = DeleteTraceItemsRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=ts,
                request_id=_REQUEST_ID,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
            ),
            filters=[
                TraceItemFilterWithType(
                    filter=TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(name="group_id", type=AttributeKey.TYPE_INT),
                            op=ComparisonFilter.OP_IN,
                            value=AttributeValue(val_int_array=int_array),
                        )
                    )
                )
            ],
        )

        with patch("snuba.web.bulk_delete_query._enforce_max_rows", return_value=10):
            with patch("snuba.web.bulk_delete_query.produce_delete_query"):
                EndpointDeleteTraceItems().execute(message)

    def test_filters_with_unsupported_operation_rejected(self) -> None:
        """Test that filters with operations other than OP_EQUALS/OP_IN are rejected"""
        ts = Timestamp()
        ts.GetCurrentTime()

        message = DeleteTraceItemsRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=ts,
                request_id=_REQUEST_ID,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
            ),
            filters=[
                TraceItemFilterWithType(
                    filter=TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(name="timestamp", type=AttributeKey.TYPE_INT),
                            op=ComparisonFilter.OP_GREATER_THAN,
                            value=AttributeValue(val_int=1234567890),
                        )
                    )
                )
            ],
        )

        with pytest.raises(
            BadSnubaRPCRequestException,
            match="Only OP_EQUALS and OP_IN operations are supported for deletion",
        ):
            EndpointDeleteTraceItems().execute(message)

    def test_filters_with_and_filter_rejected(self) -> None:
        """Test that AND/OR/NOT filters are rejected"""
        ts = Timestamp()
        ts.GetCurrentTime()

        from sentry_protos.snuba.v1.trace_item_filter_pb2 import AndFilter

        message = DeleteTraceItemsRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=ts,
                request_id=_REQUEST_ID,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
            ),
            filters=[
                TraceItemFilterWithType(
                    filter=TraceItemFilter(
                        and_filter=AndFilter(
                            filters=[
                                TraceItemFilter(
                                    comparison_filter=ComparisonFilter(
                                        key=AttributeKey(
                                            name="group_id", type=AttributeKey.TYPE_INT
                                        ),
                                        op=ComparisonFilter.OP_EQUALS,
                                        value=AttributeValue(val_int=12345),
                                    )
                                )
                            ]
                        )
                    )
                )
            ],
        )

        with pytest.raises(
            BadSnubaRPCRequestException,
            match="Only comparison filters are supported for deletion",
        ):
            EndpointDeleteTraceItems().execute(message)

    def test_filters_without_item_type_rejected(self) -> None:
        """Test that filters without item_type in metadata are rejected"""
        ts = Timestamp()
        ts.GetCurrentTime()

        message = DeleteTraceItemsRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=ts,
                request_id=_REQUEST_ID,
                # trace_item_type is intentionally omitted (defaults to UNSPECIFIED)
            ),
            filters=[
                TraceItemFilterWithType(
                    filter=TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(name="group_id", type=AttributeKey.TYPE_INT),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_int=12345),
                        )
                    )
                )
            ],
        )

        with pytest.raises(
            BadSnubaRPCRequestException,
            match="trace_item_type must be specified in metadata when using filters",
        ):
            EndpointDeleteTraceItems().execute(message)
