import uuid

import pytest
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_delete_trace_items_pb2 import (
    DeleteTraceItemsRequest,
    DeleteTraceItemsResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, ResponseMeta

from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.endpoint_delete_trace_items import EndpointDeleteTraceItems
from tests.base import BaseApiTest

_REQUEST_ID = uuid.uuid4().hex


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
            # trace_id is intentionally omitted
        )

        with pytest.raises(BadSnubaRPCRequestException) as exc_info:
            EndpointDeleteTraceItems().execute(message)

        assert "trace_id is required for deleting a trace." in str(exc_info.value)

    def test_valid_trace_id_returns_success_response(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        trace_id = uuid.uuid4().hex
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
            trace_ids=[trace_id],
        )

        response = EndpointDeleteTraceItems().execute(message)

        expected_response = DeleteTraceItemsResponse(
            meta=ResponseMeta(request_id=_REQUEST_ID),
            matching_items_count=0,
        )

        assert MessageToDict(response) == MessageToDict(expected_response)
