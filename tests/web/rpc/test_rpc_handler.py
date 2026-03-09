from unittest import mock

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeNamesRequest,
    TraceItemAttributeNamesResponse,
)
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.web import QueryException
from snuba.web.rpc import run_rpc_handler
from snuba.web.rpc.common.exceptions import RPCRequestException
from tests.base import BaseApiTest


def test_rpc_handler_bad_request() -> None:
    resp = run_rpc_handler("EndpointTraceItemAttributeNames", "v1", b"invalid-proto-data")
    assert isinstance(resp, ErrorProto)
    assert resp.code == 400


def test_rpc_handler_not_found() -> None:
    resp = run_rpc_handler("SomeWeirdRequest", "v1", b"invalid-proto-data")
    assert isinstance(resp, ErrorProto)
    assert resp.code == 404


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_basic() -> None:
    message = TraceItemAttributeNamesRequest(
        meta=RequestMeta(
            project_ids=[1, 2, 3],
            organization_id=1,
            cogs_category="something",
            referrer="something",
            start_timestamp=Timestamp(seconds=0),
            end_timestamp=Timestamp(seconds=100),
        ),
        type=AttributeKey.Type.TYPE_STRING,
        limit=10,
        offset=20,
    )

    resp = run_rpc_handler("EndpointTraceItemAttributeNames", "v1", message.SerializeToString())
    assert isinstance(resp, TraceItemAttributeNamesResponse)


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
@pytest.mark.parametrize(
    "expected_status_code, error",
    [
        pytest.param(
            502,
            RPCRequestException(status_code=502, message="blah"),
            id="specified_status_code",
        ),
        pytest.param(429, QueryException(exception_type="RateLimitExceeded"), id="rate_limit"),
        pytest.param(500, QueryException(exception_type="AttributeError"), id="arbitrary_error"),
    ],
)
def test_internal_error(expected_status_code: int, error: Exception) -> None:
    message = TraceItemAttributeNamesRequest(
        meta=RequestMeta(
            project_ids=[1, 2, 3],
            organization_id=1,
            cogs_category="something",
            referrer="something",
            start_timestamp=Timestamp(seconds=0),
            end_timestamp=Timestamp(seconds=100),
        ),
        type=AttributeKey.Type.TYPE_STRING,
        limit=10,
        offset=20,
    )

    with mock.patch(
        "snuba.web.rpc.v1.endpoint_trace_item_attribute_names.EndpointTraceItemAttributeNames._execute"
    ) as patch:
        patch.side_effect = error
        resp = run_rpc_handler("EndpointTraceItemAttributeNames", "v1", message.SerializeToString())
        assert isinstance(resp, ErrorProto)
        assert resp.code == expected_status_code


class TestAPIFailures(BaseApiTest):
    def test_unknown_rpc(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        response = self.app.post("/rpc/bloop/shmoop", data=b"asdasdasd")
        assert response.status_code == 404

    def test_bad_serialization(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        response = self.app.post("/rpc/EndpointTraceItemTable/v1", data=b"11111asdasdasd")
        assert response.status_code == 400
