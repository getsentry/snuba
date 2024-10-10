from unittest import mock

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1alpha.endpoint_tags_list_pb2 import (
    TraceItemAttributesRequest as TraceItemAttributesRequestProto,
)
from sentry_protos.snuba.v1alpha.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1alpha.trace_item_attribute_pb2 import AttributeKey

from snuba.web import QueryException
from snuba.web.rpc import RPCRequestException, run_rpc_handler


def test_rpc_handler_bad_request() -> None:
    resp = run_rpc_handler(
        "TraceItemAttributesRequest", "v1alpha", b"invalid-proto-data"
    )
    assert resp.status_code == 400

    err = ErrorProto()
    err.ParseFromString(resp.data)
    assert err.code == 400


def test_rpc_handler_not_found() -> None:
    resp = run_rpc_handler("SomeWeirdRequest", "v1", b"invalid-proto-data")
    assert resp.status_code == 404

    err = ErrorProto()
    err.ParseFromString(resp.data)
    assert err.code == 404


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_basic() -> None:
    message = TraceItemAttributesRequestProto(
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

    resp = run_rpc_handler(
        "TraceItemAttributesRequest", "v1alpha", message.SerializeToString()
    )
    assert resp.status_code == 200


@pytest.mark.parametrize(
    "expected_status_code, error",
    [
        pytest.param(
            502,
            RPCRequestException(status_code=502, message="blah"),
            id="specified_status_code",
        ),
        pytest.param(
            429, QueryException(exception_type="RateLimitExceeded"), id="rate_limit"
        ),
        pytest.param(
            500, QueryException(exception_type="AttributeError"), id="arbitrary_error"
        ),
    ],
)
def test_internal_error(expected_status_code, error) -> None:
    message = TraceItemAttributesRequestProto(
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
        "snuba.web.rpc.v1alpha.trace_item_attribute_list.trace_item_attribute_list_query"
    ) as patch:
        patch.side_effect = error
        resp = run_rpc_handler(
            "TraceItemAttributesRequest", "v1alpha", message.SerializeToString()
        )
        assert resp.status_code == expected_status_code
