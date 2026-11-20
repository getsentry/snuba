from datetime import timedelta
from typing import Any, cast

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_options.testing import override_options
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import TraceItemAttributeNamesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.query.allocation_policies import ALLOCATION_POLICY_KEY
from snuba.web.rpc.v1.endpoint_trace_item_attribute_names import EndpointTraceItemAttributeNames
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.web.rpc.v1.test_endpoint_trace_item_attribute_names import (  # noqa: F401
    BASE_TIME,
    co_occurring_storage,
    setup_teardown,
)

# One query at a time per tenant, enforced and never pardoned, so a second request
# only runs if the first one released its concurrency slot.
_ONE_AT_A_TIME: dict[str, Any] = {
    "EAP": [
        {
            "match": {},
            "policies": [
                {
                    "name": "ConcurrentRateLimitAllocationPolicy",
                    "concurrent_limit": 1,
                    "is_enforced": 1,
                    "is_pardonable": 0,
                }
            ],
        }
    ]
}


def _meta() -> RequestMeta:
    return RequestMeta(
        project_ids=[1],
        organization_id=1,
        cogs_category="something",
        referrer="something",
        start_timestamp=Timestamp(seconds=int((BASE_TIME - timedelta(days=1)).timestamp())),
        end_timestamp=Timestamp(seconds=int((BASE_TIME + timedelta(days=1)).timestamp())),
        trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
    )


@pytest.mark.eap
@pytest.mark.redis_db
def test_trace_item_table_releases_its_slot() -> None:
    request = TraceItemTableRequest(
        meta=_meta(),
        columns=[Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.name"))],
        limit=10,
    )
    with override_options("snuba", {ALLOCATION_POLICY_KEY: cast(Any, _ONE_AT_A_TIME)}):
        EndpointTraceItemTable().execute(request)
        EndpointTraceItemTable().execute(request)


@pytest.mark.eap
@pytest.mark.redis_db
def test_trace_item_attribute_names_releases_its_slot() -> None:
    request = TraceItemAttributeNamesRequest(
        meta=_meta(), limit=10, type=AttributeKey.Type.TYPE_STRING
    )
    with override_options("snuba", {ALLOCATION_POLICY_KEY: cast(Any, _ONE_AT_A_TIME)}):
        EndpointTraceItemAttributeNames().execute(request)
        EndpointTraceItemAttributeNames().execute(request)
