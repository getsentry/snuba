import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeNamesRequest,
    TraceItemAttributeNamesResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_attribute_names import (
    EndpointTraceItemAttributeNames,
)
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events

BASE_TIME = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(
    hours=3
)


def populate_eap_spans_storage(num_rows: int) -> None:
    """
    Fills the eap_spans storage with num_rows rows of data
    """

    def generate_span_event_message(id: int) -> Mapping[str, Any]:
        res = {
            "description": "/api/0/relays/projectconfigs/",
            "duration_ms": 152,
            "event_id": "d826225de75d42d6b2f01b957d51f18f",
            "exclusive_time_ms": 0.228,
            "is_segment": True,
            "data": {},
            "measurements": {},
            "organization_id": 1,
            "origin": "auto.http.django",
            "project_id": 1,
            "received": 1721319572.877828,
            "retention_days": 90,
            "segment_id": "8873a98879faf06d",
            "sentry_tags": {
                "category": "http",
            },
            "span_id": uuid.uuid4().hex,
            "tags": {
                "http.status_code": "200",
            },
            "trace_id": uuid.uuid4().hex,
            "start_timestamp_ms": int(BASE_TIME.timestamp() * 1000),
            "start_timestamp_precise": BASE_TIME.timestamp(),
            "end_timestamp_precise": BASE_TIME.timestamp() + 1,
        }
        for i in range(id * 10, id * 10 + 10):
            res["tags"][f"a_tag_{i:03}"] = "blah"  # type: ignore
            res["measurements"][f"b_measurement_{i:03}"] = {"value": 10}  # type: ignore
        return res

    spans_storage = get_storage(StorageKey("eap_spans"))
    messages = [generate_span_event_message(i) for i in range(num_rows)]
    write_raw_unprocessed_events(spans_storage, messages)  # type: ignore


@pytest.fixture(autouse=True)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    populate_eap_spans_storage(num_rows=3)


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTraceItemAttributeNames(BaseApiTest):
    def test_basic(self) -> None:
        req = TraceItemAttributeNamesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(
                    seconds=int((BASE_TIME - timedelta(days=1)).timestamp())
                ),
                end_timestamp=Timestamp(
                    seconds=int((BASE_TIME + timedelta(days=1)).timestamp())
                ),
            ),
            limit=1000,
            offset=0,
            type=AttributeKey.Type.TYPE_STRING,
            value_substring_match="",
        )
        res = EndpointTraceItemAttributeNames().execute(req)

        expected = []
        for i in range(30):
            expected.append(
                TraceItemAttributeNamesResponse.Attribute(
                    name=f"a_tag_{str(i).zfill(3)}", type=AttributeKey.Type.TYPE_STRING
                )
            )
        expected += [
            TraceItemAttributeNamesResponse.Attribute(
                name="http.status_code", type=AttributeKey.Type.TYPE_STRING
            ),
            TraceItemAttributeNamesResponse.Attribute(
                name="sentry.category", type=AttributeKey.Type.TYPE_STRING
            ),
            TraceItemAttributeNamesResponse.Attribute(
                name="sentry.name", type=AttributeKey.Type.TYPE_STRING
            ),
            TraceItemAttributeNamesResponse.Attribute(
                name="sentry.segment_name", type=AttributeKey.Type.TYPE_STRING
            ),
            TraceItemAttributeNamesResponse.Attribute(
                name="sentry.service", type=AttributeKey.Type.TYPE_STRING
            ),
        ]
        assert res == TraceItemAttributeNamesResponse(attributes=expected)
