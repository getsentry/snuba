import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeNamesRequest,
    TraceItemAttributeNamesResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import PageToken, RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey
from sentry_protos.snuba.v1.trace_item_filter_pb2 import TraceItemFilter

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

# the number of spans generated as test data to be used by the tests
TOTAL_GENERATED_SPANS = 3
# number of attributes that are generated on each span, for each type
NUM_ATTR_PER_SPAN_PER_TYPE = 10

TOTAL_GENERATED_ATTR_PER_TYPE = TOTAL_GENERATED_SPANS * NUM_ATTR_PER_SPAN_PER_TYPE


def populate_eap_spans_storage(num_rows: int) -> None:
    """
    Fills the eap_spans storage with data for num_rows spans.
    Each span will have at least 10 unique tags.
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
        for i in range(
            id * NUM_ATTR_PER_SPAN_PER_TYPE,
            id * NUM_ATTR_PER_SPAN_PER_TYPE + NUM_ATTR_PER_SPAN_PER_TYPE,
        ):
            res["tags"][f"a_tag_{i:03}"] = "blah"  # type: ignore
            res["measurements"][f"b_measurement_{i:03}"] = {"value": 10}  # type: ignore
        return res

    spans_storage = get_storage(StorageKey("eap_spans"))
    messages = [generate_span_event_message(i) for i in range(num_rows)]
    write_raw_unprocessed_events(spans_storage, messages)  # type: ignore


@pytest.fixture(autouse=True)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    populate_eap_spans_storage(num_rows=TOTAL_GENERATED_SPANS)


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
            type=AttributeKey.Type.TYPE_STRING,
            value_substring_match="",
        )
        res = EndpointTraceItemAttributeNames().execute(req)

        expected = []
        for i in range(TOTAL_GENERATED_ATTR_PER_TYPE):
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
        assert res.attributes == expected

    def test_simple_float(self) -> None:
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
            type=AttributeKey.Type.TYPE_FLOAT,
            value_substring_match="",
        )
        res = EndpointTraceItemAttributeNames().execute(req)
        expected = []
        for i in range(TOTAL_GENERATED_ATTR_PER_TYPE):
            expected.append(
                TraceItemAttributeNamesResponse.Attribute(
                    name=f"b_measurement_{str(i).zfill(3)}",
                    type=AttributeKey.Type.TYPE_FLOAT,
                )
            )
        expected.append(
            TraceItemAttributeNamesResponse.Attribute(
                name="sentry.duration_ms", type=AttributeKey.Type.TYPE_FLOAT
            )
        )
        assert res.attributes == expected

    def test_with_filter(self) -> None:
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
            type=AttributeKey.Type.TYPE_STRING,
            value_substring_match="28",
        )
        res = EndpointTraceItemAttributeNames().execute(req)

        expected = [
            TraceItemAttributeNamesResponse.Attribute(
                name="a_tag_028", type=AttributeKey.Type.TYPE_STRING
            )
        ]
        assert res.attributes == expected

    def test_with_page_token(self) -> None:
        # this is all the expected attributes
        expected_attributes = []
        for i in range(TOTAL_GENERATED_ATTR_PER_TYPE):
            expected_attributes.append(
                TraceItemAttributeNamesResponse.Attribute(
                    name=f"a_tag_{str(i).zfill(3)}",
                    type=AttributeKey.Type.TYPE_STRING,
                )
            )
        expected_attributes += [
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
        # we just get the first 10
        limit = 10
        page_token = None
        while True:
            # and grab `limit` at a time until we get them all
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
                limit=limit,
                type=AttributeKey.Type.TYPE_STRING,
                value_substring_match="",
                page_token=page_token,
            )
            res = EndpointTraceItemAttributeNames().execute(req)
            page_token = res.page_token
            reslen = len(res.attributes)
            if reslen == 0:
                # we are done, we got everything
                break
            else:
                assert res.attributes == expected_attributes[:reslen]
                expected_attributes = expected_attributes[reslen:]
        assert expected_attributes == []

    def test_page_token_offset_filter(self) -> None:
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
            limit=10,
            type=AttributeKey.Type.TYPE_STRING,
            value_substring_match="",
            page_token=PageToken(filter_offset=TraceItemFilter()),
        )
        with pytest.raises(NotImplementedError):
            EndpointTraceItemAttributeNames().execute(req)

    def test_response_metadata(self) -> None:
        # debug must be true in RequestMeta for it to return query_info in the response
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
                debug=True,
            ),
            limit=1000,
            type=AttributeKey.Type.TYPE_STRING,
            value_substring_match="",
        )
        res = EndpointTraceItemAttributeNames().execute(req)
        assert res.meta.query_info != []
