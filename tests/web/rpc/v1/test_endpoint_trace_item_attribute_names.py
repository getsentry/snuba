import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeNamesRequest,
    TraceItemAttributeNamesResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import (
    PageToken,
    RequestMeta,
    TraceItemType,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_attribute_names import (
    EndpointTraceItemAttributeNames,
)
from tests.base import BaseApiTest
from tests.conftest import SnubaSetConfig
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
    items_storage = get_storage(StorageKey("eap_items"))
    messages = [generate_span_event_message(i) for i in range(num_rows)]
    write_raw_unprocessed_events(spans_storage, messages)  # type: ignore
    write_raw_unprocessed_events(items_storage, messages)  # type: ignore


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
            limit=TOTAL_GENERATED_ATTR_PER_TYPE,
            type=AttributeKey.Type.TYPE_STRING,
            value_substring_match="a_tag",
        )
        res = EndpointTraceItemAttributeNames().execute(req)
        expected = []
        for i in range(TOTAL_GENERATED_ATTR_PER_TYPE):
            expected.append(
                TraceItemAttributeNamesResponse.Attribute(
                    name=f"a_tag_{str(i).zfill(3)}", type=AttributeKey.Type.TYPE_STRING
                )
            )
        assert res.attributes == expected

    def test_simple_float_backward_compat(self) -> None:
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
            limit=TOTAL_GENERATED_ATTR_PER_TYPE,
            type=AttributeKey.Type.TYPE_FLOAT,
            value_substring_match="b_mea",
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
        assert res.attributes == expected

    def test_simple_double(self) -> None:
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
            limit=TOTAL_GENERATED_ATTR_PER_TYPE,
            type=AttributeKey.Type.TYPE_DOUBLE,
            value_substring_match="b_mea",
        )
        res = EndpointTraceItemAttributeNames().execute(req)
        expected = []
        for i in range(TOTAL_GENERATED_ATTR_PER_TYPE):
            expected.append(
                TraceItemAttributeNamesResponse.Attribute(
                    name=f"b_measurement_{str(i).zfill(3)}",
                    type=AttributeKey.Type.TYPE_DOUBLE,
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
            limit=TOTAL_GENERATED_ATTR_PER_TYPE,
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

    def test_with_page_token_offset(self) -> None:
        # this is all the expected attributes
        expected_attributes = []
        for i in range(TOTAL_GENERATED_ATTR_PER_TYPE):
            expected_attributes.append(
                TraceItemAttributeNamesResponse.Attribute(
                    name=f"a_tag_{str(i).zfill(3)}",
                    type=AttributeKey.Type.TYPE_STRING,
                )
            )
        # grab 10 at a time until we get them all
        done = 0
        page_token = PageToken(offset=0)
        at_a_time = 10
        while done < TOTAL_GENERATED_ATTR_PER_TYPE:
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
                limit=at_a_time,
                type=AttributeKey.Type.TYPE_STRING,
                value_substring_match="",
                page_token=page_token,
            )
            res = EndpointTraceItemAttributeNames().execute(req)
            page_token = res.page_token
            assert res.attributes == expected_attributes[:at_a_time]
            expected_attributes = expected_attributes[at_a_time:]
            done += at_a_time
        assert expected_attributes == []

    def test_empty_results(self) -> None:
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
            type=AttributeKey.Type.TYPE_STRING,
            value_substring_match="this_definitely_doesnt_exist_93710",
        )
        res = EndpointTraceItemAttributeNames().execute(req)
        assert res.attributes == []

    def test_page_token_offset_filter(self) -> None:

        expected_attributes = []
        for i in range(TOTAL_GENERATED_ATTR_PER_TYPE):
            expected_attributes.append(
                TraceItemAttributeNamesResponse.Attribute(
                    name=f"a_tag_{str(i).zfill(3)}",
                    type=AttributeKey.Type.TYPE_STRING,
                )
            )
        # grab 10 at a time until we get them all
        done = 0
        page_token = None
        at_a_time = 10

        while done < TOTAL_GENERATED_ATTR_PER_TYPE:
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
                limit=at_a_time,
                type=AttributeKey.Type.TYPE_STRING,
                value_substring_match="",
                page_token=page_token,
            )
            res = EndpointTraceItemAttributeNames().execute(req)
            page_token = res.page_token
            assert res.page_token.WhichOneof("value") == "filter_offset"
            assert res.attributes == expected_attributes[:at_a_time]
            expected_attributes = expected_attributes[at_a_time:]
            done += at_a_time
        assert expected_attributes == []

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

    def test_backwards_compat_names(self) -> None:
        PROJECT_ID = 4555754011230209
        ORGANIZATION_ID = 4555754011164672
        true = True

        spans_storage = get_storage(StorageKey("eap_spans"))
        items_storage = get_storage(StorageKey("eap_items"))
        messages = [
            {
                "project_id": PROJECT_ID,
                "organization_id": ORGANIZATION_ID,
                "span_id": "9d1a44cc6388423d",
                "trace_id": "4708357b20c041f39e40848e2980947b",
                "duration_ms": 100,
                "start_timestamp_precise": 1742410542.0,
                "end_timestamp_precise": 1742410542.1,
                "exclusive_time_ms": 100,
                "is_segment": True,
                "received": 1742411142.980593,
                "start_timestamp_ms": 1742410542000,
                "sentry_tags": {"transaction": "foo"},
                "retention_days": 90,
                "tags": {"foo": "foo"},
                "event_id": "654cfc4376f84645a70d889fbe9284a0",
                "segment_id": "654cfc4376f84645",
                "ingest_in_eap": True,
            },
            {
                "project_id": PROJECT_ID,
                "organization_id": ORGANIZATION_ID,
                "span_id": "b91705f800054f21",
                "trace_id": "d3bf3091daf84eb395f704a47b11f83c",
                "duration_ms": 100,
                "start_timestamp_precise": 1742410543.0,
                "end_timestamp_precise": 1742410543.1,
                "exclusive_time_ms": 100,
                "is_segment": true,
                "received": 1742411143.021623,
                "start_timestamp_ms": 1742410543000,
                "sentry_tags": {"transaction": "foo"},
                "retention_days": 90,
                "tags": {"bar": "bar"},
                "event_id": "8af9dc00313a45f4b0e09d755c56b353",
                "segment_id": "8af9dc00313a45f4",
                "ingest_in_eap": true,
            },
            {
                "project_id": PROJECT_ID,
                "organization_id": ORGANIZATION_ID,
                "span_id": "49ab2a01ceea41c6",
                "trace_id": "2e59d7af13994adfb11f05705bd1f81c",
                "duration_ms": 100,
                "start_timestamp_precise": 1742410543.0,
                "end_timestamp_precise": 1742410543.1,
                "exclusive_time_ms": 100,
                "is_segment": true,
                "received": 1742411143.060675,
                "start_timestamp_ms": 1742410543000,
                "sentry_tags": {"transaction": "foo"},
                "retention_days": 90,
                "tags": {"baz": "baz"},
                "event_id": "a1002b1d7458424ca523efdc53e90637",
                "segment_id": "a1002b1d7458424c",
                "ingest_in_eap": true,
            },
        ]

        write_raw_unprocessed_events(spans_storage, messages)  # type: ignore
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

        req = TraceItemAttributeNamesRequest(
            meta=RequestMeta(
                project_ids=[PROJECT_ID],
                organization_id=ORGANIZATION_ID,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(
                    seconds=int((BASE_TIME - timedelta(days=1)).timestamp())
                ),
                end_timestamp=Timestamp(
                    seconds=int((BASE_TIME + timedelta(days=1)).timestamp())
                ),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            limit=1000,
            type=AttributeKey.Type.TYPE_STRING,
        )
        res = EndpointTraceItemAttributeNames().execute(req)
        expected = [
            TraceItemAttributeNamesResponse.Attribute(
                name=attr_name, type=AttributeKey.Type.TYPE_STRING
            )
            for attr_name in [
                "bar",
                "baz",
                "foo",
                "sentry.name",
                "sentry.segment_name",
                "sentry.service",
            ]
        ]
        assert res.attributes == expected


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTraceItemAttributeNamesEAPItems(TestTraceItemAttributeNames):
    @pytest.fixture(autouse=True)
    def use_eap_items_table(
        self, snuba_set_config: SnubaSetConfig, redis_db: None
    ) -> None:
        snuba_set_config("use_eap_items_attrs_table_start_timestamp_seconds", 0)
        snuba_set_config("use_eap_items_attrs_table_all", True)

    def test_with_page_token_offset(self) -> None:
        pass

    def test_page_token_offset_filter(self) -> None:
        pass
