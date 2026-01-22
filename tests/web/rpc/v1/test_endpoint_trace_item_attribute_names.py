from datetime import UTC, datetime, timedelta

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeNamesRequest,
    TraceItemAttributeNamesResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey
from sentry_protos.snuba.v1.trace_item_filter_pb2 import ExistsFilter, TraceItemFilter
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_attribute_names import (
    EndpointTraceItemAttributeNames,
)
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import gen_item_message

BASE_TIME = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=3)

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

    def generate_span_event_message(id: int) -> bytes:
        attributes = {
            "bar": AnyValue(string_value="some"),
            "baz": AnyValue(string_value="some"),
            "foo": AnyValue(string_value="some"),
            "sentry.name": AnyValue(string_value="some"),
            "sentry.segment_name": AnyValue(string_value="some"),
            "sentry.service": AnyValue(string_value="some"),
        }
        for i in range(
            id * NUM_ATTR_PER_SPAN_PER_TYPE,
            id * NUM_ATTR_PER_SPAN_PER_TYPE + NUM_ATTR_PER_SPAN_PER_TYPE,
        ):
            attributes[f"a_tag_{i:03}"] = AnyValue(string_value="blah")
            attributes[f"c_tag_{i:03}"] = AnyValue(string_value="blah")
            attributes[f"b_measurement_{i:03}"] = AnyValue(double_value=10)
            attributes[f"d_bool_{i:03}"] = AnyValue(bool_value=i % 2 == 0)
        return gen_item_message(
            start_timestamp=BASE_TIME + timedelta(minutes=id),
            attributes=attributes,
        )

    items_storage = get_storage(StorageKey("eap_items"))
    messages = [generate_span_event_message(i) for i in range(num_rows)]
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
                start_timestamp=Timestamp(seconds=int((BASE_TIME - timedelta(days=1)).timestamp())),
                end_timestamp=Timestamp(seconds=int((BASE_TIME + timedelta(days=1)).timestamp())),
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
                start_timestamp=Timestamp(seconds=int((BASE_TIME - timedelta(days=1)).timestamp())),
                end_timestamp=Timestamp(seconds=int((BASE_TIME + timedelta(days=1)).timestamp())),
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
                start_timestamp=Timestamp(seconds=int((BASE_TIME - timedelta(days=1)).timestamp())),
                end_timestamp=Timestamp(seconds=int((BASE_TIME + timedelta(days=1)).timestamp())),
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
                start_timestamp=Timestamp(seconds=int((BASE_TIME - timedelta(days=1)).timestamp())),
                end_timestamp=Timestamp(seconds=int((BASE_TIME + timedelta(days=1)).timestamp())),
            ),
            limit=TOTAL_GENERATED_ATTR_PER_TYPE,
            type=AttributeKey.Type.TYPE_STRING,
            value_substring_match="28",
        )
        res = EndpointTraceItemAttributeNames().execute(req)

        expected = [
            TraceItemAttributeNamesResponse.Attribute(
                name="a_tag_028", type=AttributeKey.Type.TYPE_STRING
            ),
            TraceItemAttributeNamesResponse.Attribute(
                name="c_tag_028", type=AttributeKey.Type.TYPE_STRING
            ),
        ]
        assert res.attributes == expected

    def test_empty_results(self) -> None:
        req = TraceItemAttributeNamesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int((BASE_TIME - timedelta(days=1)).timestamp())),
                end_timestamp=Timestamp(seconds=int((BASE_TIME + timedelta(days=1)).timestamp())),
            ),
            type=AttributeKey.Type.TYPE_STRING,
            value_substring_match="this_definitely_doesnt_exist_93710",
        )
        res = EndpointTraceItemAttributeNames().execute(req)
        assert res.attributes == []

    def test_response_metadata(self) -> None:
        # debug must be true in RequestMeta for it to return query_info in the response
        req = TraceItemAttributeNamesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int((BASE_TIME - timedelta(days=1)).timestamp())),
                end_timestamp=Timestamp(seconds=int((BASE_TIME + timedelta(days=1)).timestamp())),
                debug=True,
            ),
            limit=1000,
            type=AttributeKey.Type.TYPE_STRING,
            value_substring_match="",
        )
        res = EndpointTraceItemAttributeNames().execute(req)
        assert res.meta.query_info != []

    def test_basic_co_occurring_attrs(self) -> None:
        req = TraceItemAttributeNamesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int((BASE_TIME - timedelta(days=1)).timestamp())),
                end_timestamp=Timestamp(seconds=int((BASE_TIME + timedelta(days=1)).timestamp())),
            ),
            limit=TOTAL_GENERATED_ATTR_PER_TYPE,
            intersecting_attributes_filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="a_tag_000")
                )
            ),
            value_substring_match="000",
            type=AttributeKey.Type.TYPE_STRING,
        )
        res = EndpointTraceItemAttributeNames().execute(req)
        expected = [
            TraceItemAttributeNamesResponse.Attribute(
                name="a_tag_000", type=AttributeKey.Type.TYPE_STRING
            ),
            TraceItemAttributeNamesResponse.Attribute(
                name="c_tag_000", type=AttributeKey.Type.TYPE_STRING
            ),
        ]
        assert res.attributes == expected

    def test_simple_boolean(self) -> None:
        req = TraceItemAttributeNamesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int((BASE_TIME - timedelta(days=1)).timestamp())),
                end_timestamp=Timestamp(seconds=int((BASE_TIME + timedelta(days=1)).timestamp())),
            ),
            limit=TOTAL_GENERATED_ATTR_PER_TYPE,
            type=AttributeKey.Type.TYPE_BOOLEAN,
            value_substring_match="d_bool",
        )
        res = EndpointTraceItemAttributeNames().execute(req)
        expected = []
        for i in range(TOTAL_GENERATED_ATTR_PER_TYPE):
            expected.append(
                TraceItemAttributeNamesResponse.Attribute(
                    name=f"d_bool_{str(i).zfill(3)}",
                    type=AttributeKey.Type.TYPE_BOOLEAN,
                )
            )
        assert res.attributes == expected
