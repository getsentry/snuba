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
def setup_teardown(eap: None, redis_db: None) -> None:
    populate_eap_spans_storage(num_rows=TOTAL_GENERATED_SPANS)


@pytest.mark.eap
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

    def test_default_order_is_alphabetical(self) -> None:
        """Without order_by, the endpoint keeps its historical name-ascending order
        regardless of frequency, so existing consumers are unaffected."""
        req = TraceItemAttributeNamesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int((BASE_TIME - timedelta(days=1)).timestamp())),
                end_timestamp=Timestamp(seconds=int((BASE_TIME + timedelta(days=1)).timestamp())),
            ),
            limit=1000,
            type=AttributeKey.Type.TYPE_STRING,
        )
        res = EndpointTraceItemAttributeNames().execute(req)
        attr_names = [attr.name for attr in res.attributes]

        # Fully alphabetical: the high-frequency "foo" does NOT jump ahead of "a_tag_000".
        assert attr_names == sorted(attr_names)
        assert attr_names.index("a_tag_000") < attr_names.index("foo")
        # No counts are returned on the default path.
        assert all(not attr.HasField("count") for attr in res.attributes)

    def test_order_by_count_desc(self) -> None:
        """order_by COLUMN_COUNT descending returns the most frequent keys first
        (name ascending as a tiebreaker) and populates counts in the response."""
        req = TraceItemAttributeNamesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int((BASE_TIME - timedelta(days=1)).timestamp())),
                end_timestamp=Timestamp(seconds=int((BASE_TIME + timedelta(days=1)).timestamp())),
            ),
            limit=1000,
            type=AttributeKey.Type.TYPE_STRING,
            order_by=TraceItemAttributeNamesRequest.OrderBy(
                column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_COUNT,
                descending=True,
            ),
        )
        res = EndpointTraceItemAttributeNames().execute(req)
        attr_names = [attr.name for attr in res.attributes]

        # "foo"/"bar"/"baz" occur on every span (frequency 3) while each "a_tag_*"
        # occurs once, so the common attributes must sort ahead of the rare ones.
        for common in ("foo", "bar", "baz"):
            assert attr_names.index(common) < attr_names.index("a_tag_000"), (
                f"high-frequency '{common}' should sort before low-frequency 'a_tag_000'"
            )

        # Counts are populated and reflect relative frequency. The value is a
        # co-occurring-row count (approximate; exact per-item counts come with the
        # v2 storage follow-up), so assert the relationship, not an exact value.
        counts = {attr.name: attr.count for attr in res.attributes if attr.HasField("count")}
        assert counts["foo"] > counts["a_tag_000"]
        assert counts["a_tag_000"] >= 1

        # Equal-frequency keys tie-break alphabetically.
        a_tags = [name for name in attr_names if name.startswith("a_tag_")]
        assert a_tags == sorted(a_tags)

    def test_order_by_count_equal_frequency_tiebreak(self) -> None:
        """Under count ordering, keys with equal frequency tie-break alphabetically."""
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
            order_by=TraceItemAttributeNamesRequest.OrderBy(
                column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_COUNT,
                descending=True,
            ),
        )
        res = EndpointTraceItemAttributeNames().execute(req)
        attr_names = [attr.name for attr in res.attributes]
        assert attr_names == [
            f"a_tag_{str(i).zfill(3)}" for i in range(TOTAL_GENERATED_ATTR_PER_TYPE)
        ]

    def test_order_by_count_pins_non_stored_first_both_directions(self) -> None:
        """Synthetic non-stored attributes (e.g. sentry.service) are pinned first under
        count ordering regardless of direction (regression: ascending placed them last)."""
        for descending in (True, False):
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
                order_by=TraceItemAttributeNamesRequest.OrderBy(
                    column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_COUNT,
                    descending=descending,
                ),
            )
            res = EndpointTraceItemAttributeNames().execute(req)
            attr_names = [attr.name for attr in res.attributes]
            assert attr_names[0] == "sentry.service", (
                f"non-stored attr should be first when descending={descending}, got {attr_names[:3]}"
            )
