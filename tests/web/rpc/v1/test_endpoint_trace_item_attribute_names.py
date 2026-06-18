import uuid
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
from snuba.query.expressions import FunctionCall, Lambda, Literal
from snuba.web.rpc.v1.endpoint_trace_item_attribute_names import (
    UNSEARCHABLE_ATTRIBUTE_KEYS,
    EndpointTraceItemAttributeNames,
    get_co_occurring_attributes,
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

    def test_co_occurring_attrs_excludes_unsearchable_keys_without_in_set(self) -> None:
        """Regression guard for SNUBA-B82 (mixed-version distributed reads).

        The unsearchable-key exclusion must be emitted as ``NOT has(array(...), x)``,
        never as ``NOT (x IN (...))``. A constant ``IN`` set makes ClickHouse build a
        prepared set whose server-generated ``__set_String_<hash>_<hash>`` identifier
        lands in the (arrayJoin'd) result-block column name. On a mixed-version cluster
        the two sides hash the set differently, so the column names disagree across
        ``Remote`` and the distributed read fails with
        ``Code: 10 ... Not found column ... While executing Remote.``. ``has`` over a
        constant array keeps the array inline in the column name, which is byte-stable
        across versions.
        """
        req = TraceItemAttributeNamesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                request_id=str(uuid.uuid4()),
                start_timestamp=Timestamp(seconds=int((BASE_TIME - timedelta(days=1)).timestamp())),
                end_timestamp=Timestamp(seconds=int((BASE_TIME + timedelta(days=1)).timestamp())),
            ),
            limit=TOTAL_GENERATED_ATTR_PER_TYPE,
            type=AttributeKey.Type.TYPE_STRING,
        )

        query = get_co_occurring_attributes(req).query

        # distinct(arrayJoin(arrayFilter((attr) -> not(has(array(...), attr.2)), ...)))
        distinct = query.get_selected_columns()[0].expression
        assert isinstance(distinct, FunctionCall) and distinct.function_name == "distinct"
        array_join = distinct.parameters[0]
        assert isinstance(array_join, FunctionCall) and array_join.function_name == "arrayJoin"
        array_filter = array_join.parameters[0]
        assert (
            isinstance(array_filter, FunctionCall) and array_filter.function_name == "arrayFilter"
        )
        predicate = array_filter.parameters[0]
        assert isinstance(predicate, Lambda)

        body = predicate.transformation
        assert isinstance(body, FunctionCall) and body.function_name == "not"
        has_call = body.parameters[0]
        assert isinstance(has_call, FunctionCall) and has_call.function_name == "has"

        keys_array, needle = has_call.parameters
        assert isinstance(keys_array, FunctionCall) and keys_array.function_name == "array"
        key_values = [p.value for p in keys_array.parameters if isinstance(p, Literal)]
        assert key_values == UNSEARCHABLE_ATTRIBUTE_KEYS
        assert isinstance(needle, FunctionCall) and needle.function_name == "tupleElement"

        # No IN set may be built over the unsearchable keys (that would reintroduce
        # the __set_* prepared-set identifier and the mixed-version failure).
        unsearchable = set(UNSEARCHABLE_ATTRIBUTE_KEYS)
        for exp in query.get_all_expressions():
            if isinstance(exp, FunctionCall) and exp.function_name == "in":
                for param in exp.parameters:
                    if isinstance(param, FunctionCall) and param.function_name == "array":
                        values = {p.value for p in param.parameters if isinstance(p, Literal)}
                        assert values != unsearchable

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
