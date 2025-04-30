import random
from datetime import datetime, timedelta, timezone

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeNamesRequest,
    TraceItemAttributeNamesResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AndFilter,
    ComparisonFilter,
    TraceItemFilter,
)
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_attribute_names import (
    EndpointTraceItemAttributeNames,
)
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import gen_item_message

BASE_TIME = datetime.now(timezone.utc).replace(
    minute=0, second=0, microsecond=0
) - timedelta(minutes=180)


META = RequestMeta(
    project_ids=[1],
    organization_id=1,
    cogs_category="something",
    referrer="something",
    start_timestamp=Timestamp(seconds=int((BASE_TIME - timedelta(days=1)).timestamp())),
    end_timestamp=Timestamp(seconds=int((BASE_TIME + timedelta(days=1)).timestamp())),
    trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
)


@pytest.fixture(autouse=False)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    items_storage = get_storage(StorageKey("eap_items"))
    start = BASE_TIME

    num_messages_per_set = 10
    messages = []
    num_attr_sets = 5
    num_attributes_per_item = 5

    for attr_set in range(num_attr_sets):
        for _ in range(num_messages_per_set):
            messages.append(
                gen_item_message(
                    start_timestamp=start,
                    attributes={
                        f"test_measure_{attr_set}_{i}": AnyValue(
                            double_value=random.random()
                        )
                        for i in range(num_attributes_per_item)
                    }
                    | {
                        f"test_tag_{attr_set}_{i}": AnyValue(string_attribute="value")
                        for i in range(num_attributes_per_item)
                    },
                )
            )
    write_raw_unprocessed_events(items_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestSmartAutocompleteData:
    def test_limit_to_type(self, setup_teardown: None) -> None:
        req = TraceItemAttributeNamesRequest(
            meta=META,
            limit=1000,
            value_substring_match="test_",
            type=AttributeKey.Type.TYPE_STRING,
            intersecting_attributes_filter=TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="test_tag_1_0"
                    ),
                    op=ComparisonFilter.OP_EQUALS,
                    value=AttributeValue(val_str="value"),
                )
            ),
        )
        res = EndpointTraceItemAttributeNames().execute(req)
        expected = []
        # tag_1_{1..5}
        for i in range(1, 5):
            expected.append(
                TraceItemAttributeNamesResponse.Attribute(
                    name=f"test_tag_1_{i}", type=AttributeKey.Type.TYPE_STRING
                )
            )
        assert res.attributes == expected

    def test_query_across_types(self, setup_teardown: None) -> None:
        req = TraceItemAttributeNamesRequest(
            meta=META,
            limit=1000,
            value_substring_match="test_",
            intersecting_attributes_filter=TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="test_tag_1_0"
                    ),
                    op=ComparisonFilter.OP_EQUALS,
                    value=AttributeValue(val_str="value"),
                )
            ),
        )
        res = EndpointTraceItemAttributeNames().execute(req)
        expected = []
        # measure_1_{0..5}
        for i in range(5):
            expected.append(
                TraceItemAttributeNamesResponse.Attribute(
                    name=f"test_measure_1_{i}", type=AttributeKey.Type.TYPE_DOUBLE
                )
            )

        # tag_1_{1..5}
        for i in range(1, 5):
            expected.append(
                TraceItemAttributeNamesResponse.Attribute(
                    name=f"test_tag_1_{i}", type=AttributeKey.Type.TYPE_STRING
                )
            )

        assert res.attributes == expected

    def test_many_filters(self, setup_teardown: None) -> None:
        req = TraceItemAttributeNamesRequest(
            meta=META,
            limit=1000,
            value_substring_match="test_",
            intersecting_attributes_filter=TraceItemFilter(
                and_filter=AndFilter(
                    filters=[
                        TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_STRING, name="test_tag_1_0"
                                ),
                                op=ComparisonFilter.OP_EQUALS,
                                value=AttributeValue(val_str="value"),
                            )
                        ),
                        TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_DOUBLE,
                                    name="test_measure_1_0",
                                ),
                                op=ComparisonFilter.OP_EQUALS,
                                value=AttributeValue(val_double=1),
                            )
                        ),
                    ]
                )
            ),
        )
        res = EndpointTraceItemAttributeNames().execute(req)
        expected = []
        for i in range(1, 5):
            expected.append(
                TraceItemAttributeNamesResponse.Attribute(
                    name=f"test_measure_1_{i}", type=AttributeKey.Type.TYPE_DOUBLE
                )
            )
        for i in range(1, 5):
            expected.append(
                TraceItemAttributeNamesResponse.Attribute(
                    name=f"test_tag_1_{i}", type=AttributeKey.Type.TYPE_STRING
                )
            )

        assert res.attributes == expected

    def test_no_co_occurrence(self, setup_teardown: None) -> None:
        req = TraceItemAttributeNamesRequest(
            meta=META,
            limit=1000,
            value_substring_match="test_",
            intersecting_attributes_filter=TraceItemFilter(
                and_filter=AndFilter(
                    filters=[
                        # tag_1_* and tag_2_* do not co-occur
                        TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_STRING, name="test_tag_1_0"
                                ),
                                op=ComparisonFilter.OP_EQUALS,
                                value=AttributeValue(val_str="value"),
                            )
                        ),
                        TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_DOUBLE,
                                    name="test_measure_2_0",
                                ),
                                op=ComparisonFilter.OP_EQUALS,
                                value=AttributeValue(val_double=1),
                            )
                        ),
                    ]
                )
            ),
        )
        res = EndpointTraceItemAttributeNames().execute(req)
        assert res.attributes == []

    def test_backwards_compat_names(self, setup_teardown: None) -> None:
        # this tests makes sure the the old field names supported by EAP spans would still be returned
        # by the autocomplete endpoint
        req = TraceItemAttributeNamesRequest(
            meta=META,
            limit=1000,
            value_substring_match="test_",
            intersecting_attributes_filter=TraceItemFilter(
                and_filter=AndFilter(
                    filters=[
                        TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_STRING, name="sentry.name"
                                ),
                                op=ComparisonFilter.OP_EQUALS,
                                value=AttributeValue(val_str="value"),
                            )
                        ),
                    ]
                )
            ),
        )
        res = EndpointTraceItemAttributeNames().execute(req)
        assert len(res.attributes) > 0
