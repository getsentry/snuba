import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

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

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_attribute_names import (
    EndpointTraceItemAttributeNames,
)
from tests.helpers import write_raw_unprocessed_events

_RELEASE_TAG = "4.2.0"


def gen_message(
    project_id: int,
    dt: datetime,
    measurements: dict[str, dict[str, float]] | None = None,
    tags: dict[str, str] | None = None,
) -> Mapping[str, Any]:
    measurements = measurements or {}
    tags = tags or {}
    return {
        "description": "/api/0/relays/projectconfigs/",
        "duration_ms": 152,
        "event_id": "d826225de75d42d6b2f01b957d51f18f",
        "exclusive_time_ms": 0.228,
        "is_segment": True,
        "data": {
            "sentry.environment": "development",
            "sentry.release": "abcde",
            "thread.name": "uWSGIWorker1Core0",
            "thread.id": "8522009600",
            "sentry.segment.name": "/api/0/relays/projectconfigs/",
            "sentry.sdk.name": "sentry.python.django",
            "sentry.sdk.version": "2.7.0",
            "my.float.field": 101.2,
            "my.int.field": 2000,
            "my.neg.field": -100,
            "my.neg.float.field": -101.2,
            "my.true.bool.field": True,
            "my.false.bool.field": False,
        },
        "measurements": {
            "num_of_spans": {"value": 50.0},
            "eap.measurement": {"value": random.choice([1, 100, 1000])},
            **measurements,
        },
        "organization_id": 1,
        "origin": "auto.http.django",
        "project_id": project_id,
        "received": 1721319572.877828,
        "retention_days": 90,
        "segment_id": "8873a98879faf06d",
        "sentry_tags": {
            "category": "http",
            "environment": "development",
            "op": "http.server",
            "platform": "python",
            "release": _RELEASE_TAG,
            "sdk.name": "sentry.python.django",
            "sdk.version": "2.7.0",
            "status": "ok",
            "status_code": "200",
            "thread.id": "8522009600",
            "thread.name": "uWSGIWorker1Core0",
            "trace.status": "ok",
            "transaction": "/api/0/relays/projectconfigs/",
            "transaction.method": "POST",
            "transaction.op": "http.server",
            "user": "ip:127.0.0.1",
        },
        "span_id": uuid.uuid4().hex,
        "tags": {
            **tags,
        },
        "trace_id": uuid.uuid4().hex,
        "start_timestamp_ms": int(dt.timestamp()) * 1000 - int(random.gauss(1000, 200)),
        "start_timestamp_precise": dt.timestamp(),
        "end_timestamp_precise": dt.timestamp() + 1,
    }


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
                gen_message(
                    1,
                    start,
                    measurements={
                        f"test_measure_{attr_set}_{i}": {"value": random.random()}
                        for i in range(num_attributes_per_item)
                    },
                    tags={
                        f"test_tag_{attr_set}_{i}": "value"
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

    def test_no_co_occurrence(self, setup_teardown: None):
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
