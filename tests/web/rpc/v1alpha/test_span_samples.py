import random
import uuid
from datetime import datetime, timedelta
from typing import Any, Mapping

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1alpha.endpoint_span_samples_pb2 import (
    SpanSamplesRequest as SpanSamplesRequestProto,
)
from sentry_protos.snuba.v1alpha.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1alpha.trace_item_attribute_pb2 import (
    AttributeKey,
    AttributeValue,
    VirtualColumnContext,
)
from sentry_protos.snuba.v1alpha.trace_item_filter_pb2 import (
    ComparisonFilter,
    ExistsFilter,
    OrFilter,
    TraceItemFilter,
)

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1alpha.span_samples import SpanSamplesRequest
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events

_RELEASE_TAG = "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b"


def gen_message(dt: datetime) -> Mapping[str, Any]:
    return {
        "description": "/api/0/relays/projectconfigs/",
        "duration_ms": 152,
        "event_id": "d826225de75d42d6b2f01b957d51f18f",
        "exclusive_time_ms": 0.228,
        "is_segment": True,
        "data": {
            "sentry.environment": "development",
            "sentry.release": _RELEASE_TAG,
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
        },
        "organization_id": 1,
        "origin": "auto.http.django",
        "project_id": 1,
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
        "span_id": "123456781234567D",
        "tags": {
            "http.status_code": "200",
            "relay_endpoint_version": "3",
            "relay_id": "88888888-4444-4444-8444-cccccccccccc",
            "relay_no_cache": "False",
            "relay_protocol_version": "3",
            "relay_use_post_or_schedule": "True",
            "relay_use_post_or_schedule_rejected": "version",
            "server_name": "D23CXQ4GK2.local",
            "spans_over_limit": "False",
            "color": random.choice(["red", "green", "blue"]),
            "location": random.choice(["mobile", "frontend", "backend"]),
        },
        "trace_id": uuid.uuid4().hex,
        "start_timestamp_ms": int(dt.timestamp()) * 1000 - int(random.gauss(1000, 200)),
        "start_timestamp_precise": dt.timestamp(),
        "end_timestamp_precise": dt.timestamp() + 1,
    }


BASE_TIME = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(
    minutes=180
)


@pytest.fixture(autouse=True)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    spans_storage = get_storage(StorageKey("eap_spans"))
    start = BASE_TIME
    messages = [gen_message(start - timedelta(minutes=i)) for i in range(120)]
    write_raw_unprocessed_events(spans_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestSpanSamples(BaseApiTest):
    def test_basic(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = SpanSamplesRequestProto(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=ts,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color")
                )
            ),
            keys=[AttributeKey(type=AttributeKey.TYPE_STRING, name="location")],
            order_by=[
                SpanSamplesRequestProto.OrderBy(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                )
            ],
            limit=10,
        )
        response = self.app.post(
            "/rpc/SpanSamplesRequest/v1alpha", data=message.SerializeToString()
        )
        assert response.status_code == 200, response.text

    def test_with_data(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        message = SpanSamplesRequestProto(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color")
                )
            ),
            keys=[AttributeKey(type=AttributeKey.TYPE_STRING, name="server_name")],
            order_by=[
                SpanSamplesRequestProto.OrderBy(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="server_name")
                )
            ],
            limit=61,
        )
        response = SpanSamplesRequest().execute(message)
        assert [
            dict((k, x.results[k].val_str) for k in x.results)
            for x in response.span_samples
        ] == [{"server_name": "D23CXQ4GK2.local"} for _ in range(60)]

    def test_booleans_and_number_compares(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        message = SpanSamplesRequestProto(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
            ),
            filter=TraceItemFilter(
                or_filter=OrFilter(
                    filters=[
                        TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_STRING,
                                    name="eap.measurement",
                                ),
                                op=ComparisonFilter.OP_LESS_THAN_OR_EQUALS,
                                value=AttributeValue(val_float=101),
                            ),
                        ),
                        TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_STRING,
                                    name="eap.measurement",
                                ),
                                op=ComparisonFilter.OP_GREATER_THAN,
                                value=AttributeValue(val_float=999),
                            ),
                        ),
                    ]
                )
            ),
            keys=[
                AttributeKey(type=AttributeKey.TYPE_BOOLEAN, name="sentry.is_segment"),
                AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.span_id"),
            ],
            order_by=[
                SpanSamplesRequestProto.OrderBy(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.status"
                    )
                )
            ],
            limit=61,
        )
        response = SpanSamplesRequest().execute(message)
        assert [
            dict(
                (k, (x.results[k].val_bool or x.results[k].val_str)) for k in x.results
            )
            for x in response.span_samples
        ] == [
            {"sentry.is_segment": True, "sentry.span_id": "123456781234567d"}
            for _ in range(60)
        ]

    def test_with_virtual_columns(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        message = SpanSamplesRequestProto(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.category"
                    )
                )
            ),
            keys=[
                AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.project_name"),
                AttributeKey(
                    type=AttributeKey.TYPE_STRING, name="sentry.release_version"
                ),
                AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.sdk.name"),
            ],
            order_by=[
                SpanSamplesRequestProto.OrderBy(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="project_name"
                    ),
                )
            ],
            limit=61,
            virtual_column_contexts=[
                VirtualColumnContext(
                    from_column_name="sentry.project_id",
                    to_column_name="sentry.project_name",
                    value_map={"1": "sentry", "2": "snuba"},
                ),
                VirtualColumnContext(
                    from_column_name="sentry.release",
                    to_column_name="sentry.release_version",
                    value_map={_RELEASE_TAG: "4.2.0.69"},
                ),
            ],
        )
        response = SpanSamplesRequest().execute(message)
        assert [
            dict((k, x.results[k].val_str) for k in x.results)
            for x in response.span_samples
        ] == [
            {
                "sentry.project_name": "sentry",
                "sentry.sdk.name": "sentry.python.django",
                "sentry.release_version": "4.2.0.69",
            }
            for _ in range(60)
        ]

    def test_order_by_virtual_columns(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        message = SpanSamplesRequestProto(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.category"
                    )
                )
            ),
            keys=[
                AttributeKey(type=AttributeKey.TYPE_STRING, name="special_color"),
            ],
            order_by=[
                SpanSamplesRequestProto.OrderBy(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="special_color"
                    )
                )
            ],
            limit=61,
            virtual_column_contexts=[
                VirtualColumnContext(
                    from_column_name="color",
                    to_column_name="special_color",
                    value_map={"red": "1", "green": "2", "blue": "3"},
                ),
            ],
        )
        response = SpanSamplesRequest().execute(message)
        result_dicts = [
            dict((k, x.results[k].val_str) for k in x.results)
            for x in response.span_samples
        ]
        colors = [d["special_color"] for d in result_dicts]
        assert sorted(colors) == colors
