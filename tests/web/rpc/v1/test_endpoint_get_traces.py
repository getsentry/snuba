import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_get_traces_pb2 import (
    GetTracesRequest,
    GetTracesResponse,
    TraceColumn,
)
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1.request_common_pb2 import (
    PageToken,
    RequestMeta,
    ResponseMeta,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_get_traces import EndpointGetTraces
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events

_RELEASE_TAG = "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b"
_SERVER_NAME = "D23CXQ4GK2.local"
_TRACE_IDS = [uuid.uuid4().hex for _ in range(10)]
_BASE_TIME = datetime.now(tz=timezone.utc).replace(
    minute=0,
    second=0,
    microsecond=0,
) - timedelta(minutes=180)


def gen_message(
    dt: datetime,
    trace_id: str,
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
            **measurements,
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
            "server_name": _SERVER_NAME,
            "spans_over_limit": "False",
            "color": random.choice(["red", "green", "blue"]),
            "location": random.choice(["mobile", "frontend", "backend"]),
            **tags,
        },
        "trace_id": trace_id,
        "start_timestamp_ms": int(dt.timestamp()) * 1000 - int(random.gauss(1000, 200)),
        "start_timestamp_precise": dt.timestamp(),
        "end_timestamp_precise": dt.timestamp() + 1,
    }


@pytest.fixture(autouse=False)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    spans_storage = get_storage(StorageKey("eap_spans"))
    start = _BASE_TIME
    messages = [
        gen_message(
            dt=start - timedelta(minutes=i),
            trace_id=_TRACE_IDS[i % len(_TRACE_IDS)],
        )
        for i in range(120)
    ]
    write_raw_unprocessed_events(spans_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestGetTraces(BaseApiTest):
    def test_no_data(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = GetTracesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=ts,
            ),
            columns=[
                TraceColumn(
                    name=TraceColumn.Name.NAME_TRACE_ID,
                    type=AttributeKey.TYPE_STRING,
                )
            ],
            limit=10,
        )
        response = self.app.post(
            "/rpc/EndpointGetTraces/v1", data=message.SerializeToString()
        )
        error_proto = ErrorProto()
        if response.status_code != 200:
            error_proto.ParseFromString(response.data)
        assert response.status_code == 200, error_proto

    def test_with_data_and_order_by(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        hour_ago = int((_BASE_TIME - timedelta(hours=1)).timestamp())
        message = GetTracesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
            ),
            columns=[
                TraceColumn(
                    name=TraceColumn.Name.NAME_TRACE_ID,
                    type=AttributeKey.TYPE_STRING,
                )
            ],
            order_by=[
                GetTracesRequest.OrderBy(
                    column=TraceColumn(
                        name=TraceColumn.Name.NAME_TRACE_ID,
                        type=AttributeKey.TYPE_STRING,
                    ),
                ),
            ],
        )
        response = EndpointGetTraces().execute(message)
        expected_response = GetTracesResponse(
            traces=[
                GetTracesResponse.Trace(
                    columns=[
                        GetTracesResponse.Trace.Column(
                            name=TraceColumn.Name.NAME_TRACE_ID,
                            value=AttributeValue(
                                val_str=trace_id,
                            ),
                        ),
                    ],
                )
                for trace_id in sorted(_TRACE_IDS)
            ],
            page_token=PageToken(offset=len(_TRACE_IDS)),
            meta=ResponseMeta(request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480"),
        )
        assert response == expected_response

    def test_with_data_order_by_and_limit(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        hour_ago = int((_BASE_TIME - timedelta(hours=1)).timestamp())
        message = GetTracesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
            ),
            columns=[
                TraceColumn(
                    name=TraceColumn.Name.NAME_TRACE_ID,
                    type=AttributeKey.TYPE_STRING,
                ),
            ],
            order_by=[
                GetTracesRequest.OrderBy(
                    column=TraceColumn(
                        name=TraceColumn.Name.NAME_TRACE_ID,
                        type=AttributeKey.TYPE_STRING,
                    ),
                ),
            ],
            limit=1,
        )
        response = EndpointGetTraces().execute(message)
        expected_response = GetTracesResponse(
            traces=[
                GetTracesResponse.Trace(
                    columns=[
                        GetTracesResponse.Trace.Column(
                            name=TraceColumn.Name.NAME_TRACE_ID,
                            value=AttributeValue(
                                val_str=sorted(_TRACE_IDS)[0],
                            ),
                        )
                    ],
                )
            ],
            page_token=PageToken(offset=1),
            meta=ResponseMeta(request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480"),
        )
        assert response == expected_response

    def test_with_data_and_filter(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        hour_ago = int((_BASE_TIME - timedelta(hours=1)).timestamp())
        message = GetTracesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
            ),
            filter=TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING,
                        name="sentry.trace_id",
                    ),
                    op=ComparisonFilter.OP_EQUALS,
                    value=AttributeValue(
                        val_str=_TRACE_IDS[0],
                    ),
                ),
            ),
            columns=[
                TraceColumn(
                    name=TraceColumn.Name.NAME_TRACE_ID,
                    type=AttributeKey.TYPE_STRING,
                ),
            ],
        )
        response = EndpointGetTraces().execute(message)
        expected_response = GetTracesResponse(
            traces=[
                GetTracesResponse.Trace(
                    columns=[
                        GetTracesResponse.Trace.Column(
                            name=TraceColumn.Name.NAME_TRACE_ID,
                            value=AttributeValue(
                                val_str=_TRACE_IDS[0],
                            ),
                        )
                    ],
                )
            ],
            page_token=PageToken(offset=1),
            meta=ResponseMeta(request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480"),
        )
        assert response == expected_response
