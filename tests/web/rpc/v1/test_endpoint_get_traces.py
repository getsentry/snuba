import random
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from operator import itemgetter
from typing import Any, Mapping

import pytest
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_get_traces_pb2 import (
    GetTracesRequest,
    GetTracesResponse,
    TraceAttribute,
)
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1.request_common_pb2 import (
    PageToken,
    RequestMeta,
    ResponseMeta,
    TraceItemType,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.endpoint_get_traces import EndpointGetTraces
from tests.base import BaseApiTest
from tests.conftest import SnubaSetConfig
from tests.helpers import write_raw_unprocessed_events

_RELEASE_TAG = "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b"
_SERVER_NAME = "D23CXQ4GK2.local"
_TRACE_IDS = [uuid.uuid4().hex for _ in range(10)]
_BASE_TIME = datetime.now(tz=timezone.utc).replace(
    minute=0,
    second=0,
    microsecond=0,
) - timedelta(minutes=180)
_SPAN_COUNT = 120
_REQUEST_ID = uuid.uuid4().hex


def gen_message(
    dt: datetime,
    trace_id: str,
    measurements: dict[str, dict[str, float]] | None = None,
    tags: dict[str, str] | None = None,
    span_op: str = "http.server",
    span_name: str = "root",
    is_segment: bool = False,
    parent_span_id: str = "0" * 16,
) -> Mapping[str, Any]:
    measurements = measurements or {}
    tags = tags or {}
    timestamp = dt.timestamp()
    if not is_segment:
        timestamp += random.random()
    return {
        "description": span_name,
        "duration_ms": 152,
        "event_id": uuid.uuid4().hex,
        "exclusive_time_ms": 0.228,
        "is_segment": is_segment,
        "parent_span_id": parent_span_id,
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
        "segment_id": trace_id[:16],
        "sentry_tags": {
            "category": "http",
            "environment": "development",
            "op": span_op,
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
        "span_id": uuid.uuid4().hex[:16],
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
        "start_timestamp_ms": int(timestamp * 1000),
        "start_timestamp_precise": timestamp,
        "end_timestamp_precise": timestamp + 1,
    }


_SPANS = [
    gen_message(
        dt=_BASE_TIME + timedelta(minutes=i),
        trace_id=_TRACE_IDS[i % len(_TRACE_IDS)],
        span_op="navigation" if i < len(_TRACE_IDS) else "db",
        span_name=(
            "root"
            if i < len(_TRACE_IDS)
            else f"child {i % len(_TRACE_IDS) + 1} of {_SPAN_COUNT // len(_TRACE_IDS) - 1}"
        ),
        is_segment=i < len(_TRACE_IDS),
        parent_span_id="0" * 16 if i < len(_TRACE_IDS) else "1" * 16,
    )
    for i in range(_SPAN_COUNT)
]


@pytest.fixture(autouse=False)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    spans_storage = get_storage(StorageKey("eap_spans"))
    items_storage = get_storage(StorageKey("eap_items"))
    write_raw_unprocessed_events(spans_storage, _SPANS)  # type: ignore
    write_raw_unprocessed_events(items_storage, _SPANS)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestGetTraces(BaseApiTest):
    def test_without_data(self) -> None:
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
                request_id=_REQUEST_ID,
            ),
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_TRACE_ID,
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

    def test_with_data(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        three_hours_later = int((_BASE_TIME + timedelta(hours=3)).timestamp())
        start_timestamp_per_trace_id: dict[str, float] = defaultdict(lambda: 2 * 1e10)
        for s in _SPANS:
            start_timestamp_per_trace_id[s["trace_id"]] = min(
                start_timestamp_per_trace_id[s["trace_id"]],
                s["start_timestamp_precise"],
            )
        trace_id_per_start_timestamp: dict[float, str] = {
            timestamp: trace_id
            for trace_id, timestamp in start_timestamp_per_trace_id.items()
        }

        message = GetTracesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=Timestamp(seconds=three_hours_later),
                request_id=_REQUEST_ID,
            ),
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_TRACE_ID,
                ),
            ],
        )
        response = EndpointGetTraces().execute(message)
        expected_response = GetTracesResponse(
            traces=[
                GetTracesResponse.Trace(
                    attributes=[
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_TRACE_ID,
                            type=AttributeKey.TYPE_STRING,
                            value=AttributeValue(
                                val_str=trace_id_per_start_timestamp[start_timestamp],
                            ),
                        ),
                    ],
                )
                for start_timestamp in reversed(
                    sorted(trace_id_per_start_timestamp.keys())
                )
            ],
            page_token=PageToken(offset=len(_TRACE_IDS)),
            meta=ResponseMeta(request_id=_REQUEST_ID),
        )
        assert MessageToDict(response) == MessageToDict(expected_response)

    def test_with_data_and_limit(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        three_hours_later = int((_BASE_TIME + timedelta(hours=3)).timestamp())
        message = GetTracesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=Timestamp(seconds=three_hours_later),
                request_id=_REQUEST_ID,
            ),
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_TRACE_ID,
                ),
            ],
            limit=1,
        )
        response = EndpointGetTraces().execute(message)
        last_span = sorted(
            _SPANS,
            key=itemgetter("start_timestamp_ms"),
        )[len(_SPANS) - 1]
        expected_response = GetTracesResponse(
            traces=[
                GetTracesResponse.Trace(
                    attributes=[
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_TRACE_ID,
                            type=AttributeKey.Type.TYPE_STRING,
                            value=AttributeValue(
                                val_str=last_span["trace_id"],
                            ),
                        )
                    ],
                )
            ],
            page_token=PageToken(offset=1),
            meta=ResponseMeta(request_id=_REQUEST_ID),
        )
        assert MessageToDict(response) == MessageToDict(expected_response)

    def test_with_data_and_filter(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        three_hours_later = int((_BASE_TIME + timedelta(hours=3)).timestamp())
        message = GetTracesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=Timestamp(seconds=three_hours_later),
                request_id=_REQUEST_ID,
            ),
            filters=[
                GetTracesRequest.TraceFilter(
                    item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    filter=TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                name="sentry.trace_id",
                                type=AttributeKey.TYPE_STRING,
                            ),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(
                                val_str=_TRACE_IDS[0],
                            ),
                        ),
                    ),
                ),
            ],
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_TRACE_ID,
                ),
            ],
        )
        response = EndpointGetTraces().execute(message)
        expected_response = GetTracesResponse(
            traces=[
                GetTracesResponse.Trace(
                    attributes=[
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_TRACE_ID,
                            type=AttributeKey.Type.TYPE_STRING,
                            value=AttributeValue(
                                val_str=_TRACE_IDS[0],
                            ),
                        )
                    ],
                )
            ],
            page_token=PageToken(offset=1),
            meta=ResponseMeta(request_id=_REQUEST_ID),
        )
        assert MessageToDict(response) == MessageToDict(expected_response)

    def test_with_data_and_aggregated_fields_all_keys(
        self, setup_teardown: Any
    ) -> None:

        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        three_hours_later = int((_BASE_TIME + timedelta(hours=3)).timestamp())
        start_timestamp_per_trace_id: dict[str, float] = defaultdict(lambda: 2 * 1e10)
        for s in _SPANS:
            start_timestamp_per_trace_id[s["trace_id"]] = min(
                start_timestamp_per_trace_id[s["trace_id"]],
                s["start_timestamp_precise"],
            )
        trace_id_per_start_timestamp: dict[float, str] = {
            timestamp: trace_id
            for trace_id, timestamp in start_timestamp_per_trace_id.items()
        }
        message = GetTracesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=Timestamp(seconds=three_hours_later),
                request_id=_REQUEST_ID,
            ),
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_TRACE_ID,
                    type=AttributeKey.TYPE_STRING,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_START_TIMESTAMP,
                    type=AttributeKey.TYPE_DOUBLE,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_TOTAL_ITEM_COUNT,
                    type=AttributeKey.TYPE_INT,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_FILTERED_ITEM_COUNT,
                    type=AttributeKey.TYPE_INT,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_ROOT_SPAN_NAME,
                    type=AttributeKey.TYPE_STRING,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_ROOT_SPAN_DURATION_MS,
                    type=AttributeKey.TYPE_INT,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_ROOT_SPAN_PROJECT_ID,
                    type=AttributeKey.TYPE_INT,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_EARLIEST_SPAN_NAME,
                    type=AttributeKey.TYPE_STRING,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_EARLIEST_SPAN_PROJECT_ID,
                    type=AttributeKey.TYPE_INT,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_EARLIEST_SPAN_DURATION_MS,
                    type=AttributeKey.TYPE_INT,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_EARLIEST_FRONTEND_SPAN,
                    type=AttributeKey.TYPE_STRING,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_EARLIEST_FRONTEND_SPAN_PROJECT_ID,
                    type=AttributeKey.TYPE_INT,
                ),
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_EARLIEST_FRONTEND_SPAN_DURATION_MS,
                    type=AttributeKey.TYPE_INT,
                ),
            ],
            filters=[
                GetTracesRequest.TraceFilter(
                    item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    filter=TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                name="sentry.op",
                                type=AttributeKey.TYPE_STRING,
                            ),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_str="db"),
                        ),
                    ),
                ),
            ],
        )
        response = EndpointGetTraces().execute(message)
        expected_response = GetTracesResponse(
            traces=[
                GetTracesResponse.Trace(
                    attributes=[
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_TRACE_ID,
                            type=AttributeKey.TYPE_STRING,
                            value=AttributeValue(
                                val_str=trace_id_per_start_timestamp[start_timestamp],
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_START_TIMESTAMP,
                            type=AttributeKey.TYPE_DOUBLE,
                            value=AttributeValue(
                                val_double=start_timestamp_per_trace_id[
                                    trace_id_per_start_timestamp[start_timestamp]
                                ],
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_TOTAL_ITEM_COUNT,
                            type=AttributeKey.TYPE_INT,
                            value=AttributeValue(
                                val_int=_SPAN_COUNT // len(_TRACE_IDS),
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_FILTERED_ITEM_COUNT,
                            type=AttributeKey.TYPE_INT,
                            value=AttributeValue(
                                val_int=(_SPAN_COUNT // len(_TRACE_IDS)) - 1,
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_ROOT_SPAN_NAME,
                            type=AttributeKey.TYPE_STRING,
                            value=AttributeValue(
                                val_str="root",
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_ROOT_SPAN_DURATION_MS,
                            type=AttributeKey.TYPE_INT,
                            value=AttributeValue(
                                val_int=1000,
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_ROOT_SPAN_PROJECT_ID,
                            type=AttributeKey.TYPE_INT,
                            value=AttributeValue(
                                val_int=1,
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_EARLIEST_SPAN_NAME,
                            type=AttributeKey.TYPE_STRING,
                            value=AttributeValue(
                                val_str="root",
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_EARLIEST_SPAN_PROJECT_ID,
                            type=AttributeKey.TYPE_INT,
                            value=AttributeValue(
                                val_int=1,
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_EARLIEST_SPAN_DURATION_MS,
                            type=AttributeKey.TYPE_INT,
                            value=AttributeValue(
                                val_int=1000,
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_EARLIEST_FRONTEND_SPAN,
                            type=AttributeKey.TYPE_STRING,
                            value=AttributeValue(
                                val_str="root",
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_EARLIEST_FRONTEND_SPAN_PROJECT_ID,
                            type=AttributeKey.TYPE_INT,
                            value=AttributeValue(
                                val_int=1,
                            ),
                        ),
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_EARLIEST_FRONTEND_SPAN_DURATION_MS,
                            type=AttributeKey.TYPE_INT,
                            value=AttributeValue(
                                val_int=1000,
                            ),
                        ),
                    ],
                )
                for start_timestamp in reversed(
                    sorted(trace_id_per_start_timestamp.keys())
                )
            ],
            page_token=PageToken(offset=len(_TRACE_IDS)),
            meta=ResponseMeta(request_id=_REQUEST_ID),
        )
        assert MessageToDict(response) == MessageToDict(expected_response)

    def test_with_data_and_aggregated_fields(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        three_hours_later = int((_BASE_TIME + timedelta(hours=3)).timestamp())
        start_timestamp_per_trace_id: dict[str, float] = defaultdict(lambda: 2 * 1e10)
        for s in _SPANS:
            start_timestamp_per_trace_id[s["trace_id"]] = min(
                start_timestamp_per_trace_id[s["trace_id"]],
                s["start_timestamp_precise"],
            )
        trace_id_per_start_timestamp: dict[float, str] = {
            timestamp: trace_id
            for trace_id, timestamp in start_timestamp_per_trace_id.items()
        }
        message = GetTracesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=Timestamp(seconds=three_hours_later),
                request_id=_REQUEST_ID,
            ),
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_START_TIMESTAMP,
                    type=AttributeKey.TYPE_DOUBLE,
                ),
            ],
            filters=[
                GetTracesRequest.TraceFilter(
                    item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    filter=TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                name="sentry.op",
                                type=AttributeKey.TYPE_STRING,
                            ),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_str="db"),
                        ),
                    ),
                ),
            ],
        )
        response = EndpointGetTraces().execute(message)
        expected_response = GetTracesResponse(
            traces=[
                GetTracesResponse.Trace(
                    attributes=[
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_START_TIMESTAMP,
                            type=AttributeKey.TYPE_DOUBLE,
                            value=AttributeValue(
                                val_double=start_timestamp_per_trace_id[
                                    trace_id_per_start_timestamp[start_timestamp]
                                ],
                            ),
                        ),
                    ],
                )
                for start_timestamp in reversed(
                    sorted(trace_id_per_start_timestamp.keys())
                )
            ],
            page_token=PageToken(offset=len(_TRACE_IDS)),
            meta=ResponseMeta(request_id=_REQUEST_ID),
        )
        assert MessageToDict(response) == MessageToDict(expected_response)

    def test_with_data_and_aggregated_fields_ignore_case(
        self, setup_teardown: Any
    ) -> None:
        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        three_hours_later = int((_BASE_TIME + timedelta(hours=3)).timestamp())
        start_timestamp_per_trace_id: dict[str, float] = defaultdict(lambda: 2 * 1e10)
        for s in _SPANS:
            start_timestamp_per_trace_id[s["trace_id"]] = min(
                start_timestamp_per_trace_id[s["trace_id"]],
                s["start_timestamp_precise"],
            )
        trace_id_per_start_timestamp: dict[float, str] = {
            timestamp: trace_id
            for trace_id, timestamp in start_timestamp_per_trace_id.items()
        }
        message = GetTracesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=Timestamp(seconds=three_hours_later),
                request_id=_REQUEST_ID,
            ),
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_START_TIMESTAMP,
                    type=AttributeKey.TYPE_DOUBLE,
                ),
            ],
            filters=[
                GetTracesRequest.TraceFilter(
                    item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    filter=TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                name="sentry.op",
                                type=AttributeKey.TYPE_STRING,
                            ),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_str="DB"),
                            ignore_case=True,
                        ),
                    ),
                ),
            ],
        )
        response = EndpointGetTraces().execute(message)
        expected_response = GetTracesResponse(
            traces=[
                GetTracesResponse.Trace(
                    attributes=[
                        TraceAttribute(
                            key=TraceAttribute.Key.KEY_START_TIMESTAMP,
                            type=AttributeKey.TYPE_DOUBLE,
                            value=AttributeValue(
                                val_double=start_timestamp_per_trace_id[
                                    trace_id_per_start_timestamp[start_timestamp]
                                ],
                            ),
                        ),
                    ],
                )
                for start_timestamp in reversed(
                    sorted(trace_id_per_start_timestamp.keys())
                )
            ],
            page_token=PageToken(offset=len(_TRACE_IDS)),
            meta=ResponseMeta(request_id=_REQUEST_ID),
        )
        assert MessageToDict(response) == MessageToDict(expected_response)

    def test_with_data_and_aggregated_fields_ignore_case_on_non_strings_error(
        self, setup_teardown: Any
    ) -> None:
        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        three_hours_later = int((_BASE_TIME + timedelta(hours=3)).timestamp())
        start_timestamp_per_trace_id: dict[str, float] = defaultdict(lambda: 2 * 1e10)
        for s in _SPANS:
            start_timestamp_per_trace_id[s["trace_id"]] = min(
                start_timestamp_per_trace_id[s["trace_id"]],
                s["start_timestamp_precise"],
            )

        message = GetTracesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=Timestamp(seconds=three_hours_later),
                request_id=_REQUEST_ID,
            ),
            attributes=[
                TraceAttribute(
                    key=TraceAttribute.Key.KEY_START_TIMESTAMP,
                    type=AttributeKey.TYPE_DOUBLE,
                ),
            ],
            filters=[
                GetTracesRequest.TraceFilter(
                    item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    filter=TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                name="my.float.field",
                                type=AttributeKey.TYPE_DOUBLE,
                            ),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_double=0.123),
                            ignore_case=True,
                        ),
                    ),
                ),
            ],
        )
        with pytest.raises(
            BadSnubaRPCRequestException, match="Cannot ignore case on non-string values"
        ):
            EndpointGetTraces().execute(message)


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestGetTracesEAPItems(TestGetTraces):
    """
    Run the tests again, but this time on the eap_items table as well to ensure it also works.
    """

    @pytest.fixture(autouse=True)
    def use_eap_items_table(
        self, snuba_set_config: SnubaSetConfig, redis_db: None
    ) -> None:
        snuba_set_config("use_eap_items_table", True)
        snuba_set_config("use_eap_items_table_start_timestamp_seconds", 0)
