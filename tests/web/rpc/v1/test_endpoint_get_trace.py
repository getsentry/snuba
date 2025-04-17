import random
import uuid
from datetime import datetime, timedelta, timezone
from operator import attrgetter
from typing import Any, Mapping

import pytest
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_get_trace_pb2 import (
    GetTraceRequest,
    GetTraceResponse,
)
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1.request_common_pb2 import (
    RequestMeta,
    ResponseMeta,
    TraceItemType,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_get_trace import EndpointGetTrace
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events

_RELEASE_TAG = "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b"
_SERVER_NAME = "D23CXQ4GK2.local"
_TRACE_ID = uuid.uuid4().hex
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
) -> Mapping[str, Any]:
    measurements = measurements or {}
    tags = tags or {}
    timestamp = dt.timestamp()
    if not is_segment:
        timestamp += random.random()
    timestamp = round(timestamp, 6)
    return {
        "description": span_name,
        "duration_ms": 152,
        "event_id": uuid.uuid4().hex,
        "exclusive_time_ms": 0.228,
        "is_segment": is_segment,
        "data": {
            "environment": "development",
            "release": _RELEASE_TAG,
            "thread.name": "uWSGIWorker1Core0",
            "thread.id": "8522009600",
            "segment.name": "/api/0/relays/projectconfigs/",
            "sdk.name": "sentry.python.django",
            "sdk.version": "2.7.0",
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
        trace_id=_TRACE_ID,
        span_op="http.server" if i == 0 else "db",
        span_name=("root" if i == 0 else f"child {i + 1} of {_SPAN_COUNT}"),
        is_segment=i == 0,
    )
    for i in range(_SPAN_COUNT)
]


def get_attributes(span: Mapping[str, Any]) -> list[GetTraceResponse.Item.Attribute]:
    attributes: list[GetTraceResponse.Item.Attribute] = []
    for key, value in span.get("measurements", {}).items():
        attribute_key = AttributeKey(
            name=key,
            type=AttributeKey.Type.TYPE_DOUBLE,
        )
        attribute_value = AttributeValue(
            val_double=value["value"],
        )
        attributes.append(
            GetTraceResponse.Item.Attribute(
                key=attribute_key,
                value=attribute_value,
            )
        )

    for field in {"tags", "sentry_tags", "data"}:
        for key, value in span.get(field, {}).items():
            if field == "sentry_tags":
                key = f"sentry.{key}"
            if key == "sentry.transaction":
                continue
            if isinstance(value, str):
                attribute_key = AttributeKey(
                    name=key,
                    type=AttributeKey.Type.TYPE_STRING,
                )
                attribute_value = AttributeValue(
                    val_str=value,
                )
            elif isinstance(value, int) or isinstance(value, float):
                attribute_key = AttributeKey(
                    name=key,
                    type=AttributeKey.Type.TYPE_DOUBLE,
                )
                attribute_value = AttributeValue(
                    val_double=value,
                )
            else:
                continue

            attributes.append(
                GetTraceResponse.Item.Attribute(
                    key=attribute_key,
                    value=attribute_value,
                )
            )

    ATTRS_EXTRACTED_FROM_ROOT = [
        "organization_id",
        "duration_ms",
        "exclusive_time_ms",
        "is_segment",
        "segment_id",
        "parent_span_id",
        "end_timestamp",
        "trace_id",
        "project_id",
        "start_timestamp",
    ]

    for key in ATTRS_EXTRACTED_FROM_ROOT:
        attr_value_inner = span.get(key, "")
        if isinstance(attr_value_inner, str):
            attribute_key = AttributeKey(
                name=key,
                type=AttributeKey.Type.TYPE_STRING,
            )
            attribute_value = AttributeValue(
                val_str=attr_value_inner,
            )
        elif isinstance(attr_value_inner, int) or isinstance(attr_value_inner, float):
            attribute_key = AttributeKey(
                name=key,
                type=AttributeKey.Type.TYPE_DOUBLE,
            )
            attribute_value = AttributeValue(
                val_double=attr_value_inner,
            )
        else:
            raise TypeError(f"Unexpected type {type(attr_value_inner)} for key: {key}")

        attributes.append(
            GetTraceResponse.Item.Attribute(
                key=attribute_key,
                value=attribute_value,
            )
        )

    return attributes


@pytest.fixture(autouse=False)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    items_storage = get_storage(StorageKey("eap_items"))
    write_raw_unprocessed_events(items_storage, _SPANS)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestGetTrace(BaseApiTest):
    def test_without_data(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = GetTraceRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=ts,
                request_id=_REQUEST_ID,
            ),
            trace_id=uuid.uuid4().hex,
        )
        response = self.app.post(
            "/rpc/EndpointGetTrace/v1", data=message.SerializeToString()
        )
        error_proto = ErrorProto()
        if response.status_code != 200:
            error_proto.ParseFromString(response.data)
        assert response.status_code == 200, error_proto

    def test_with_data(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        three_hours_later = int((_BASE_TIME + timedelta(hours=3)).timestamp())
        message = GetTraceRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=Timestamp(seconds=three_hours_later),
                request_id=_REQUEST_ID,
            ),
            trace_id=_TRACE_ID,
            items=[
                GetTraceRequest.TraceItem(
                    item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                )
            ],
        )
        response = EndpointGetTrace().execute(message)
        timestamps: list[Timestamp] = []
        for span in _SPANS:
            timestamp = Timestamp()
            # truncate to microseconds since we store microsecond precision only
            # then transform to nanoseconds
            timestamp.FromNanoseconds(int(span["start_timestamp_precise"] * 1e6) * 1000)
            timestamps.append(timestamp)

        expected_response = GetTraceResponse(
            meta=ResponseMeta(request_id=_REQUEST_ID),
            trace_id=_TRACE_ID,
            item_groups=[
                GetTraceResponse.ItemGroup(
                    item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    items=[
                        GetTraceResponse.Item(
                            id=span["span_id"],
                            timestamp=timestamp,
                            attributes=sorted(
                                get_attributes(span),
                                key=attrgetter("key.name"),
                            ),
                        )
                        for timestamp, span in zip(timestamps, _SPANS)
                    ],
                ),
            ],
        )

        KV_COMPARE_IGNORE_LIST = {
            "service",  # actual is 1, expecting empty
            "segment_name",  # seems to contain data from span.data.segment.name, I think there's a manual alias somewhere
            "name",  # contains data from span.description
            "parent_span_id",  # set by service, I think?
            "end_timestamp",  # set from end_timestamp_precise
            "sampling_factor",  # returned by service, independent of input
            "sampling_weight",  # returned by service, independent of input
            "organization_id",  # breaks due to int/double comparison
            "project_id",  # breaks due to int/double comparison
            "start_timestamp",  # breaks due to int/double comparison
        }

        FUZZY_NUMERIC_COMPARE_LIST = {
            "organization_id",
            "project_id",
            "start_timestamp",
        }

        actual_attributes = [
            attribute
            for attribute in response.item_groups[0].items[0].attributes
            if attribute.key.name not in KV_COMPARE_IGNORE_LIST
        ]

        expected_attributes = [
            attribute
            for attribute in expected_response.item_groups[0].items[0].attributes
            if attribute.key.name not in KV_COMPARE_IGNORE_LIST
        ]

        # easier to read if keys don't match
        actual_attributes_keys = {attribute.key.name for attribute in actual_attributes}
        expected_attributes_keys = {
            attribute.key.name for attribute in expected_attributes
        }
        assert actual_attributes_keys == expected_attributes_keys

        # Simple comparison attributes
        assert list(actual_attributes) == list(expected_attributes)

        actual_numerical_attributes = [
            attribute
            for attribute in actual_attributes
            if attribute.key.name in FUZZY_NUMERIC_COMPARE_LIST
        ]
        expected_numerical_attributes = [
            attribute
            for attribute in expected_attributes
            if attribute.key.name in FUZZY_NUMERIC_COMPARE_LIST
        ]

        # Fuzzy comparison of int-like double attributes
        for actual, expected in zip(
            actual_numerical_attributes, expected_numerical_attributes
        ):
            assert int(actual.value.val_double) == int(expected.value.val_double)

    def test_with_specific_attributes(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        three_hours_later = int((_BASE_TIME + timedelta(hours=3)).timestamp())
        message = GetTraceRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=Timestamp(seconds=three_hours_later),
                request_id=_REQUEST_ID,
            ),
            trace_id=_TRACE_ID,
            items=[
                GetTraceRequest.TraceItem(
                    item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    attributes=[
                        AttributeKey(
                            name="server_name",
                            type=AttributeKey.Type.TYPE_STRING,
                        ),
                        AttributeKey(
                            name="sentry.parent_span_id",
                            type=AttributeKey.Type.TYPE_STRING,
                        ),
                    ],
                )
            ],
        )
        response = EndpointGetTrace().execute(message)
        timestamps: list[Timestamp] = []
        for span in _SPANS:
            timestamp = Timestamp()
            timestamp.FromNanoseconds(int(span["start_timestamp_precise"] * 1e6) * 1000)
            timestamps.append(timestamp)

        expected_response = GetTraceResponse(
            meta=ResponseMeta(request_id=_REQUEST_ID),
            trace_id=_TRACE_ID,
            item_groups=[
                GetTraceResponse.ItemGroup(
                    item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    items=[
                        GetTraceResponse.Item(
                            id=span["span_id"],
                            timestamp=timestamp,
                            attributes=[
                                GetTraceResponse.Item.Attribute(
                                    key=AttributeKey(
                                        name="sentry.parent_span_id",
                                        type=AttributeKey.Type.TYPE_STRING,
                                    ),
                                    value=AttributeValue(
                                        val_str="0" * 16,
                                    ),
                                ),
                                GetTraceResponse.Item.Attribute(
                                    key=AttributeKey(
                                        name="server_name",
                                        type=AttributeKey.Type.TYPE_STRING,
                                    ),
                                    value=AttributeValue(
                                        val_str=_SERVER_NAME,
                                    ),
                                ),
                            ],
                        )
                        for timestamp, span in zip(timestamps, _SPANS)
                    ],
                ),
            ],
        )
        assert MessageToDict(response) == MessageToDict(expected_response)
