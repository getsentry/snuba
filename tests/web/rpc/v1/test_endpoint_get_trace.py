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
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_get_trace import EndpointGetTrace
from snuba.web.rpc.v1.resolvers.R_eap_spans.resolver_get_trace import (
    NORMALIZED_COLUMNS_TO_INCLUDE,
)
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import gen_item_message

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
_SPANS = [
    gen_item_message(
        start_timestamp=_BASE_TIME + timedelta(minutes=i),
        trace_id=_TRACE_ID,
        attributes={
            "span_op": AnyValue(string_value="http.server" if i == 0 else "db"),
            "span_name": AnyValue(
                string_value="root" if i == 0 else f"child {i + 1} of {_SPAN_COUNT}",
            ),
            "is_segment": AnyValue(bool_value=i == 0),
        },
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

        assert list(
            [
                attribute
                for attribute in response.item_groups[0].items[0].attributes
                if attribute.key.name not in NORMALIZED_COLUMNS_TO_INCLUDE
            ]
        ) == list(expected_response.item_groups[0].items[0].attributes)
        assert set(
            [
                attribute.key.name
                for attribute in response.item_groups[0].items[0].attributes
                if attribute.key.name in NORMALIZED_COLUMNS_TO_INCLUDE
            ]
        ) == set(NORMALIZED_COLUMNS_TO_INCLUDE)

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
