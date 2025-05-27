import uuid
from datetime import datetime, timedelta, timezone
from operator import attrgetter
from typing import Any

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
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, TraceItem

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_get_trace import EndpointGetTrace
from snuba.web.rpc.v1.resolvers.R_eap_spans.resolver_get_trace import (
    _value_to_attribute,
)
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import SERVER_NAME, gen_item_message

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
        item_id=int(uuid.uuid4().hex[:16], 16).to_bytes(
            16,
            byteorder="little",
            signed=False,
        ),
        attributes={
            "sentry.op": AnyValue(string_value="http.server" if i == 0 else "db"),
            "sentry.raw_description": AnyValue(
                string_value="root" if i == 0 else f"child {i + 1} of {_SPAN_COUNT}",
            ),
            "sentry.is_segment": AnyValue(bool_value=i == 0),
        },
    )
    for i in range(_SPAN_COUNT)
]

_PROTOBUF_TO_SENTRY_PROTOS = {
    "string_value": ("val_str", AttributeKey.Type.TYPE_STRING),
    "double_value": ("val_double", AttributeKey.Type.TYPE_DOUBLE),
    # we store integers as double
    "int_value": ("val_double", AttributeKey.Type.TYPE_DOUBLE),
    # we store boolean as double
    "bool_value": ("val_double", AttributeKey.Type.TYPE_DOUBLE),
}


def get_attributes(
    span: TraceItem,
) -> list[GetTraceResponse.Item.Attribute]:
    attributes: list[GetTraceResponse.Item.Attribute] = [
        GetTraceResponse.Item.Attribute(
            key=AttributeKey(
                name="sampling_factor",
                type=AttributeKey.Type.TYPE_DOUBLE,
            ),
            value=AttributeValue(val_double=1.0),
        ),
    ]

    for key in {"organization_id", "project_id", "trace_id"}:
        attribute_key, attribute_value = _value_to_attribute(key, getattr(span, key))
        attributes.append(
            GetTraceResponse.Item.Attribute(
                key=attribute_key,
                value=attribute_value,
            )
        )
    for key, value in span.attributes.items():
        value_type = value.WhichOneof("value")
        if value_type:
            attribute_key = AttributeKey(
                name=key,
                type=_PROTOBUF_TO_SENTRY_PROTOS[value_type][1],
            )
            args = {
                _PROTOBUF_TO_SENTRY_PROTOS[value_type][0]: getattr(value, value_type)
            }
        else:
            continue

        attribute_value = AttributeValue(**args)
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

    def test_with_data_all_attributes(self, setup_teardown: Any) -> None:
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
        spans, timestamps = generate_spans_and_timestamps()
        expected_response = GetTraceResponse(
            meta=ResponseMeta(request_id=_REQUEST_ID),
            trace_id=_TRACE_ID,
            item_groups=[
                GetTraceResponse.ItemGroup(
                    item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    items=[
                        GetTraceResponse.Item(
                            id=get_span_id(span),
                            timestamp=timestamp,
                            attributes=sorted(
                                get_attributes(span),
                                key=attrgetter("key.name"),
                            ),
                        )
                        for timestamp, span in zip(timestamps, spans)
                    ],
                ),
            ],
        )

        assert MessageToDict(response) == MessageToDict(expected_response)

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
        spans, timestamps = generate_spans_and_timestamps()
        expected_response = GetTraceResponse(
            meta=ResponseMeta(request_id=_REQUEST_ID),
            trace_id=_TRACE_ID,
            item_groups=[
                GetTraceResponse.ItemGroup(
                    item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    items=[
                        GetTraceResponse.Item(
                            id=get_span_id(span),
                            timestamp=timestamp,
                            attributes=[
                                GetTraceResponse.Item.Attribute(
                                    key=AttributeKey(
                                        name="sentry.parent_span_id",
                                        type=AttributeKey.Type.TYPE_STRING,
                                    ),
                                    value=AttributeValue(
                                        val_str="",
                                    ),
                                ),
                                GetTraceResponse.Item.Attribute(
                                    key=AttributeKey(
                                        name="server_name",
                                        type=AttributeKey.Type.TYPE_STRING,
                                    ),
                                    value=AttributeValue(
                                        val_str=SERVER_NAME,
                                    ),
                                ),
                            ],
                        )
                        for timestamp, span in zip(timestamps, spans)
                    ],
                ),
            ],
        )
        assert MessageToDict(response) == MessageToDict(expected_response)


def generate_spans_and_timestamps() -> tuple[list[TraceItem], list[Timestamp]]:
    timestamps: list[Timestamp] = []
    spans: list[TraceItem] = []
    for payload in _SPANS:
        span = TraceItem()
        span.ParseFromString(payload)
        timestamp = Timestamp()
        timestamp.FromNanoseconds(
            int(span.attributes["sentry.start_timestamp_precise"].double_value * 1e6)
            * 1000
        )
        timestamps.append(timestamp)
        spans.append(span)
    return spans, timestamps


def get_span_id(span: TraceItem) -> str:
    # cut the 0x prefix
    return hex(int.from_bytes(span.item_id, byteorder="little", signed=False,))[
        2:
    ].rjust(16, "0")
