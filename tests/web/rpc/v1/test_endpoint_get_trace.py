import uuid
from datetime import datetime, timedelta, timezone
from operator import attrgetter
from typing import Any
from unittest.mock import patch

import pytest
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_get_trace_pb2 import (
    GetTraceRequest,
    GetTraceResponse,
)
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1.request_common_pb2 import (
    PageToken,
    QueryInfo,
    QueryMetadata,
    QueryStats,
    RequestMeta,
    ResponseMeta,
    TraceItemType,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    Array,
    AttributeKey,
    AttributeValue,
)
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, TraceItem

from snuba import state
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.settings import ENABLE_TRACE_PAGINATION_DEFAULT
from snuba.web import QueryResult
from snuba.web.query import run_query
from snuba.web.rpc.v1.endpoint_get_trace import (
    APPLY_FINAL_ROLLOUT_PERCENTAGE_CONFIG_KEY,
    EndpointGetTrace,
    _build_query,
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
_LOGS = [
    gen_item_message(
        start_timestamp=_BASE_TIME + timedelta(minutes=i),
        trace_id=_TRACE_ID,
        type=TraceItemType.TRACE_ITEM_TYPE_LOG,
        item_id=int(uuid.uuid4().hex[:16], 16).to_bytes(
            16,
            byteorder="little",
            signed=False,
        ),
    )
    for i in range(10)
]

_PROTOBUF_TO_SENTRY_PROTOS: dict[str, tuple[str, AttributeKey.Type.ValueType]] = {
    "string_value": ("val_str", AttributeKey.Type.TYPE_STRING),
    "double_value": ("val_double", AttributeKey.Type.TYPE_DOUBLE),
    "int_value": ("val_int", AttributeKey.Type.TYPE_INT),
    "bool_value": ("val_bool", AttributeKey.Type.TYPE_BOOLEAN),
    "array_value": ("val_array", AttributeKey.Type.TYPE_ARRAY),
}


def get_attributes(
    span: TraceItem,
) -> list[GetTraceResponse.Item.Attribute]:
    attributes: dict[str, GetTraceResponse.Item.Attribute] = {
        "sampling_factor": GetTraceResponse.Item.Attribute(
            key=AttributeKey(
                name="sampling_factor",
                type=AttributeKey.Type.TYPE_DOUBLE,
            ),
            value=AttributeValue(val_double=1.0),
        ),
    }

    for key in {"organization_id", "project_id", "trace_id"}:
        attribute_key, attribute_value = _value_to_attribute(key, getattr(span, key))
        attributes[key] = GetTraceResponse.Item.Attribute(
            key=attribute_key,
            value=attribute_value,
        )

    def _convert_to_attribute_value(value: AnyValue) -> AttributeValue:
        value_type = value.WhichOneof("value")
        if value_type:
            arg_name = _PROTOBUF_TO_SENTRY_PROTOS[str(value_type)][0]
            arg_value = getattr(value, value_type)
            if value_type == "array_value":
                arg_value = Array(values=[_convert_to_attribute_value(v) for v in arg_value.values])
            args = {arg_name: arg_value}
        else:
            args = {"is_null": True}

        return AttributeValue(**args)

    for key, value in span.attributes.items():
        value_type = value.WhichOneof("value")
        if not value_type:
            continue
        attribute_key = AttributeKey(
            name=key,
            type=_PROTOBUF_TO_SENTRY_PROTOS[str(value_type)][1],
        )
        attribute_value = _convert_to_attribute_value(value)
        attributes[key] = GetTraceResponse.Item.Attribute(
            key=attribute_key,
            value=attribute_value,
        )
    return list(attributes.values())


@pytest.fixture(autouse=False)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    items_storage = get_storage(StorageKey("eap_items"))

    write_raw_unprocessed_events(items_storage, _SPANS)  # type: ignore
    write_raw_unprocessed_events(items_storage, _LOGS)  # type: ignore


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
        response = self.app.post("/rpc/EndpointGetTrace/v1", data=message.SerializeToString())
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
            meta=ResponseMeta(
                request_id=_REQUEST_ID,
                query_info=[
                    QueryInfo(
                        stats=QueryStats(
                            progress_bytes=response.meta.query_info[0].stats.progress_bytes
                        ),
                        metadata=QueryMetadata(),
                        trace_logs="",
                    )
                ],
            ),
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
            page_token=(
                PageToken(end_pagination=True)
                if state.get_int_config("enable_trace_pagination", ENABLE_TRACE_PAGINATION_DEFAULT)
                else None
            ),
        )
        response_dict = MessageToDict(response)
        for item_group in response_dict["itemGroups"]:
            for item in item_group["items"]:
                item["attributes"] = [
                    attr
                    for attr in item["attributes"]
                    if not attr["key"]["name"].startswith("sentry._internal")
                ]

        assert response_dict == MessageToDict(expected_response)

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
            meta=ResponseMeta(
                request_id=_REQUEST_ID,
                query_info=[
                    QueryInfo(
                        stats=QueryStats(
                            progress_bytes=response.meta.query_info[0].stats.progress_bytes
                        ),
                        metadata=QueryMetadata(),
                        trace_logs="",
                    )
                ],
            ),
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
            page_token=(
                PageToken(end_pagination=True)
                if state.get_int_config("enable_trace_pagination", ENABLE_TRACE_PAGINATION_DEFAULT)
                else None
            ),
        )
        assert MessageToDict(response) == MessageToDict(expected_response)

    def test_build_query_with_final(store_outcomes_data: Any) -> None:
        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        three_hours_later = int((_BASE_TIME + timedelta(hours=3)).timestamp())
        item = GetTraceRequest.TraceItem(
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
            items=[item],
        )

        state.set_config(
            APPLY_FINAL_ROLLOUT_PERCENTAGE_CONFIG_KEY,
            1.0,
        )

        query = _build_query(message, item)

        assert query.get_final() == True

        state.set_config(
            APPLY_FINAL_ROLLOUT_PERCENTAGE_CONFIG_KEY,
            0.0,
        )

        query = _build_query(message, item)

        assert query.get_final() == False

    def test_with_logs(self, setup_teardown: Any) -> None:
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
                    item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                    attributes=[
                        AttributeKey(
                            name="sentry.item_id",
                            type=AttributeKey.Type.TYPE_STRING,
                        ),
                    ],
                )
            ],
        )
        response = EndpointGetTrace().execute(message)
        logs, timestamps = generate_logs_and_timestamps()
        expected_response = GetTraceResponse(
            meta=ResponseMeta(
                request_id=_REQUEST_ID,
                query_info=[
                    QueryInfo(
                        stats=QueryStats(
                            progress_bytes=response.meta.query_info[0].stats.progress_bytes
                        ),
                        metadata=QueryMetadata(),
                        trace_logs="",
                    )
                ],
            ),
            trace_id=_TRACE_ID,
            item_groups=[
                GetTraceResponse.ItemGroup(
                    item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                    items=[
                        GetTraceResponse.Item(
                            id=get_span_id(log),
                            timestamp=timestamp,
                            attributes=[
                                GetTraceResponse.Item.Attribute(
                                    key=AttributeKey(
                                        name="sentry.item_id",
                                        type=AttributeKey.Type.TYPE_STRING,
                                    ),
                                    value=AttributeValue(
                                        val_str=get_span_id(log),
                                    ),
                                ),
                            ],
                        )
                        for timestamp, log in zip(timestamps, logs)
                    ],
                ),
            ],
            page_token=(
                PageToken(end_pagination=True)
                if state.get_int_config("enable_trace_pagination", ENABLE_TRACE_PAGINATION_DEFAULT)
                else None
            ),
        )
        assert MessageToDict(response) == MessageToDict(expected_response)


def generate_spans_and_timestamps() -> tuple[list[TraceItem], list[Timestamp]]:
    return generate_items_and_timestamps(_SPANS)


def generate_logs_and_timestamps() -> tuple[list[TraceItem], list[Timestamp]]:
    return generate_items_and_timestamps(_LOGS)


def generate_items_and_timestamps(payloads: list[bytes]) -> tuple[list[TraceItem], list[Timestamp]]:
    timestamps: list[Timestamp] = []
    items: list[TraceItem] = []
    for payload in payloads:
        item = TraceItem()
        item.ParseFromString(payload)
        timestamp = Timestamp()
        timestamp.FromNanoseconds(
            int(item.attributes["sentry.start_timestamp_precise"].double_value * 1e6) * 1000
        )
        timestamps.append(timestamp)
        items.append(item)
    return items, timestamps


def get_span_id(span: TraceItem) -> str:
    # cut the 0x prefix
    return hex(
        int.from_bytes(
            span.item_id,
            byteorder="little",
            signed=False,
        )
    )[
        2:
    ].rjust(16, "0")


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestGetTracePagination(BaseApiTest):
    def test_pagination_with_user_limit(self, setup_teardown: Any) -> None:
        """Test that pagination respects user-provided limit"""
        state.set_config("enable_trace_pagination", 1)
        ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
        three_hours_later = int((_BASE_TIME + timedelta(hours=3)).timestamp())
        mylimit = 10

        # Request with a limit of 10 spans (less than the 120 available)
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
            limit=mylimit,
            items=[
                GetTraceRequest.TraceItem(
                    item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    attributes=[
                        AttributeKey(
                            name="sentry.item_id",
                            type=AttributeKey.Type.TYPE_STRING,
                        ),
                    ],
                ),
                GetTraceRequest.TraceItem(
                    item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                    attributes=[
                        AttributeKey(
                            name="sentry.item_id",
                            type=AttributeKey.Type.TYPE_STRING,
                        ),
                    ],
                ),
            ],
        )
        items_received = set[str]()
        while True:
            response = EndpointGetTrace().execute(message)
            curr_response_len = 0
            for group in response.item_groups:
                for item in group.items:
                    curr_response_len += 1
                    assert item.id not in items_received
                    items_received.add(item.id)
            assert curr_response_len <= mylimit
            if curr_response_len < mylimit:
                assert response.page_token.end_pagination == True
            if response.page_token.end_pagination:
                break
            message.page_token.CopyFrom(response.page_token)
        assert len(items_received) == len(_SPANS) + len(_LOGS)

    def test_pagination_with_no_user_limit(self, setup_teardown: Any) -> None:
        """Test that pagination respects uses the default max items value when no user limit is provided"""
        configmax = 9
        with patch(
            "snuba.web.rpc.v1.endpoint_get_trace.ENDPOINT_GET_TRACE_PAGINATION_MAX_ITEMS", configmax
        ):
            state.set_config("enable_trace_pagination", 1)
            """
            import snuba.web.rpc.v1.endpoint_get_trace as endpoint_get_trace

            reload(endpoint_get_trace)
            from snuba.web.rpc.v1.endpoint_get_trace import (
                EndpointGetTrace,
            )
            """

            ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
            three_hours_later = int((_BASE_TIME + timedelta(hours=3)).timestamp())

            # Request with a limit of 10 spans (less than the 120 available)
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
                                name="sentry.item_id",
                                type=AttributeKey.Type.TYPE_STRING,
                            ),
                        ],
                    ),
                    GetTraceRequest.TraceItem(
                        item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                        attributes=[
                            AttributeKey(
                                name="sentry.item_id",
                                type=AttributeKey.Type.TYPE_STRING,
                            ),
                        ],
                    ),
                ],
            )
            items_received = set[str]()
            while True:
                response = EndpointGetTrace().execute(message)
                curr_response_len = 0
                for group in response.item_groups:
                    for item in group.items:
                        curr_response_len += 1
                        assert item.id not in items_received
                        items_received.add(item.id)
                assert curr_response_len <= configmax
                if curr_response_len < configmax:
                    assert response.page_token.end_pagination == True
                if response.page_token.end_pagination:
                    break
                message.page_token.CopyFrom(response.page_token)
            assert len(items_received) == len(_SPANS) + len(_LOGS)

    def test_no_transformation_on_order_by(self, setup_teardown: Any, monkeypatch: Any) -> None:
        # Wrap the real run_query to capture the actual QueryResult while still hitting ClickHouse.
        captured: dict[str, Any] = {}

        def wrapper(dataset, request, timer, robust: bool = False, concurrent_queries_gauge=None) -> QueryResult:  # type: ignore[no-untyped-def]
            qr = run_query(dataset, request, timer, robust, concurrent_queries_gauge)
            captured["query_result"] = qr
            return qr

        monkeypatch.setattr("snuba.web.rpc.v1.endpoint_get_trace.run_query", wrapper)

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

        EndpointGetTrace().execute(message)
        breakpoint()

        qr = captured["query_result"]
        assert (
            "ORDER BY organization_id ASC, project_id ASC, item_type ASC, timestamp ASC, trace_id ASC, item_id ASC"
            in qr.extra["sql"]
        )
