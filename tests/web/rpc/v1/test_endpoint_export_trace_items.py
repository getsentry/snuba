import uuid
from datetime import timedelta
from typing import Any

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_items_pb2 import ExportTraceItemsRequest
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_pb2 import TraceItem

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web import QueryResult
from snuba.web.query import run_query
from snuba.web.rpc.v1.endpoint_export_trace_items import EndpointExportTraceItems
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import _DEFAULT_ATTRIBUTES, BASE_TIME, gen_item_message

_SPAN_COUNT = 120
_LOG_COUNT = 10
_REQUEST_ID = uuid.uuid4().hex
_SPANS_TRACE_IDS = [uuid.uuid4().hex for _ in range(_SPAN_COUNT)]
_SPANS_ITEM_IDS = [
    int(uuid.uuid4().hex[:16], 16).to_bytes(16, byteorder="little", signed=False)
    for _ in range(_SPAN_COUNT)
]
_LOGS_TRACE_IDS = [uuid.uuid4().hex for _ in range(_LOG_COUNT)]
_LOGS_ITEM_IDS = [
    int(uuid.uuid4().hex[:16], 16).to_bytes(16, byteorder="little", signed=False)
    for _ in range(_LOG_COUNT)
]

_SPANS = [
    gen_item_message(
        start_timestamp=BASE_TIME + timedelta(seconds=i),
        trace_id=_SPANS_TRACE_IDS[i],
        item_id=_SPANS_ITEM_IDS[i],
        project_id=i % 3 + 1,
    )
    for i in range(_SPAN_COUNT)  # 2 minutes
]
_LOGS = [
    gen_item_message(
        start_timestamp=BASE_TIME + timedelta(seconds=i),
        trace_id=_LOGS_TRACE_IDS[i],
        type=TraceItemType.TRACE_ITEM_TYPE_LOG,
        item_id=_LOGS_ITEM_IDS[i],
        project_id=i % 3 + 1,
    )
    for i in range(_LOG_COUNT)
]


def _assert_attributes_keys(trace_items: list[TraceItem]) -> None:
    for trace_item in trace_items:
        actual_keys = set(dict(trace_item.attributes).keys())
        expected_keys = set(
            _DEFAULT_ATTRIBUTES.keys()
            | {
                "sentry.end_timestamp_precise",
                "sentry.received",
                "sentry.start_timestamp_precise",
                "start_timestamp_ms",
                "sentry._internal.ingested_at",
            }
        )
        assert actual_keys == expected_keys


@pytest.fixture(autouse=False)
def setup_teardown(eap: None, redis_db: None) -> None:
    items_storage = get_storage(StorageKey("eap_items"))
    write_raw_unprocessed_events(items_storage, _SPANS)  # type: ignore
    write_raw_unprocessed_events(items_storage, _LOGS)  # type: ignore


@pytest.mark.eap
@pytest.mark.redis_db
class TestExportTraceItems(BaseApiTest):
    def test_timerange_without_data(self, setup_teardown: Any) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = ExportTraceItemsRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(
                    seconds=int((BASE_TIME - timedelta(seconds=10)).timestamp())
                ),
                end_timestamp=Timestamp(seconds=int((BASE_TIME).timestamp())),
                request_id=_REQUEST_ID,
            ),
        )
        response = EndpointExportTraceItems().execute(message)

        assert response.trace_items == []

    def test_with_pagination(self, setup_teardown: Any) -> None:
        response = None
        message = ExportTraceItemsRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int((BASE_TIME).timestamp())),
                end_timestamp=Timestamp(
                    seconds=int((BASE_TIME + timedelta(seconds=_SPAN_COUNT)).timestamp())
                ),
            ),
            limit=20,
        )
        items: list[TraceItem] = []
        for _ in range(1, 100):
            response = EndpointExportTraceItems().execute(message)
            items.extend(response.trace_items)
            if len(response.trace_items) == 20:
                assert response.page_token.end_pagination == False
            else:
                assert response.page_token.end_pagination == True
                break
            message.page_token.CopyFrom(response.page_token)

        _assert_attributes_keys(items)

        assert len(items) == _SPAN_COUNT + _LOG_COUNT

    def test_pagination_with_128_bit_item_id(self, eap: Any, redis_db: Any) -> None:
        num_items = 120
        trace_id = uuid.uuid4().hex
        items_data = [
            gen_item_message(
                start_timestamp=BASE_TIME,
                trace_id=trace_id,
                item_id=uuid.uuid4().int.to_bytes(16, byteorder="little"),
                project_id=1,
            )
            for _ in range(num_items)
        ]
        items_storage = get_storage(StorageKey("eap_items"))
        write_raw_unprocessed_events(items_storage, items_data)  # type: ignore

        message = ExportTraceItemsRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(
                    seconds=int((BASE_TIME + timedelta(seconds=1)).timestamp())
                ),
            ),
            limit=1,
        )
        items: list[TraceItem] = []
        seen_item_ids: set[bytes] = set()
        for _ in range(1, num_items + 2):
            response = EndpointExportTraceItems().execute(message)
            for item in response.trace_items:
                assert item.item_id not in seen_item_ids, (
                    f"item_id {item.item_id.hex()} returned more than once, "
                    f"pagination is not making progress"
                )
                seen_item_ids.add(item.item_id)
            items.extend(response.trace_items)
            if response.page_token.end_pagination:
                break
            message.page_token.CopyFrom(response.page_token)

        assert len(items) == num_items

    def test_no_transformation_on_order_by(self, setup_teardown: Any, monkeypatch: Any) -> None:
        # Wrap the real run_query to capture the actual QueryResult while still hitting ClickHouse.
        captured: dict[str, Any] = {}

        def wrapper(
            dataset: Any,
            request: Any,
            timer: Any,
            robust: bool = False,
            concurrent_queries_gauge: Any | None = None,
        ) -> QueryResult:
            qr = run_query(dataset, request, timer, robust, concurrent_queries_gauge)
            captured["query_result"] = qr
            return qr

        monkeypatch.setattr("snuba.web.rpc.v1.endpoint_export_trace_items.run_query", wrapper)

        message = ExportTraceItemsRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="integration-test",
                start_timestamp=Timestamp(seconds=int((BASE_TIME).timestamp())),
                end_timestamp=Timestamp(
                    seconds=int((BASE_TIME + timedelta(seconds=_SPAN_COUNT)).timestamp())
                ),
                request_id=_REQUEST_ID,
            ),
        )

        EndpointExportTraceItems().execute(message)

        qr = captured["query_result"]
        assert (
            "ORDER BY organization_id ASC, project_id ASC, item_type ASC, timestamp ASC, trace_id ASC, item_id ASC"
            in qr.extra["sql"]
        )
