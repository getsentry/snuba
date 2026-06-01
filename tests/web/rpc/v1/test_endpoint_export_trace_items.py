import re
import uuid
from collections import namedtuple
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_trace_items_pb2 import ExportTraceItemsRequest
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_pb2 import TraceItem

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web import QueryResult
from snuba.web.query import run_query
from snuba.web.rpc.storage_routing.routing_strategies.outcomes_flex_time import (
    OutcomesFlexTimeRoutingStrategy,
)
from snuba.web.rpc.v1.endpoint_export_trace_items import (
    EndpointExportTraceItems,
    FlexWindow,
    KeysetCursor,
)
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
                "sentry._internal.received_at",
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
                assert not response.page_token.end_pagination
            else:
                assert response.page_token.end_pagination
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

    def test_pagination_with_real_flex_window(
        self, eap: Any, redis_db: Any, monkeypatch: Any
    ) -> None:
        """
        Verify end-to-end flex-window pagination by capturing the full sequence of
        queries.

        Setup: 20 items [T0, T0+20s), limit=4, routing mock returns 20 outcomes for
        any window crossing T0+5s → narrows to [T0+5, T0+20). After that window is
        exhausted a window-only token advances to the earlier slice [T0, T0+5).
        """
        total = 20
        max_items = 15
        start_sec = int(BASE_TIME.timestamp())
        end_sec = start_sec + total
        # 20 outcomes / max 15 → factor 4/3 → routed window = last 15s = [T0+5, T0+20)
        routed_start_sec = start_sec + 5

        write_raw_unprocessed_events(
            get_storage(StorageKey("eap_items")),  # type: ignore[arg-type]
            [
                gen_item_message(
                    start_timestamp=BASE_TIME + timedelta(seconds=i),
                    item_id=int(uuid.uuid4().hex[:16], 16).to_bytes(16, byteorder="little"),
                    type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                    project_id=1,
                )
                for i in range(total)
            ],
        )
        OutcomesFlexTimeRoutingStrategy().set_config_value("max_items_to_query", max_items)

        # outcomes_hourly buckets by hour, so second-level splits are invisible to routing.
        # Mock the count directly so each sub-range sees the logically correct volume.
        def _mock_outcomes(self: Any, routing_context: Any, time_window: Any) -> int:
            tw_start = time_window.start_timestamp.seconds
            tw_end = time_window.end_timestamp.seconds
            if tw_start < routed_start_sec < tw_end:
                return 20  # crosses the T0+5 boundary: triggers narrowing
            if tw_start >= routed_start_sec:
                return 14  # entirely in [T0+5, T0+20): below max, no narrowing
            return 6  # entirely in [T0, T0+5): below max, no narrowing

        monkeypatch.setattr(
            OutcomesFlexTimeRoutingStrategy,
            "get_ingested_items_for_timerange",
            _mock_outcomes,
        )

        # Capture the actual time window each query ran against by parsing the SQL.
        sql_queried_windows: list[tuple[int, int]] = []

        def _capture_query(
            dataset: Any,
            request: Any,
            timer: Any,
            robust: bool = False,
            concurrent_queries_gauge: Any = None,
        ) -> QueryResult:
            qr = run_query(dataset, request, timer, robust, concurrent_queries_gauge)
            sql = qr.extra.get("sql", "")
            start_m = re.search(r"greaterOrEquals\(timestamp, toDateTime\('([^']+)'\)\)", sql)
            end_m = re.search(r"less\(timestamp, toDateTime\('([^']+)'\)\)", sql)
            if start_m and end_m:

                def _to_sec(s: str) -> int:
                    return int(
                        datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
                        .replace(tzinfo=timezone.utc)
                        .timestamp()
                    )

                sql_queried_windows.append((_to_sec(start_m.group(1)), _to_sec(end_m.group(1))))
            return qr

        monkeypatch.setattr(
            "snuba.web.rpc.v1.endpoint_export_trace_items.run_query", _capture_query
        )

        message = ExportTraceItemsRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=start_sec),
                end_timestamp=Timestamp(seconds=end_sec),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.Mode.MODE_HIGHEST_ACCURACY_FLEXTIME
                ),
                request_id=uuid.uuid4().hex,
            ),
            limit=4,
        )

        PageRecord = namedtuple(
            "PageRecord",
            ["window_start", "window_end", "items_count", "has_cursor", "end_pagination"],
        )
        records: list[Any] = []

        while True:
            response = EndpointExportTraceItems().execute(message)
            token = response.page_token
            filter_count = len(token.filter_offset.and_filter.filters)
            records.append(
                PageRecord(
                    window_start=sql_queried_windows[-1][0],
                    window_end=sql_queried_windows[-1][1],
                    items_count=len(response.trace_items),
                    has_cursor=filter_count == len(FlexWindow._fields) + len(KeysetCursor._fields),
                    end_pagination=token.end_pagination,
                )
            )
            if token.end_pagination:
                break
            message.page_token.CopyFrom(token)

        routed_pages = [r for r in records if r.window_start == routed_start_sec]
        earlier_pages = [r for r in records if r.window_start == start_sec]

        assert sum(r.items_count for r in records) == total, "not all items were returned"
        assert records.index(routed_pages[-1]) < records.index(earlier_pages[0]), (
            "routed window [T0+5, T0+20) must be fully exhausted before the earlier slice [T0, T0+5) begins"
        )
        assert sum(r.items_count for r in routed_pages) == 15, (
            "routed window [T0+5, T0+20) should contain exactly 15 items"
        )
        assert all(r.has_cursor for r in routed_pages[:-1]), (
            "every full page in the routed window carries a keyset cursor to continue within that window"
        )
        assert not routed_pages[-1].has_cursor, (
            "once the routed window is exhausted the token switches to window-only (no cursor) to advance to the earlier slice"
        )
        assert not routed_pages[-1].end_pagination, (
            "exhausting the routed window is not the end — the earlier slice [T0, T0+5) still needs to be fetched"
        )
        assert sum(r.items_count for r in earlier_pages) == 5, (
            "earlier slice [T0, T0+5) should contain the remaining 5 items"
        )
        assert all(r.has_cursor for r in earlier_pages[:-1]), (
            "every full page in the earlier slice carries a keyset cursor"
        )
        assert records[-1].end_pagination, (
            "pagination ends only after the earlier slice is fully consumed"
        )
