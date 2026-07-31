import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemColumnValues,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue

from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal
from snuba.query.expressions import Expression
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.common.pagination import FlexibleTimeWindowPageWithFilters
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import TimeWindow

_START = Timestamp(seconds=1_700_000_000)
_END = Timestamp(seconds=1_700_003_600)
_TIME_WINDOW = TimeWindow(start_timestamp=_START, end_timestamp=_END)

_SEQUENCE_ALIAS = "sentry.timestamp.sequence_TYPE_INT"
_ITEM_ID_ALIAS = "sentry.item_id_TYPE_STRING"


def _request() -> TraceItemTableRequest:
    columns = [
        Column(
            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.timestamp"),
            label="sentry.timestamp",
        ),
        Column(
            key=AttributeKey(type=AttributeKey.TYPE_INT, name="sentry.timestamp.sequence"),
            label="sentry.timestamp.sequence",
        ),
        Column(
            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.item_id"),
            label="sentry.item_id",
        ),
    ]
    return TraceItemTableRequest(
        meta=RequestMeta(
            project_ids=[1],
            organization_id=1,
            start_timestamp=_START,
            end_timestamp=_END,
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
        ),
        columns=columns,
        order_by=[TraceItemTableRequest.OrderBy(column=col, descending=True) for col in columns],
    )


def _results(sequence: AttributeValue) -> list[TraceItemColumnValues]:
    return [
        TraceItemColumnValues(
            attribute_name="sentry.timestamp",
            results=[AttributeValue(val_str="2025-10-06 14:00:00")],
        ),
        TraceItemColumnValues(
            attribute_name="sentry.timestamp.sequence",
            results=[sequence],
        ),
        TraceItemColumnValues(
            attribute_name="sentry.item_id",
            results=[AttributeValue(val_str="deadbeef")],
        ),
    ]


def _expected_filters(sequence_bookmark: int) -> Expression:
    return f.less(
        f.tuple(
            column("timestamp"),
            f.ifNull(column(_SEQUENCE_ALIAS), literal(0)),
            # `sentry.item_id` is a normalized column, never NULL, so it needs no sentinel.
            column(_ITEM_ID_ALIAS),
        ),
        f.tuple(
            f.toDateTime("2025-10-06 14:00:00"),
            literal(sequence_bookmark),
            literal("deadbeef"),
        ),
    )


class TestFlexibleTimeWindowPageWithFilters:
    def test_compares_the_last_value_when_the_order_by_attribute_is_present(self) -> None:
        page = FlexibleTimeWindowPageWithFilters.create(
            _request(), _TIME_WINDOW, _results(AttributeValue(val_int=7))
        )

        assert page.get_filters() == _expected_filters(7)

    def test_compares_the_null_sentinel_when_the_order_by_attribute_is_absent(self) -> None:
        page = FlexibleTimeWindowPageWithFilters.create(
            _request(), _TIME_WINDOW, _results(AttributeValue(is_null=True))
        )

        assert page.get_filters() == _expected_filters(0)

    def test_rejects_a_null_bookmark_whose_page_token_carries_no_attribute_type(self) -> None:
        page = FlexibleTimeWindowPageWithFilters.create(
            _request(), _TIME_WINDOW, _results(AttributeValue(is_null=True))
        )
        for filter in page.page_token.filter_offset.and_filter.filters:
            filter.comparison_filter.key.ClearField("type")

        with pytest.raises(BadSnubaRPCRequestException):
            FlexibleTimeWindowPageWithFilters(page.page_token).get_filters()
