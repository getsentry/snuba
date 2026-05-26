import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, NamedTuple, Type, cast

import sentry_sdk
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_trace_items_pb2 import (
    ExportTraceItemsRequest,
    ExportTraceItemsResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import PageToken, RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AndFilter,
    ComparisonFilter,
    TraceItemFilter,
)
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, ArrayValue, TraceItem

from snuba import state
from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal
from snuba.query.expressions import FunctionCall
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.common import (
    BUCKET_COUNT,
    attribute_key_to_expression,
    base_conditions_and,
    process_arrays,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import setup_trace_query_settings
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingDecision,
    TimeWindow,
)

_DEFAULT_PAGE_SIZE = 10_000
FLEX_WIN_START = "sentry__time_window.start_timestamp"
FLEX_WIN_END = "sentry__time_window.end_timestamp"

_KEYSET_CURSOR_SCHEMA = [
    ("last_seen_project_id", AttributeKey.Type.TYPE_INT, "val_int"),
    ("last_seen_item_type", AttributeKey.Type.TYPE_INT, "val_int"),
    ("last_seen_timestamp", AttributeKey.Type.TYPE_INT, "val_int"),
    ("last_seen_trace_id", AttributeKey.Type.TYPE_STRING, "val_str"),
    ("last_seen_item_id", AttributeKey.Type.TYPE_STRING, "val_str"),
]


class FlexWindow(NamedTuple):
    """Flex window start and end timestamps"""

    start_sec: int
    end_sec: int

    @classmethod
    def from_filters(cls, filters: list[TraceItemFilter]) -> "FlexWindow":
        _SCHEMA = [
            (FLEX_WIN_START, "val_int"),
            (FLEX_WIN_END, "val_int"),
        ]
        values = []
        for expected_filter, timestamp_filter in zip(_SCHEMA, filters):
            filter_name, filter_value_type = expected_filter
            if (
                not timestamp_filter.HasField("comparison_filter")
                or timestamp_filter.comparison_filter.key.name != filter_name
                or not timestamp_filter.comparison_filter.value.HasField(filter_value_type)  # type: ignore[arg-type]
            ):
                raise ValueError("Invalid timestamp filter in page token")
            values.append(timestamp_filter.comparison_filter.value.val_int)
        start_sec, end_sec = values
        return cls(start_sec=start_sec, end_sec=end_sec)

    def to_filters(self) -> list[TraceItemFilter]:
        return [
            TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(name=FLEX_WIN_START),
                    op=ComparisonFilter.OP_GREATER_THAN_OR_EQUALS,
                    value=AttributeValue(val_int=self.start_sec),
                )
            ),
            TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(name=FLEX_WIN_END),
                    op=ComparisonFilter.OP_LESS_THAN,
                    value=AttributeValue(val_int=self.end_sec),
                )
            ),
        ]


class KeysetCursor(NamedTuple):
    last_seen_project_id: int
    last_seen_item_type: TraceItemType.ValueType
    last_seen_timestamp: int
    last_seen_trace_id: str
    last_seen_item_id: str

    @classmethod
    def from_filters(cls, filters: list[TraceItemFilter]) -> "KeysetCursor":
        if len(filters) != len(_KEYSET_CURSOR_SCHEMA):
            raise ValueError("Invalid keyset cursor in page token")

        values = []
        for i, (expected_name, expected_type, val_field) in enumerate(_KEYSET_CURSOR_SCHEMA):
            cf = filters[i].comparison_filter
            if cf.key.name != expected_name or cf.key.type != expected_type:
                raise ValueError(f"Invalid {expected_name} in page token")
            values.append(getattr(cf.value, val_field))

        project_id, item_type, timestamp, trace_id, item_id = values
        return cls(
            last_seen_project_id=project_id,
            last_seen_item_type=cast(TraceItemType.ValueType, item_type),
            last_seen_timestamp=timestamp,
            last_seen_trace_id=trace_id,
            last_seen_item_id=item_id,
        )

    def to_filters(self) -> list[TraceItemFilter]:
        return [
            TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(name=name, type=attr_type),
                    op=ComparisonFilter.OP_EQUALS,
                    value=AttributeValue(**{val_field: value}),  # type: ignore[arg-type]
                )
            )
            for (name, attr_type, val_field), value in zip(_KEYSET_CURSOR_SCHEMA, self)
        ]


class ExportTraceItemsPageToken:
    """Pagination cursor: FlexWindow plus optional KeysetCursor."""

    def __init__(
        self,
        *,
        flex_window: FlexWindow,
        cursor: KeysetCursor | None = None,
    ):
        self.keyset_cursor = cursor
        self.flex_window = flex_window

    @classmethod
    def from_protobuf(cls, page_token: PageToken) -> "ExportTraceItemsPageToken | None":
        if page_token == PageToken():
            return None
        if not page_token.filter_offset.HasField("and_filter"):
            raise ValueError("Invalid page token")
        filters = list(page_token.filter_offset.and_filter.filters)

        flex_field_count = len(FlexWindow._fields)
        cursor_field_count = len(KeysetCursor._fields)
        if len(filters) not in (flex_field_count, flex_field_count + cursor_field_count):
            raise ValueError(
                f"Invalid page token: expected {flex_field_count} or {flex_field_count + cursor_field_count} "
                f"filter clauses, got {len(filters)}"
            )
        flex_filters = filters[:flex_field_count]
        cursor_filters = filters[flex_field_count:]
        flex_window = FlexWindow.from_filters(flex_filters)
        keyset_cursor = KeysetCursor.from_filters(cursor_filters) if cursor_filters else None
        return cls(
            flex_window=flex_window,
            cursor=keyset_cursor,
        )

    def to_protobuf(self) -> PageToken:
        and_filters: list[TraceItemFilter] = self.flex_window.to_filters()
        if self.keyset_cursor is not None:
            and_filters.extend(self.keyset_cursor.to_filters())

        return PageToken(
            filter_offset=TraceItemFilter(
                and_filter=AndFilter(filters=and_filters),
            )
        )


def _next_page_token(
    *,
    items_returned: int,
    limit: int,
    flex_window: FlexWindow,
    keyset_cursor: KeysetCursor,
    is_flex: bool,
    orig_start: int,
    routed_window: TimeWindow | None,
) -> PageToken:
    if items_returned >= limit:
        return ExportTraceItemsPageToken(
            flex_window=flex_window, cursor=keyset_cursor
        ).to_protobuf()

    if is_flex and routed_window is not None:
        routed_start = routed_window.start_timestamp.seconds
        routed_end = routed_window.end_timestamp.seconds
        if orig_start < routed_start < routed_end:
            return ExportTraceItemsPageToken(
                flex_window=FlexWindow(start_sec=orig_start, end_sec=routed_start),
            ).to_protobuf()

    return PageToken(end_pagination=True)


def _is_flextime_export(in_msg: ExportTraceItemsRequest) -> bool:
    if not in_msg.meta.HasField("downsampled_storage_config"):
        return False
    return (
        in_msg.meta.downsampled_storage_config.mode
        == DownsampledStorageConfig.Mode.MODE_HIGHEST_ACCURACY_FLEXTIME
    )


def _export_query_meta(
    in_msg: ExportTraceItemsRequest, routing_decision: RoutingDecision
) -> RequestMeta:
    if routing_decision.time_window is None:
        return in_msg.meta
    meta = RequestMeta()
    meta.CopyFrom(in_msg.meta)
    meta.start_timestamp.CopyFrom(routing_decision.time_window.start_timestamp)
    meta.end_timestamp.CopyFrom(routing_decision.time_window.end_timestamp)
    return meta


def _build_query(
    in_msg: ExportTraceItemsRequest,
    limit: int,
    page_token: ExportTraceItemsPageToken | None = None,
    query_meta: RequestMeta | None = None,
) -> Query:
    selected_columns = [
        SelectedExpression("timestamp", f.toUnixTimestamp(column("timestamp"), alias="timestamp")),
        SelectedExpression(
            name="id",
            expression=(
                attribute_key_to_expression(
                    AttributeKey(name="sentry.item_id", type=AttributeKey.Type.TYPE_STRING)
                )
            ),
        ),
        SelectedExpression(
            "trace_id",
            column("trace_id", alias="trace_id"),
        ),
        SelectedExpression("organization_id", column("organization_id", alias="organization_id")),
        SelectedExpression("project_id", column("project_id", alias="project_id")),
        SelectedExpression("item_type", column("item_type", alias="item_type")),
        SelectedExpression(
            "client_sample_rate", column("client_sample_rate", alias="client_sample_rate")
        ),
        SelectedExpression(
            "server_sample_rate", column("server_sample_rate", alias="server_sample_rate")
        ),
        SelectedExpression("sampling_weight", column("sampling_weight", alias="sampling_weight")),
        SelectedExpression("sampling_factor", column("sampling_factor", alias="sampling_factor")),
        SelectedExpression(
            "attributes_string",
            f.mapConcat(
                *[column(f"attributes_string_{n}") for n in range(BUCKET_COUNT)],
                alias="attributes_string",
            ),
        ),
        SelectedExpression(
            "attributes_float",
            f.mapConcat(
                *[column(f"attributes_float_{n}") for n in range(BUCKET_COUNT)],
                alias="attributes_float",
            ),
        ),
        SelectedExpression("attributes_int", column("attributes_int", alias="attributes_int")),
        SelectedExpression("attributes_bool", column("attributes_bool", alias="attributes_bool")),
        SelectedExpression(
            "attributes_array",
            FunctionCall("attributes_array", "toJSONString", (column("attributes_array"),)),
        ),
    ]

    entity = Entity(
        key=EntityKey("eap_items"),
        schema=get_entity(EntityKey("eap_items")).get_data_model(),
        sample=None,
    )

    page_token_filter = (
        [
            f.greater(
                f.tuple(
                    column("project_id"),
                    column("item_type"),
                    column("timestamp"),
                    column("trace_id"),
                    column("item_id"),
                ),
                f.tuple(
                    literal(page_token.keyset_cursor.last_seen_project_id),
                    literal(page_token.keyset_cursor.last_seen_item_type),
                    literal(page_token.keyset_cursor.last_seen_timestamp),
                    literal(page_token.keyset_cursor.last_seen_trace_id),
                    literal(page_token.keyset_cursor.last_seen_item_id),
                ),
            )
        ]
        if page_token is not None and page_token.keyset_cursor is not None
        else []
    )
    meta = query_meta if query_meta is not None else in_msg.meta
    item_type_filter = []
    if meta.trace_item_type != TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED:
        item_type_filter.append(f.equals(column("item_type"), literal(meta.trace_item_type)))
    query = Query(
        from_clause=entity,
        selected_columns=selected_columns,
        condition=base_conditions_and(meta, *page_token_filter, *item_type_filter),
        order_by=[
            # we add organization_id and project_id to the order by to optimize data reading
            # https://clickhouse.com/docs/sql-reference/statements/select/order-by#optimization-of-data-reading
            OrderBy(direction=OrderByDirection.ASC, expression=column("organization_id")),
            OrderBy(direction=OrderByDirection.ASC, expression=column("project_id")),
            OrderBy(direction=OrderByDirection.ASC, expression=column("item_type")),
            OrderBy(direction=OrderByDirection.ASC, expression=column("timestamp")),
            OrderBy(direction=OrderByDirection.ASC, expression=column("trace_id")),
            OrderBy(direction=OrderByDirection.ASC, expression=column("item_id")),
        ],
        limit=limit,
    )

    treeify_or_and_conditions(query)
    return query


def _build_snuba_request(
    in_msg: ExportTraceItemsRequest,
    routing_decision: RoutingDecision,
    limit: int,
    page_token: ExportTraceItemsPageToken | None = None,
) -> SnubaRequest:
    query_settings = setup_trace_query_settings() if in_msg.meta.debug else HTTPQuerySettings()
    try:
        routing_decision.strategy.merge_clickhouse_settings(routing_decision, query_settings)
        query_settings.set_sampling_tier(routing_decision.tier)
    except Exception as e:
        sentry_sdk.capture_message(f"Error merging clickhouse settings: {e}")

    query_settings.set_skip_transform_order_by(True)
    return SnubaRequest(
        id=uuid.UUID(in_msg.meta.request_id),
        original_body=MessageToDict(in_msg),
        query=_build_query(
            in_msg,
            limit,
            page_token,
            query_meta=_export_query_meta(in_msg, routing_decision),
        ),
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            referrer=in_msg.meta.referrer,
            team="eap",
            feature="eap",
            tenant_ids={
                "organization_id": in_msg.meta.organization_id,
                "referrer": in_msg.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api="eap_gdpr_export",
        ),
    )


def _to_any_value(value: Any) -> AnyValue:
    if isinstance(value, bool):
        return AnyValue(bool_value=value)
    elif isinstance(value, int):
        return AnyValue(int_value=value)
    elif isinstance(value, float):
        return AnyValue(double_value=value)
    elif isinstance(value, str):
        return AnyValue(string_value=value)
    elif isinstance(value, list):
        return AnyValue(array_value=ArrayValue(values=[_to_any_value(v) for v in value]))
    elif isinstance(value, datetime):
        return AnyValue(double_value=value.timestamp())
    else:
        raise BadSnubaRPCRequestException(f"data type unknown: {type(value)}")


class ProcessedResults(NamedTuple):
    items: list[TraceItem]
    keyset_cursor: KeysetCursor


def _convert_rows(rows: Iterable[Dict[str, Any]]) -> ProcessedResults:
    items: list[TraceItem] = []
    last_seen_project_id = 0
    last_seen_item_type = TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED
    last_seen_timestamp = 0
    last_seen_trace_id = ""
    last_seen_item_id = ""

    for row in rows:
        org_id = row.pop("organization_id")
        proj_id = row.pop("project_id")
        trace_id = row.pop("trace_id")
        item_id = row.pop("id")
        item_type = row.pop("item_type")
        ts = row.pop("timestamp")
        client_sample_rate = row.pop("client_sample_rate", 1.0)
        server_sample_rate = row.pop("server_sample_rate", 1.0)
        row.pop("sampling_factor", 1.0)
        row.pop("sampling_weight", 1.0)
        arrays = row.pop("attributes_array", "{}") or "{}"
        booleans = row.pop("attributes_bool", {}) or {}
        integers = row.pop("attributes_int", {}) or {}
        floats = row.pop("attributes_float", {}) or {}

        attributes_map: dict[str, AnyValue] = {}

        for row_key, row_value in row.items():
            if isinstance(row_value, dict):
                for column_key, column_value in row_value.items():
                    attributes_map[column_key] = _to_any_value(column_value)
            else:
                attributes_map[row_key] = _to_any_value(row_value)

        for source in (process_arrays(arrays), booleans, integers, floats):
            for key, value in source.items():
                attributes_map[key] = _to_any_value(value)

        timestamp = Timestamp()
        timestamp.FromNanoseconds(int(ts * 1e6) * 1000)

        item = TraceItem(
            organization_id=int(org_id),
            project_id=int(proj_id),
            trace_id=str(trace_id),
            item_id=bytes.fromhex(item_id),
            item_type=item_type,
            timestamp=timestamp,
            attributes=attributes_map,
            client_sample_rate=client_sample_rate,
            server_sample_rate=server_sample_rate,
        )
        items.append(item)

        last_seen_project_id = int(proj_id)
        last_seen_item_type = item_type
        last_seen_timestamp = int(ts)
        last_seen_trace_id = trace_id
        last_seen_item_id = item_id

    return ProcessedResults(
        items=items,
        keyset_cursor=KeysetCursor(
            last_seen_project_id=last_seen_project_id,
            last_seen_item_type=last_seen_item_type,
            last_seen_timestamp=last_seen_timestamp,
            last_seen_trace_id=last_seen_trace_id,
            last_seen_item_id=last_seen_item_id,
        ),
    )


class EndpointExportTraceItems(RPCEndpoint[ExportTraceItemsRequest, ExportTraceItemsResponse]):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[ExportTraceItemsRequest]:
        return ExportTraceItemsRequest

    @classmethod
    def response_class(cls) -> Type[ExportTraceItemsResponse]:
        return ExportTraceItemsResponse

    def _execute(self, in_msg: ExportTraceItemsRequest) -> ExportTraceItemsResponse:
        default_page_size = (
            state.get_int_config("export_trace_items_default_page_size", _DEFAULT_PAGE_SIZE)
            or _DEFAULT_PAGE_SIZE
        )
        if in_msg.limit > 0:
            limit = min(in_msg.limit, default_page_size)
        else:
            limit = default_page_size
        page_token = ExportTraceItemsPageToken.from_protobuf(in_msg.page_token)
        results = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=_build_snuba_request(in_msg, self.routing_decision, limit, page_token),
            timer=self._timer,
        )

        rows = results.result.get("data", [])
        processed_results = _convert_rows(rows)
        is_flex = _is_flextime_export(in_msg)
        orig_start = in_msg.meta.start_timestamp.seconds
        routed = self.routing_decision.time_window

        if routed is not None:
            w_start, w_end = routed.start_timestamp.seconds, routed.end_timestamp.seconds
        else:
            w_start, w_end = in_msg.meta.start_timestamp.seconds, in_msg.meta.end_timestamp.seconds
        flex_window = FlexWindow(start_sec=w_start, end_sec=w_end)

        keyset_cursor = processed_results.keyset_cursor
        next_token = _next_page_token(
            items_returned=len(processed_results.items),
            limit=limit,
            flex_window=flex_window,
            keyset_cursor=keyset_cursor,
            is_flex=is_flex,
            orig_start=orig_start,
            routed_window=routed,
        )

        return ExportTraceItemsResponse(
            trace_items=processed_results.items,
            page_token=next_token,
        )
