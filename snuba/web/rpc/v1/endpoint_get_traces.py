import uuid
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable, Optional, Type

from google.protobuf.internal.containers import RepeatedCompositeFieldContainer
from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_get_traces_pb2 import (
    GetTracesRequest,
    GetTracesResponse,
    TraceAttribute,
)
from sentry_protos.snuba.v1.request_common_pb2 import (
    PageToken,
    RequestMeta,
    TraceItemType,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import AndFilter, TraceItemFilter

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import (
    and_cond,
    column,
    if_cond,
    in_cond,
    literal,
    literals_array,
    or_cond,
)
from snuba.query.expressions import Expression
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.request import Request as SnubaRequest
from snuba.state import get_config
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.common import (
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import (
    extract_response_meta,
    setup_trace_query_settings,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers.common.cross_item_queries import (
    get_trace_ids_for_cross_item_query,
)
from snuba.web.rpc.v1.resolvers.R_eap_items.common.common import (
    attribute_key_to_expression,
)

_DEFAULT_ROW_LIMIT = 10_000
_BUFFER_WINDOW = 2 * 3600  # 2 hours


_ATTRIBUTES: dict[
    TraceAttribute.Key.ValueType,
    tuple[str, AttributeKey.Type.ValueType],
] = {
    TraceAttribute.Key.KEY_TRACE_ID: (
        "trace_id",
        AttributeKey.Type.TYPE_STRING,
    ),
    TraceAttribute.Key.KEY_START_TIMESTAMP: (
        "trace_start_timestamp",
        AttributeKey.Type.TYPE_DOUBLE,
    ),
    TraceAttribute.Key.KEY_END_TIMESTAMP: (
        "trace_end_timestamp",
        AttributeKey.Type.TYPE_DOUBLE,
    ),
    TraceAttribute.Key.KEY_TOTAL_ITEM_COUNT: (
        "total_item_count",
        AttributeKey.Type.TYPE_INT,
    ),
    TraceAttribute.Key.KEY_FILTERED_ITEM_COUNT: (
        "filtered_item_count",
        AttributeKey.Type.TYPE_INT,
    ),
    TraceAttribute.Key.KEY_ROOT_SPAN_NAME: (
        "root_span_name",
        AttributeKey.Type.TYPE_STRING,
    ),
    TraceAttribute.Key.KEY_ROOT_SPAN_DURATION_MS: (
        "root_span_duration_ms",
        AttributeKey.Type.TYPE_INT,
    ),
    TraceAttribute.Key.KEY_ROOT_SPAN_PROJECT_ID: (
        "root_span_project_id",
        AttributeKey.Type.TYPE_INT,
    ),
    TraceAttribute.Key.KEY_EARLIEST_SPAN_NAME: (
        "earliest_span_name",
        AttributeKey.Type.TYPE_STRING,
    ),
    TraceAttribute.Key.KEY_EARLIEST_SPAN_PROJECT_ID: (
        "earliest_span_project_id",
        AttributeKey.Type.TYPE_INT,
    ),
    TraceAttribute.Key.KEY_EARLIEST_SPAN_DURATION_MS: (
        "earliest_span_duration_ms",
        AttributeKey.Type.TYPE_INT,
    ),
    TraceAttribute.Key.KEY_EARLIEST_FRONTEND_SPAN: (
        "earliest_frontend_span",
        AttributeKey.Type.TYPE_STRING,
    ),
    TraceAttribute.Key.KEY_EARLIEST_FRONTEND_SPAN_PROJECT_ID: (
        "earliest_frontend_span_project_id",
        AttributeKey.Type.TYPE_INT,
    ),
    TraceAttribute.Key.KEY_EARLIEST_FRONTEND_SPAN_DURATION_MS: (
        "earliest_frontend_span_duration_ms",
        AttributeKey.Type.TYPE_INT,
    ),
}


_TYPES_TO_CLICKHOUSE: dict[
    AttributeKey.Type.ValueType,
    tuple[str, Callable[[Any], AttributeValue]],
] = {
    AttributeKey.Type.TYPE_STRING: (
        "String",
        lambda x: AttributeValue(val_str=str(x)),
    ),
    AttributeKey.Type.TYPE_INT: (
        "Int64",
        lambda x: AttributeValue(val_int=int(x)),
    ),
    AttributeKey.Type.TYPE_FLOAT: (
        "Float64",
        lambda x: AttributeValue(val_float=float(x)),
    ),
    AttributeKey.Type.TYPE_DOUBLE: (
        "Float64",
        lambda x: AttributeValue(val_double=float(x)),
    ),
}


def _get_attribute_expression(
    attribute_name: str,
    attribute_type: AttributeKey.Type.ValueType,
    request_meta: RequestMeta,
) -> Expression:
    return attribute_key_to_expression(
        AttributeKey(name=attribute_name, type=attribute_type)
    )


def _attribute_to_expression(
    trace_attribute: TraceAttribute,
    condition: Optional[Expression],
    request_meta: RequestMeta,
) -> Expression:
    def _get_root_span_attribute(
        attribute_name: str, attribute_type: AttributeKey.Type.ValueType
    ) -> Expression:
        return f.argMinIf(
            _get_attribute_expression(attribute_name, attribute_type, request_meta),
            if_cond(
                f.equals(
                    _get_attribute_expression(
                        "sentry.start_timestamp",
                        AttributeKey.Type.TYPE_DOUBLE,
                        request_meta,
                    ),
                    literal(0),
                ),
                _get_attribute_expression(
                    "sentry.timestamp",
                    AttributeKey.Type.TYPE_DOUBLE,
                    request_meta,
                ),
                _get_attribute_expression(
                    "sentry.start_timestamp",
                    AttributeKey.Type.TYPE_DOUBLE,
                    request_meta,
                ),
            ),
            and_cond(
                f.equals(column("item_type"), TraceItemType.TRACE_ITEM_TYPE_SPAN),
                f.equals(
                    _get_attribute_expression(
                        "sentry.parent_span_id",
                        AttributeKey.Type.TYPE_STRING,
                        request_meta,
                    ),
                    # root spans don't have a parent span set so the value defaults to empty string
                    literal(""),
                ),
            ),
            alias=alias,
        )

    def _get_earliest_span_attribute(
        attribute_name: str, attribute_type: AttributeKey.Type.ValueType
    ) -> Expression:
        return f.argMinIf(
            _get_attribute_expression(attribute_name, attribute_type, request_meta),
            if_cond(
                f.equals(
                    _get_attribute_expression(
                        "sentry.start_timestamp",
                        AttributeKey.Type.TYPE_DOUBLE,
                        request_meta,
                    ),
                    literal(0),
                ),
                _get_attribute_expression(
                    "sentry.timestamp",
                    AttributeKey.Type.TYPE_DOUBLE,
                    request_meta,
                ),
                _get_attribute_expression(
                    "sentry.start_timestamp",
                    AttributeKey.Type.TYPE_DOUBLE,
                    request_meta,
                ),
            ),
            f.equals(column("item_type"), TraceItemType.TRACE_ITEM_TYPE_SPAN),
            alias=alias,
        )

    def _get_earliest_frontend_span_attribute(
        attribute_name: str, attribute_type: AttributeKey.Type.ValueType
    ) -> Expression:
        span_op = _get_attribute_expression(
            "sentry.op", AttributeKey.Type.TYPE_STRING, request_meta
        )
        return f.argMinIf(
            _get_attribute_expression(attribute_name, attribute_type, request_meta),
            if_cond(
                f.equals(
                    _get_attribute_expression(
                        "sentry.start_timestamp_precise",
                        AttributeKey.Type.TYPE_DOUBLE,
                        request_meta,
                    ),
                    literal(0),
                ),
                _get_attribute_expression(
                    "sentry.timestamp",
                    AttributeKey.Type.TYPE_DOUBLE,
                    request_meta,
                ),
                _get_attribute_expression(
                    "sentry.start_timestamp_precise",
                    AttributeKey.Type.TYPE_DOUBLE,
                    request_meta,
                ),
            ),
            and_cond(
                f.equals(column("item_type"), TraceItemType.TRACE_ITEM_TYPE_SPAN),
                or_cond(
                    f.equals(span_op, literal("pageload")),
                    f.equals(span_op, literal("navigation")),
                ),
            ),
            alias=alias,
        )

    key = trace_attribute.key
    if key in _ATTRIBUTES:
        attribute_name, attribute_type = _ATTRIBUTES[key]
        clickhouse_type = _TYPES_TO_CLICKHOUSE[attribute_type][0]
        alias = attribute_name

        if key == TraceAttribute.Key.KEY_START_TIMESTAMP:
            return f.cast(
                f.min(
                    if_cond(
                        f.equals(
                            _get_attribute_expression(
                                "sentry.start_timestamp_precise",
                                AttributeKey.Type.TYPE_DOUBLE,
                                request_meta,
                            ),
                            literal(0),
                        ),
                        _get_attribute_expression(
                            "sentry.timestamp",
                            AttributeKey.Type.TYPE_DOUBLE,
                            request_meta,
                        ),
                        _get_attribute_expression(
                            "sentry.start_timestamp_precise",
                            AttributeKey.Type.TYPE_DOUBLE,
                            request_meta,
                        ),
                    )
                ),
                clickhouse_type,
                alias=alias,
            )
        elif key == TraceAttribute.Key.KEY_END_TIMESTAMP:
            return f.cast(
                f.max(
                    if_cond(
                        f.equals(
                            _get_attribute_expression(
                                "sentry.end_timestamp_precise",
                                AttributeKey.Type.TYPE_DOUBLE,
                                request_meta,
                            ),
                            literal(0),
                        ),
                        _get_attribute_expression(
                            "sentry.timestamp",
                            AttributeKey.Type.TYPE_DOUBLE,
                            request_meta,
                        ),
                        _get_attribute_expression(
                            "sentry.end_timestamp_precise",
                            AttributeKey.Type.TYPE_DOUBLE,
                            request_meta,
                        ),
                    )
                ),
                clickhouse_type,
                alias=alias,
            )
        elif key == TraceAttribute.Key.KEY_TOTAL_ITEM_COUNT:
            return f.count(alias=alias)
        elif key == TraceAttribute.Key.KEY_FILTERED_ITEM_COUNT:
            if condition:
                return f.countIf(condition, alias=alias)
            else:
                return f.count(alias=alias)
        elif key == TraceAttribute.Key.KEY_ROOT_SPAN_NAME:
            return _get_root_span_attribute(
                "sentry.raw_description", AttributeKey.Type.TYPE_STRING
            )
        elif key == TraceAttribute.Key.KEY_ROOT_SPAN_DURATION_MS:
            return _get_root_span_attribute(
                "sentry.duration_ms", AttributeKey.Type.TYPE_DOUBLE
            )
        elif key == TraceAttribute.Key.KEY_ROOT_SPAN_PROJECT_ID:
            return _get_root_span_attribute(
                "sentry.project_id", AttributeKey.Type.TYPE_INT
            )
        elif key == TraceAttribute.Key.KEY_EARLIEST_SPAN_NAME:
            return _get_earliest_span_attribute(
                "sentry.raw_description", AttributeKey.Type.TYPE_STRING
            )
        elif key == TraceAttribute.Key.KEY_EARLIEST_SPAN_PROJECT_ID:
            return _get_earliest_span_attribute(
                "sentry.project_id", AttributeKey.Type.TYPE_INT
            )
        elif key == TraceAttribute.Key.KEY_EARLIEST_SPAN_DURATION_MS:
            return _get_earliest_span_attribute(
                "sentry.duration_ms", AttributeKey.Type.TYPE_DOUBLE
            )
        elif key == TraceAttribute.Key.KEY_EARLIEST_FRONTEND_SPAN:
            return _get_earliest_frontend_span_attribute(
                "sentry.raw_description", AttributeKey.Type.TYPE_STRING
            )
        elif key == TraceAttribute.Key.KEY_EARLIEST_FRONTEND_SPAN_PROJECT_ID:
            return _get_earliest_frontend_span_attribute(
                "sentry.project_id", AttributeKey.Type.TYPE_INT
            )
        elif key == TraceAttribute.Key.KEY_EARLIEST_FRONTEND_SPAN_DURATION_MS:
            return _get_earliest_frontend_span_attribute(
                "sentry.duration_ms", AttributeKey.Type.TYPE_DOUBLE
            )
        elif key == TraceAttribute.Key.KEY_TRACE_ID:
            return column("trace_id", alias="hex_trace_id")
        else:
            return f.cast(column(attribute_name), clickhouse_type, alias=alias)

    raise BadSnubaRPCRequestException(
        f"{key} had an unknown or unset type: {trace_attribute.type}"
    )


def _build_snuba_request(
    request: GetTracesRequest,
    query: Query,
    clickhouse_settings: dict[str, Any] = {},
    query_settings: QuerySettings | None = None,
) -> SnubaRequest:
    query_settings = query_settings or (
        setup_trace_query_settings() if request.meta.debug else HTTPQuerySettings()
    )

    for key, value in clickhouse_settings.items():
        query_settings.push_clickhouse_setting(key, value)

    return SnubaRequest(
        id=uuid.UUID(request.meta.request_id),
        original_body=MessageToDict(request),
        query=query,
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            referrer=request.meta.referrer,
            team="eap",
            feature="eap",
            tenant_ids={
                "organization_id": request.meta.organization_id,
                "referrer": request.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api="eap_span_samples",
        ),
    )


def _convert_results(
    request: GetTracesRequest,
    data: Iterable[Dict[str, Any]],
) -> list[GetTracesResponse.Trace]:
    res: list[GetTracesResponse.Trace] = []
    column_ordering = {
        trace_attribute.key: i for i, trace_attribute in enumerate(request.attributes)
    }

    for row in data:
        values: defaultdict[
            TraceAttribute.Key.ValueType,
            TraceAttribute,
        ] = defaultdict(TraceAttribute)
        for attribute in request.attributes:
            value = row[_ATTRIBUTES[attribute.key][0]]
            type = _ATTRIBUTES[attribute.key][1]
            values[attribute.key] = TraceAttribute(
                key=attribute.key,
                value=_TYPES_TO_CLICKHOUSE[type][1](value),
                type=type,
            )
        res.append(
            GetTracesResponse.Trace(
                # we return the columns in the order they were requested
                attributes=sorted(
                    values.values(),
                    key=lambda c: column_ordering[c.key],
                )
            )
        )

    return res


def _get_page_token(
    request: GetTracesRequest,
    rows: list[GetTracesResponse.Trace],
) -> PageToken:
    if not rows:
        return PageToken(offset=0)
    num_rows = len(rows)
    return PageToken(offset=request.page_token.offset + num_rows)


def _validate_order_by(in_msg: GetTracesRequest) -> None:
    order_by_cols = set([ob.key for ob in in_msg.order_by])
    selected_columns = set([c.key for c in in_msg.attributes])
    if not order_by_cols.issubset(selected_columns):
        raise BadSnubaRPCRequestException(
            f"Ordered by columns {order_by_cols} not selected: {selected_columns}"
        )


class EndpointGetTraces(RPCEndpoint[GetTracesRequest, GetTracesResponse]):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[GetTracesRequest]:
        return GetTracesRequest

    @classmethod
    def response_class(cls) -> Type[GetTracesResponse]:
        return GetTracesResponse

    def _execute(self, in_msg: GetTracesRequest) -> GetTracesResponse:
        _validate_order_by(in_msg)

        in_msg.meta.request_id = getattr(in_msg.meta, "request_id", None) or str(
            uuid.uuid4()
        )
        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            [],
            [self._timer],
        )

        # Get a dict of trace IDs and timestamps.
        if self._is_cross_event_query(in_msg.filters):
            trace_ids = get_trace_ids_for_cross_item_query(
                in_msg, in_msg.meta, in_msg.filters, self._timer
            )
        else:
            trace_ids = self._get_trace_ids_for_single_item_query(request=in_msg)

        if len(trace_ids) == 0:
            return GetTracesResponse(meta=response_meta)

        # Get metadata for those traces.
        traces = self._get_metadata_for_traces(request=in_msg, trace_ids=trace_ids)
        return GetTracesResponse(
            traces=traces,
            page_token=_get_page_token(in_msg, traces),
            meta=response_meta,
        )

    def _is_cross_event_query(
        self, filters: RepeatedCompositeFieldContainer[GetTracesRequest.TraceFilter]
    ) -> bool:
        return len(set([f.item_type for f in filters])) > 1

    def _get_trace_item_filter_expressions(
        self, filters: RepeatedCompositeFieldContainer[GetTracesRequest.TraceFilter]
    ) -> dict[TraceItemType.ValueType, Expression]:
        """
        Returns a dict mapping item types to a filter expression for that item type.
        """
        filters_by_item_type: dict[
            TraceItemType.ValueType, list[TraceItemFilter]
        ] = defaultdict(list)
        filter_expressions_by_item_type: dict[TraceItemType.ValueType, Expression] = {}
        for trace_filter in filters:
            filters_by_item_type[trace_filter.item_type].append(trace_filter.filter)

        for item_type in filters_by_item_type:
            filter_expressions_by_item_type[item_type] = and_cond(
                f.equals(column("item_type"), item_type),
                trace_item_filters_to_expression(
                    TraceItemFilter(
                        and_filter=AndFilter(
                            filters=filters_by_item_type[item_type],
                        ),
                    ),
                    attribute_key_to_expression,
                ),
            )

        return filter_expressions_by_item_type

    def _get_trace_ids_for_single_item_query(
        self,
        request: GetTracesRequest,
    ) -> list[str]:
        if request.filters:
            item_type = request.filters[0].item_type
        elif request.meta.trace_item_type != TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED:
            item_type = request.meta.trace_item_type
        else:
            item_type = TraceItemType.TRACE_ITEM_TYPE_SPAN

        trace_item_filters_expression = trace_item_filters_to_expression(
            TraceItemFilter(
                and_filter=AndFilter(
                    filters=[f.filter for f in request.filters],
                ),
            ),
            attribute_key_to_expression,
        )
        selected_columns: list[SelectedExpression] = [
            SelectedExpression(
                name="trace_id",
                expression=f.distinct(
                    column("trace_id"),
                ),
            )
        ]
        entity = Entity(
            key=EntityKey("eap_items"),
            schema=get_entity(EntityKey("eap_items")).get_data_model(),
            sample=None,
        )
        query = Query(
            from_clause=entity,
            selected_columns=selected_columns,
            condition=base_conditions_and(
                request.meta,
                trace_item_filters_expression,
                f.equals(column("item_type"), item_type),
            ),
            order_by=[
                OrderBy(
                    direction=OrderByDirection.DESC,
                    expression=column("organization_id"),
                ),
                OrderBy(
                    direction=OrderByDirection.DESC,
                    expression=column("project_id"),
                ),
                OrderBy(
                    direction=OrderByDirection.DESC,
                    expression=column("item_type"),
                ),
                OrderBy(
                    direction=OrderByDirection.DESC,
                    expression=column("timestamp"),
                ),
            ],
            limit=request.limit if request.limit > 0 else _DEFAULT_ROW_LIMIT,
            offset=request.page_token.offset,
        )

        treeify_or_and_conditions(query)
        settings = (
            setup_trace_query_settings() if request.meta.debug else HTTPQuerySettings()
        )
        if get_config("enable_trace_sampling", False):
            settings.set_sampling_tier(self.routing_decision.tier)
        results = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=_build_snuba_request(request, query, query_settings=settings),
            timer=self._timer,
        )
        trace_ids: list[str] = []
        for row in results.result.get("data", []):
            trace_ids.append(list(row.values())[0])
        return trace_ids

    def _get_metadata_for_traces(
        self,
        request: GetTracesRequest,
        trace_ids: list[str],
    ) -> list[GetTracesResponse.Trace]:
        # We use the item type specified in the request meta for the trace item filter conditions.
        # If no item type is specified, we use all the filters.
        filter_expressions_by_item_type = self._get_trace_item_filter_expressions(
            request.filters
        )
        trace_item_filters_expression = None
        item_type = None
        if request.meta.trace_item_type in filter_expressions_by_item_type:
            trace_item_filters_expression = filter_expressions_by_item_type[
                request.meta.trace_item_type
            ]
            item_type = request.meta.trace_item_type
        elif len(filter_expressions_by_item_type) == 1:
            trace_item_filters_expression = next(
                iter(filter_expressions_by_item_type.values())
            )
            item_type = next(iter(filter_expressions_by_item_type.keys()))
        elif len(filter_expressions_by_item_type) > 1:
            trace_item_filters_expression = or_cond(
                *[expression for expression in filter_expressions_by_item_type.values()]
            )
        else:
            item_type = TraceItemType.TRACE_ITEM_TYPE_SPAN

        selected_columns: list[SelectedExpression] = []
        start_timestamp_requested = False
        for trace_attribute in request.attributes:
            if trace_attribute.key == TraceAttribute.Key.KEY_START_TIMESTAMP:
                start_timestamp_requested = True
            selected_columns.append(
                SelectedExpression(
                    name=_ATTRIBUTES[trace_attribute.key][0],
                    expression=_attribute_to_expression(
                        trace_attribute,
                        trace_item_filters_expression,
                        request_meta=request.meta,
                    ),
                )
            )

        # Since we're always ordering by start_timestamp, we need to request
        # the field unless it's already been requested.
        if not start_timestamp_requested:
            trace_attribute = TraceAttribute(key=TraceAttribute.Key.KEY_START_TIMESTAMP)
            selected_columns.append(
                SelectedExpression(
                    name=_ATTRIBUTES[trace_attribute.key][0],
                    expression=_attribute_to_expression(
                        trace_attribute,
                        trace_item_filters_expression,
                        request_meta=request.meta,
                    ),
                )
            )

        entity = Entity(
            key=EntityKey("eap_items"),
            schema=get_entity(EntityKey("eap_items")).get_data_model(),
            sample=None,
        )

        if item_type:
            condition = base_conditions_and(
                request.meta,
                in_cond(
                    column("trace_id"),
                    literals_array(None, [literal(trace_id) for trace_id in trace_ids]),
                ),
                f.equals(column("item_type"), item_type),
            )
        else:
            condition = base_conditions_and(
                request.meta,
                in_cond(
                    column("trace_id"),
                    literals_array(None, [literal(trace_id) for trace_id in trace_ids]),
                ),
            )

        query = Query(
            from_clause=entity,
            selected_columns=selected_columns,
            condition=condition,
            groupby=[
                _attribute_to_expression(
                    TraceAttribute(
                        key=TraceAttribute.Key.KEY_TRACE_ID,
                    ),
                    None,
                    request_meta=request.meta,
                ),
            ],
            order_by=[
                OrderBy(
                    direction=OrderByDirection.DESC,
                    expression=column("trace_start_timestamp"),
                ),
            ],
        )

        treeify_or_and_conditions(query)

        results = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=_build_snuba_request(request, query),
            timer=self._timer,
        )

        return _convert_results(request, results.result.get("data", []))
