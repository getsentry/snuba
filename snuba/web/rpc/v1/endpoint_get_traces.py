import uuid
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable, Sequence, Type

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_get_traces_pb2 import (
    GetTracesRequest,
    GetTracesResponse,
    TraceColumn,
)
from sentry_protos.snuba.v1.request_common_pb2 import PageToken
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column
from snuba.query.expressions import Expression
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
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

_DEFAULT_ROW_LIMIT = 10_000

_COLUMN_TO_NAME: dict[TraceColumn.Name, str] = {
    TraceColumn.Name.NAME_FILTERED_SPAN_COUNT: "filtered_span_count",
    TraceColumn.Name.NAME_ROOT_SPAN_NAME: "root_span_name",
    TraceColumn.Name.NAME_START_TIMESTAMP: "start_timestamp",
    TraceColumn.Name.NAME_TOTAL_SPAN_COUNT: "total_span_count",
    TraceColumn.Name.NAME_TRACE_ID: "trace_id",
}

_NAME_TO_COLUMN: dict[str, TraceColumn.Name] = {
    v: k for k, v in _COLUMN_TO_NAME.items()
}

_TYPES_TO_CLICKHOUSE: dict[AttributeKey.Type, str] = {
    AttributeKey.Type.TYPE_STRING: "String",
    AttributeKey.Type.TYPE_INT: "Int64",
    AttributeKey.Type.TYPE_FLOAT: "Float64",
}

_POSSIBLE_TYPES: dict[TraceColumn.Name, set[AttributeKey.Type]] = {
    TraceColumn.Name.NAME_TRACE_ID: {
        AttributeKey.Type.TYPE_STRING,
    },
    TraceColumn.Name.NAME_START_TIMESTAMP: {
        AttributeKey.Type.TYPE_STRING,
        AttributeKey.Type.TYPE_INT,
        AttributeKey.Type.TYPE_FLOAT,
    },
}


def _column_to_expression(trace_column: TraceColumn, conditions=None) -> Expression:
    if trace_column.name == TraceColumn.Name.NAME_TOTAL_SPAN_COUNT:
        return f.count(
            alias=_COLUMN_TO_NAME[trace_column.name],
        )
    if trace_column.name == TraceColumn.Name.NAME_FILTERED_SPAN_COUNT:
        return f.countIf(
            conditions,
            alias=_COLUMN_TO_NAME[trace_column.name],
        )
    if trace_column.name == TraceColumn.Name.NAME_START_TIMESTAMP:
        return f.CAST(
            f.min(column("start_timestamp")),
            _TYPES_TO_CLICKHOUSE[trace_column.type],
            alias=_COLUMN_TO_NAME[trace_column.name],
        )
    if trace_column.name == TraceColumn.Name.NAME_ROOT_SPAN_NAME:
        return f.anyIf(
            column("name"),
            f.equals(column("is_segment"), True),
            alias=_COLUMN_TO_NAME[trace_column.name],
        )
    if (
        trace_column.name in _COLUMN_TO_NAME
        and trace_column.type in _POSSIBLE_TYPES.get(trace_column.name, {})
    ):
        return f.CAST(
            column(
                _COLUMN_TO_NAME[trace_column.name],
            ),
            _TYPES_TO_CLICKHOUSE[trace_column.type],
            alias=_COLUMN_TO_NAME[trace_column.name],
        )
    raise BadSnubaRPCRequestException(
        f"{trace_column.name} had an unknown or unset type: {trace_column.type}"
    )


def _convert_order_by(
    order_by: Sequence[GetTracesRequest.OrderBy],
) -> Sequence[OrderBy]:
    res: list[OrderBy] = []
    for x in order_by:
        direction = OrderByDirection.DESC if x.descending else OrderByDirection.ASC
        res.append(
            OrderBy(
                direction=direction,
                expression=_column_to_expression(x.column),
            )
        )
    return res


def _build_query(request: GetTracesRequest) -> Query:
    trace_item_filter_expressions = trace_item_filters_to_expression(request.filter)
    selected_columns = []

    for trace_column in request.columns:
        expression = _column_to_expression(trace_column, trace_item_filter_expressions)
        selected_columns.append(
            SelectedExpression(
                name=_COLUMN_TO_NAME[trace_column.name],
                expression=expression,
            )
        )

    entity = Entity(
        key=EntityKey("eap_spans"),
        schema=get_entity(EntityKey("eap_spans")).get_data_model(),
        sample=None,
    )
    res = Query(
        from_clause=entity,
        selected_columns=selected_columns,
        condition=base_conditions_and(
            request.meta,
        ),
        order_by=_convert_order_by(request.order_by),
        groupby=[
            _column_to_expression(
                TraceColumn(
                    type=AttributeKey.TYPE_STRING,
                    name=TraceColumn.Name.NAME_TRACE_ID,
                ),
            ),
        ],
        limit=request.limit if request.limit > 0 else _DEFAULT_ROW_LIMIT,
    )

    treeify_or_and_conditions(res)

    return res


def _build_snuba_request(request: GetTracesRequest) -> SnubaRequest:
    query_settings = (
        setup_trace_query_settings() if request.meta.debug else HTTPQuerySettings()
    )

    return SnubaRequest(
        id=uuid.UUID(request.meta.request_id),
        original_body=MessageToDict(request),
        query=_build_query(request),
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
    request: GetTracesRequest, data: Iterable[Dict[str, Any]]
) -> list[GetTracesResponse.Trace]:
    converters: Dict[str, Callable[[Any], AttributeValue]] = {}

    for trace_column in request.columns:
        if trace_column.type == AttributeKey.TYPE_BOOLEAN:
            converters[trace_column.name] = lambda x: AttributeValue(val_bool=bool(x))
        elif trace_column.type == AttributeKey.TYPE_STRING:
            converters[trace_column.name] = lambda x: AttributeValue(val_str=str(x))
        elif trace_column.type == AttributeKey.TYPE_INT:
            converters[trace_column.name] = lambda x: AttributeValue(val_int=int(x))
        elif trace_column.type == AttributeKey.TYPE_FLOAT:
            converters[trace_column.name] = lambda x: AttributeValue(val_float=float(x))

    res: list[GetTracesResponse.Trace] = []
    column_ordering = {
        trace_column.name: i for i, trace_column in enumerate(request.columns)
    }

    for row in data:
        values: defaultdict[
            TraceColumn.Name,
            GetTracesResponse.Trace.Column,
        ] = defaultdict(GetTracesResponse.Trace.Column)
        for column_name, value in row.items():
            name = _NAME_TO_COLUMN[column_name]
            if name in converters.keys():
                values[name] = GetTracesResponse.Trace.Column(
                    name=name,
                    value=converters[name](value),
                )
        res.append(
            GetTracesResponse.Trace(
                # we return the columns in the order they were requested
                columns=sorted(
                    values.values(),
                    key=lambda c: column_ordering[c.name],
                )
            )
        )

    return res


def _get_page_token(
    request: GetTracesRequest, response: list[GetTracesResponse.Trace]
) -> PageToken:
    if not response:
        return PageToken(offset=0)
    num_rows = len(response)
    return PageToken(offset=request.page_token.offset + num_rows)


def _validate_order_by(in_msg: GetTracesRequest) -> None:
    order_by_cols = set([ob.column.name for ob in in_msg.order_by])
    selected_columns = set([c.name for c in in_msg.columns])
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
        snuba_request = _build_snuba_request(in_msg)
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=self._timer,
        )
        traces = _convert_results(in_msg, res.result.get("data", []))
        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            [res],
            [self._timer],
        )
        return GetTracesResponse(
            traces=traces,
            page_token=_get_page_token(in_msg, traces),
            meta=response_meta,
        )
