import uuid
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable, Type

from google.protobuf.internal.containers import RepeatedCompositeFieldContainer
from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_get_traces_pb2 import (
    GetTracesRequest,
    GetTracesResponse,
    TraceAttribute,
)
from sentry_protos.snuba.v1.request_common_pb2 import PageToken, TraceItemName
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import TraceItemFilter

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import LimitBy, OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, column, in_cond, literal, literals_array
from snuba.query.expressions import Expression
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.common import (
    base_conditions_and,
    project_id_and_org_conditions,
    timestamp_in_range_condition,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import (
    extract_response_meta,
    setup_trace_query_settings,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException

_DEFAULT_ROW_LIMIT = 10_000
_BUFFER_WINDOW = 2 * 3600  # 2 hours

_ATTRIBUTES: dict[
    TraceAttribute.Key.ValueType,
    tuple[str, AttributeKey.Type.ValueType],
] = {
    TraceAttribute.Key.KEY_FILTERED_ITEM_COUNT: (
        "filtered_item_count",
        AttributeKey.Type.TYPE_INT,
    ),
    TraceAttribute.Key.KEY_ROOT_SPAN_NAME: (
        "root_span_name",
        AttributeKey.Type.TYPE_STRING,
    ),
    TraceAttribute.Key.KEY_START_TIMESTAMP: (
        "trace_start_timestamp",
        AttributeKey.Type.TYPE_FLOAT,
    ),
    TraceAttribute.Key.KEY_TOTAL_ITEM_COUNT: (
        "total_item_count",
        AttributeKey.Type.TYPE_INT,
    ),
    TraceAttribute.Key.KEY_TRACE_ID: (
        "trace_id",
        AttributeKey.Type.TYPE_STRING,
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
}


def _attribute_to_expression(
    trace_attribute: TraceAttribute,
    *conditions: Expression,
) -> Expression:
    if trace_attribute.key == TraceAttribute.Key.KEY_TOTAL_ITEM_COUNT:
        return f.count(
            alias=_ATTRIBUTES[trace_attribute.key][0],
        )
    if trace_attribute.key == TraceAttribute.Key.KEY_FILTERED_ITEM_COUNT:
        return f.countIf(
            *conditions,
            alias=_ATTRIBUTES[trace_attribute.key][0],
        )
    if trace_attribute.key == TraceAttribute.Key.KEY_START_TIMESTAMP:
        attribute = _ATTRIBUTES[trace_attribute.key]
        return f.cast(
            f.min(column("start_timestamp")),
            _TYPES_TO_CLICKHOUSE[attribute[1]][0],
            alias=_ATTRIBUTES[trace_attribute.key][0],
        )
    if trace_attribute.key == TraceAttribute.Key.KEY_ROOT_SPAN_NAME:
        # TODO: Change to return the root span name instead of the trace's first span's name.
        return f.argMin(
            column("name"),
            column("start_timestamp"),
            alias=_ATTRIBUTES[trace_attribute.key][0],
        )
    if trace_attribute.key in _ATTRIBUTES:
        attribute = _ATTRIBUTES[trace_attribute.key]
        return f.cast(
            column(attribute[0]),
            _TYPES_TO_CLICKHOUSE[attribute[1]][0],
            alias=attribute[0],
        )
    raise BadSnubaRPCRequestException(
        f"{trace_attribute.key} had an unknown or unset type: {trace_attribute.type}"
    )


def _build_snuba_request(request: GetTracesRequest, query: Query) -> SnubaRequest:
    query_settings = (
        setup_trace_query_settings() if request.meta.debug else HTTPQuerySettings()
    )

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
    request: GetTracesRequest, data: Iterable[Dict[str, Any]]
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


# TODO: support more than one filter.
def _select_supported_filters(
    filters: RepeatedCompositeFieldContainer[GetTracesRequest.TraceFilter],
) -> TraceItemFilter:
    filter_count = len(filters)
    if filter_count == 0:
        return TraceItemFilter()
    if filter_count > 1:
        raise BadSnubaRPCRequestException("Multiple filters are not supported.")
    try:
        # Find first span filter.
        return next(
            f.filter
            for f in filters
            if f.item_name == TraceItemName.TRACE_ITEM_NAME_EAP_SPANS
        )
    except StopIteration:
        raise BadSnubaRPCRequestException("Only one span filter is supported.")


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
        trace_ids = self._list_trace_ids(request=in_msg)
        if len(trace_ids) == 0:
            return GetTracesResponse(meta=response_meta)

        # Get metadata for those traces.
        traces = self._get_metadata_for_traces(request=in_msg, trace_ids=trace_ids)
        return GetTracesResponse(
            traces=traces,
            page_token=_get_page_token(in_msg, traces),
            meta=response_meta,
        )

    def _list_trace_ids(
        self,
        request: GetTracesRequest,
    ) -> dict[str, int]:
        trace_item_filters_expression = trace_item_filters_to_expression(
            _select_supported_filters(request.filters),
        )
        selected_columns: list[SelectedExpression] = [
            SelectedExpression(
                name="trace_id",
                expression=f.cast(
                    column("trace_id"),
                    "String",
                    alias="trace_id",
                ),
            ),
            SelectedExpression(
                name="_sort_timestamp",
                expression=f.cast(
                    column("_sort_timestamp"),
                    "UInt32",
                    alias="_sort_timestamp",
                ),
            ),
        ]
        entity = Entity(
            key=EntityKey("eap_spans"),
            schema=get_entity(EntityKey("eap_spans")).get_data_model(),
            sample=None,
        )
        query = Query(
            from_clause=entity,
            selected_columns=selected_columns,
            condition=base_conditions_and(
                request.meta,
                trace_item_filters_expression,
            ),
            order_by=[
                OrderBy(
                    direction=OrderByDirection.DESC,
                    expression=column("_sort_timestamp"),
                ),
            ],
            limitby=LimitBy(limit=1, columns=[column("trace_id")]),
            limit=request.limit if request.limit > 0 else _DEFAULT_ROW_LIMIT,
        )

        treeify_or_and_conditions(query)

        results = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=_build_snuba_request(request, query),
            timer=self._timer,
        )
        trace_ids: dict[str, int] = {}
        for row in results.result.get("data", []):
            trace_ids[row["trace_id"]] = row["_sort_timestamp"]
        return trace_ids

    def _get_metadata_for_traces(
        self,
        request: GetTracesRequest,
        trace_ids: dict[str, int],
    ) -> list[GetTracesResponse.Trace]:
        trace_item_filters_expression = trace_item_filters_to_expression(
            _select_supported_filters(request.filters),
        )

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
                    ),
                )
            )

        entity = Entity(
            key=EntityKey("eap_spans"),
            schema=get_entity(EntityKey("eap_spans")).get_data_model(),
            sample=None,
        )
        timestamps = trace_ids.values()
        query = Query(
            from_clause=entity,
            selected_columns=selected_columns,
            condition=and_cond(
                project_id_and_org_conditions(request.meta),
                timestamp_in_range_condition(
                    min(timestamps) - _BUFFER_WINDOW,
                    max(timestamps) + _BUFFER_WINDOW,
                ),
                in_cond(
                    f.cast(
                        column("trace_id"),
                        "String",
                        alias="trace_id",
                    ),
                    literals_array(
                        None, [literal(trace_id) for trace_id in trace_ids.keys()]
                    ),
                ),
            ),
            groupby=[
                _attribute_to_expression(
                    TraceAttribute(
                        key=TraceAttribute.Key.KEY_TRACE_ID,
                    ),
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
