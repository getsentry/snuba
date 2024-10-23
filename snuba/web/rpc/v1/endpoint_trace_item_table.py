import uuid
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable, Sequence, Type

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemColumnValues,
    TraceItemTableRequest,
    TraceItemTableResponse,
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
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.common import (
    aggregation_to_expression,
    apply_virtual_columns,
    attribute_key_to_expression,
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import extract_response_meta
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException

_DEFAULT_ROW_LIMIT = 10_000


def _convert_order_by(
    order_by: Sequence[TraceItemTableRequest.OrderBy],
) -> Sequence[OrderBy]:
    res: list[OrderBy] = []
    for x in order_by:
        direction = OrderByDirection.DESC if x.descending else OrderByDirection.ASC
        if x.column.HasField("key"):
            res.append(
                OrderBy(
                    direction=direction,
                    expression=attribute_key_to_expression(x.column.key),
                )
            )
        elif x.column.HasField("aggregation"):
            res.append(
                OrderBy(
                    direction=direction,
                    expression=aggregation_to_expression(x.column.aggregation),
                )
            )
    return res


def _build_query(request: TraceItemTableRequest) -> Query:
    # TODO: This is hardcoded still
    entity = Entity(
        key=EntityKey("eap_spans"),
        schema=get_entity(EntityKey("eap_spans")).get_data_model(),
        sample=None,
    )

    selected_columns = []
    for column in request.columns:
        if column.HasField("key"):
            key_col = attribute_key_to_expression(column.key)
            selected_columns.append(
                SelectedExpression(name=column.key.name, expression=key_col)
            )
        elif column.HasField("aggregation"):
            function_expr = aggregation_to_expression(column.aggregation)
            selected_columns.append(
                SelectedExpression(name=column.label, expression=function_expr)
            )
        else:
            raise BadSnubaRPCRequestException(
                "Column is neither an aggregate or an attribute"
            )

    res = Query(
        from_clause=entity,
        selected_columns=selected_columns,
        condition=base_conditions_and(
            request.meta,
            trace_item_filters_to_expression(request.filter),
        ),
        order_by=_convert_order_by(request.order_by),
        groupby=[
            attribute_key_to_expression(attr_key) for attr_key in request.group_by
        ],
        # protobuf sets limit to 0 by default if it is not set,
        # give it a default value that will actually return data
        limit=request.limit if request.limit > 0 else _DEFAULT_ROW_LIMIT,
    )
    treeify_or_and_conditions(res)
    apply_virtual_columns(res, request.virtual_column_contexts)
    return res


def _build_snuba_request(request: TraceItemTableRequest) -> SnubaRequest:
    return SnubaRequest(
        id=request.meta.request_id,
        original_body=MessageToDict(request),
        query=_build_query(request),
        query_settings=HTTPQuerySettings(),
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
    request: TraceItemTableRequest, data: Iterable[Dict[str, Any]]
) -> list[TraceItemColumnValues]:

    converters: Dict[str, Callable[[Any], AttributeValue]] = {}

    for column in request.columns:
        if column.HasField("key"):
            if column.key.type == AttributeKey.TYPE_BOOLEAN:
                converters[column.label] = lambda x: AttributeValue(val_bool=bool(x))
            elif column.key.type == AttributeKey.TYPE_STRING:
                converters[column.label] = lambda x: AttributeValue(val_str=str(x))
            elif column.key.type == AttributeKey.TYPE_INT:
                converters[column.label] = lambda x: AttributeValue(val_int=int(x))
            elif column.key.type == AttributeKey.TYPE_FLOAT:
                converters[column.label] = lambda x: AttributeValue(val_float=float(x))
        elif column.HasField("aggregation"):
            converters[column.label] = lambda x: AttributeValue(val_float=float(x))
        else:
            raise BadSnubaRPCRequestException(
                "column is neither an attribute or aggregation"
            )

    res: defaultdict[str, TraceItemColumnValues] = defaultdict(TraceItemColumnValues)
    for row in data:
        for column_name, value in row.items():
            res[column_name].results.append(converters[column_name](value))
            res[column_name].attribute_name = column_name

    column_ordering = {column.label: i for i, column in enumerate(request.columns)}

    return list(
        # we return the columns in the order they were requested
        sorted(
            res.values(), key=lambda c: column_ordering.__getitem__(c.attribute_name)
        )
    )


def _get_page_token(
    request: TraceItemTableRequest, response: list[TraceItemColumnValues]
) -> PageToken:
    if not response:
        return PageToken(offset=0)
    num_rows = len(response[0].results)
    return PageToken(offset=request.page_token.offset + num_rows)


def _apply_labels_to_columns(in_msg: TraceItemTableRequest) -> TraceItemTableRequest:
    def _apply_label_to_column(column: Column) -> None:
        if column.label:
            return

        if column.HasField("key"):
            column.label = column.key.name

        elif column.HasField("aggregation"):
            column.label = column.aggregation.label

    for column in in_msg.columns:
        _apply_label_to_column(column)

    for order_by in in_msg.order_by:
        _apply_label_to_column(order_by.column)

    return in_msg


def _validate_select_and_groupby(in_msg: TraceItemTableRequest) -> None:
    non_aggregted_columns = set(
        [c.key.name for c in in_msg.columns if c.HasField("key")]
    )
    grouped_by_columns = set([c.name for c in in_msg.group_by])
    aggregation_present = any([c for c in in_msg.columns if c.HasField("aggregation")])
    if non_aggregted_columns != grouped_by_columns and aggregation_present:
        raise BadSnubaRPCRequestException(
            f"Non aggregated columns should be in group_by. non_aggregted_columns: {non_aggregted_columns}, grouped_by_columns: {grouped_by_columns}"
        )


def _validate_order_by(in_msg: TraceItemTableRequest) -> None:
    order_by_cols = set([ob.column.label for ob in in_msg.order_by])
    selected_columns = set([c.label for c in in_msg.columns])
    if not order_by_cols.issubset(selected_columns):
        raise BadSnubaRPCRequestException(
            f"Ordered by columns {order_by_cols} not selected: {selected_columns}"
        )


class EndpointTraceItemTable(
    RPCEndpoint[TraceItemTableRequest, TraceItemTableResponse]
):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[TraceItemTableRequest]:
        return TraceItemTableRequest

    @classmethod
    def response_class(cls) -> Type[TraceItemTableResponse]:
        return TraceItemTableResponse

    def _execute(self, in_msg: TraceItemTableRequest) -> TraceItemTableResponse:
        in_msg = _apply_labels_to_columns(in_msg)
        _validate_select_and_groupby(in_msg)
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
        column_values = _convert_results(in_msg, res.result.get("data", []))
        response_meta = extract_response_meta(
            in_msg.meta.request_id, in_msg.meta.debug, [res], [self._timer]
        )
        return TraceItemTableResponse(
            column_values=column_values,
            page_token=_get_page_token(in_msg, column_values),
            meta=response_meta,
        )
