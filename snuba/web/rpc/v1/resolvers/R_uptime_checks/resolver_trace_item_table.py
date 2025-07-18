import uuid
from dataclasses import replace
from typing import Sequence

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    AggregationComparisonFilter,
    AggregationFilter,
    TraceItemColumnValues,
    TraceItemTableRequest,
    TraceItemTableResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import (
    PageToken,
    RequestMeta,
    TraceItemType,
)

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, or_cond
from snuba.query.expressions import Expression
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc.common.common import (
    add_existence_check_to_subscriptable_references,
    trace_item_filters_to_expression,
    use_sampling_factor,
)
from snuba.web.rpc.common.debug_info import (
    extract_response_meta,
    setup_trace_query_settings,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingDecision,
)
from snuba.web.rpc.v1.resolvers import ResolverTraceItemTable
from snuba.web.rpc.v1.resolvers.common.aggregation import aggregation_to_expression
from snuba.web.rpc.v1.resolvers.common.trace_item_table import convert_results
from snuba.web.rpc.v1.resolvers.R_uptime_checks.common.common import (
    apply_virtual_columns,
    attribute_key_to_expression,
    base_conditions_and,
    treeify_or_and_conditions,
)

_DEFAULT_ROW_LIMIT = 10_000


def aggregation_filter_to_expression(
    agg_filter: AggregationFilter, request_meta: RequestMeta
) -> Expression:
    op_to_expr = {
        AggregationComparisonFilter.OP_LESS_THAN: f.less,
        AggregationComparisonFilter.OP_GREATER_THAN: f.greater,
        AggregationComparisonFilter.OP_LESS_THAN_OR_EQUALS: f.lessOrEquals,
        AggregationComparisonFilter.OP_GREATER_THAN_OR_EQUALS: f.greaterOrEquals,
        AggregationComparisonFilter.OP_EQUALS: f.equals,
        AggregationComparisonFilter.OP_NOT_EQUALS: f.notEquals,
    }

    match agg_filter.WhichOneof("value"):
        case "comparison_filter":
            op_expr = op_to_expr.get(agg_filter.comparison_filter.op)
            if op_expr is None:
                raise BadSnubaRPCRequestException(
                    f"Unsupported aggregation filter op: {AggregationComparisonFilter.Op.Name(agg_filter.comparison_filter.op)}"
                )
            return op_expr(
                aggregation_to_expression(
                    agg_filter.comparison_filter.aggregation,
                    attribute_key_to_expression,
                    use_sampling_factor(request_meta),
                ),
                agg_filter.comparison_filter.val,
            )
        case "and_filter":
            if len(agg_filter.and_filter.filters) < 2:
                raise BadSnubaRPCRequestException(
                    f"AND filter must have at least two filters, only got {len(agg_filter.and_filter.filters)}"
                )
            return and_cond(
                *(
                    aggregation_filter_to_expression(x, request_meta)
                    for x in agg_filter.and_filter.filters
                )
            )
        case "or_filter":
            if len(agg_filter.or_filter.filters) < 2:
                raise BadSnubaRPCRequestException(
                    f"OR filter must have at least two filters, only got {len(agg_filter.or_filter.filters)}"
                )
            return or_cond(
                *(
                    aggregation_filter_to_expression(x, request_meta)
                    for x in agg_filter.or_filter.filters
                )
            )
        case default:
            raise BadSnubaRPCRequestException(
                f"Unsupported aggregation filter type: {default}"
            )


def _convert_order_by(
    order_by: Sequence[TraceItemTableRequest.OrderBy], request_meta: RequestMeta
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
        elif x.column.HasField("conditional_aggregation"):
            res.append(
                OrderBy(
                    direction=direction,
                    expression=aggregation_to_expression(
                        x.column.conditional_aggregation,
                        attribute_key_to_expression,
                        use_sampling_factor(request_meta),
                    ),
                )
            )
    return res


def _build_query(request: TraceItemTableRequest) -> Query:
    entity = Entity(
        key=EntityKey("uptime_checks"),
        schema=get_entity(EntityKey("uptime_checks")).get_data_model(),
        sample=None,
    )

    selected_columns = []
    for column in request.columns:
        if column.HasField("key"):
            key_col = attribute_key_to_expression(column.key)
            # The key_col expression alias may differ from the column label. That is okay
            # the attribute key name is used in the groupby, the column label is just the name of
            # the returned attribute value
            selected_columns.append(
                SelectedExpression(name=column.label, expression=key_col)
            )
        elif column.HasField("conditional_aggregation"):
            function_expr = aggregation_to_expression(
                column.conditional_aggregation,
                attribute_key_to_expression,
                use_sampling_factor(request.meta),
            )
            # aggregation label may not be set and the column label takes priority anyways.
            function_expr = replace(function_expr, alias=column.label)
            selected_columns.append(
                SelectedExpression(name=column.label, expression=function_expr)
            )
        elif column.HasField("formula"):
            raise BadSnubaRPCRequestException(
                "formulas are not supported for uptime checks"
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
            trace_item_filters_to_expression(
                request.filter, attribute_key_to_expression
            ),
        ),
        order_by=_convert_order_by(request.order_by, request.meta),
        groupby=[
            attribute_key_to_expression(attr_key) for attr_key in request.group_by
        ],
        # protobuf sets limit to 0 by default if it is not set,
        # give it a default value that will actually return data
        limit=request.limit if request.limit > 0 else _DEFAULT_ROW_LIMIT,
        having=(
            aggregation_filter_to_expression(request.aggregation_filter, request.meta)
            if request.HasField("aggregation_filter")
            else None
        ),
        offset=request.page_token.offset,
    )
    treeify_or_and_conditions(res)
    apply_virtual_columns(res, request.virtual_column_contexts)
    add_existence_check_to_subscriptable_references(res)
    return res


def _build_snuba_request(request: TraceItemTableRequest) -> SnubaRequest:
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
            parent_api="uptime_check_samples",
        ),
    )


def _get_page_token(
    request: TraceItemTableRequest, response: list[TraceItemColumnValues]
) -> PageToken:
    if not response:
        return PageToken(offset=0)
    num_rows = len(response[0].results)
    return PageToken(offset=request.page_token.offset + num_rows)


class ResolverTraceItemTableUptimeChecks(ResolverTraceItemTable):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_UPTIME_CHECK

    def resolve(
        self, in_msg: TraceItemTableRequest, routing_decision: RoutingDecision
    ) -> TraceItemTableResponse:
        snuba_request = _build_snuba_request(in_msg)
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=self._timer,
        )
        column_values = convert_results(in_msg, res.result.get("data", []))
        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            [res],
            [self._timer],
        )
        return TraceItemTableResponse(
            column_values=column_values,
            page_token=_get_page_token(in_msg, column_values),
            meta=response_meta,
        )
