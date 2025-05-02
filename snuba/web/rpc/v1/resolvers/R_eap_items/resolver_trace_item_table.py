from dataclasses import replace
from typing import List, Sequence

from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    AggregationComparisonFilter,
    AggregationFilter,
    Column,
    TraceItemColumnValues,
    TraceItemTableRequest,
    TraceItemTableResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import PageToken, RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import ExtrapolationMode

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond
from snuba.query.dsl import column as snuba_column
from snuba.query.dsl import literal, or_cond
from snuba.query.expressions import Expression
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.timer import Timer
from snuba.web.rpc.common.common import (
    add_existence_check_to_subscriptable_references,
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
    use_sampling_factor,
)
from snuba.web.rpc.common.debug_info import (
    extract_response_meta,
    setup_trace_query_settings,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers.common.aggregation import (
    aggregation_to_expression,
    get_average_sample_rate_column,
    get_confidence_interval_column,
    get_count_column,
)
from snuba.web.rpc.v1.resolvers.common.trace_item_table import convert_results
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.sampling_in_storage_util import (
    run_query_to_correct_tier,
)
from snuba.web.rpc.v1.resolvers.R_eap_spans.common.common import (
    apply_virtual_columns_eap_items,
    attribute_key_to_expression_eap_items,
)

_DEFAULT_ROW_LIMIT = 10_000

OP_TO_EXPR = {
    Column.BinaryFormula.OP_ADD: f.plus,
    Column.BinaryFormula.OP_SUBTRACT: f.minus,
    Column.BinaryFormula.OP_MULTIPLY: f.multiply,
    Column.BinaryFormula.OP_DIVIDE: f.divide,
}


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
                    agg_filter.comparison_filter.conditional_aggregation,
                    attribute_key_to_expression_eap_items,
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
    groupby: List[Expression],
    order_by: Sequence[TraceItemTableRequest.OrderBy],
    request_meta: RequestMeta,
) -> Sequence[OrderBy]:
    if len(order_by) == 1:
        order = order_by[0]
        if order.column.key.name == "sentry.timestamp" and len(groupby) == 0:
            direction = (
                OrderByDirection.DESC if order.descending else OrderByDirection.ASC
            )
            return [
                OrderBy(
                    direction=direction,
                    expression=snuba_column("organization_id"),
                ),
                OrderBy(
                    direction=direction,
                    expression=snuba_column("project_id"),
                ),
                OrderBy(
                    direction=direction,
                    expression=snuba_column("item_type"),
                ),
                OrderBy(
                    direction=direction,
                    expression=snuba_column("timestamp"),
                ),
            ]

    res: list[OrderBy] = []
    for x in order_by:
        direction = OrderByDirection.DESC if x.descending else OrderByDirection.ASC
        if x.column.HasField("key"):
            res.append(
                OrderBy(
                    direction=direction,
                    expression=attribute_key_to_expression_eap_items(x.column.key),
                )
            )
        elif x.column.HasField("conditional_aggregation"):
            res.append(
                OrderBy(
                    direction=direction,
                    expression=aggregation_to_expression(
                        x.column.conditional_aggregation,
                        attribute_key_to_expression_eap_items,
                        use_sampling_factor(request_meta),
                    ),
                )
            )
        elif x.column.HasField("formula"):
            res.append(
                OrderBy(
                    direction=direction,
                    expression=_formula_to_expression(x.column.formula, request_meta),
                )
            )
    return res


def _get_reliability_context_columns(
    column: Column, request_meta: RequestMeta
) -> list[SelectedExpression]:
    """
    extrapolated aggregates need to request extra columns to calculate the reliability of the result.
    this function returns the list of columns that need to be requested.
    """
    if not (column.HasField("conditional_aggregation")):
        return []

    if (
        column.conditional_aggregation.extrapolation_mode
        == ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED
    ):
        context_columns = []
        confidence_interval_column = get_confidence_interval_column(
            column.conditional_aggregation,
            attribute_key_to_expression_eap_items,
        )
        if confidence_interval_column is not None:
            context_columns.append(
                SelectedExpression(
                    name=confidence_interval_column.alias,
                    expression=confidence_interval_column,
                )
            )

        average_sample_rate_column = get_average_sample_rate_column(
            column.conditional_aggregation,
            attribute_key_to_expression_eap_items,
        )
        count_column = get_count_column(
            column.conditional_aggregation,
            attribute_key_to_expression_eap_items,
        )
        context_columns.append(
            SelectedExpression(
                name=average_sample_rate_column.alias,
                expression=average_sample_rate_column,
            )
        )
        context_columns.append(
            SelectedExpression(name=count_column.alias, expression=count_column)
        )
        return context_columns
    return []


def _formula_to_expression(
    formula: Column.BinaryFormula, request_meta: RequestMeta
) -> Expression:
    formula_expr = OP_TO_EXPR[formula.op](
        _column_to_expression(formula.left, request_meta),
        _column_to_expression(formula.right, request_meta),
    )
    match formula.WhichOneof("default_value"):
        case None:
            return formula_expr
        case "default_value_double":
            return f.coalesce(formula_expr, formula.default_value_double)
        case "default_value_int64":
            return f.coalesce(formula_expr, formula.default_value_int64)
        case default:
            raise BadSnubaRPCRequestException(
                f"Unknown default_value in formula. Expected default_value_double or default_value_int64 but got {default}"
            )


def _column_to_expression(column: Column, request_meta: RequestMeta) -> Expression:
    """
    Given a column protobuf object, translates it into a Expression object and returns it.
    """
    if column.HasField("key"):
        return attribute_key_to_expression_eap_items(column.key)
    elif column.HasField("conditional_aggregation"):
        function_expr = aggregation_to_expression(
            column.conditional_aggregation,
            attribute_key_to_expression_eap_items,
            use_sampling_factor(request_meta),
        )
        # aggregation label may not be set and the column label takes priority anyways.
        function_expr = replace(function_expr, alias=column.label)
        return function_expr
    elif column.HasField("formula"):
        formula_expr = _formula_to_expression(column.formula, request_meta)
        formula_expr = replace(formula_expr, alias=column.label)
        return formula_expr
    elif column.HasField("literal"):
        return literal(column.literal.val_double)
    else:
        raise BadSnubaRPCRequestException(
            "Column is not one of: aggregate, attribute key, or formula"
        )


def build_query(request: TraceItemTableRequest) -> Query:
    entity = Entity(
        key=EntityKey("eap_items"),
        schema=get_entity(EntityKey("eap_items")).get_data_model(),
        sample=None,
    )

    selected_columns = []
    for column in request.columns:
        # The key_col expression alias may differ from the column label. That is okay
        # the attribute key name is used in the groupby, the column label is just the name of
        # the returned attribute value
        selected_columns.append(
            SelectedExpression(
                name=column.label,
                expression=_column_to_expression(column, request.meta),
            )
        )
        selected_columns.extend(_get_reliability_context_columns(column, request.meta))

    item_type_conds = [
        f.equals(snuba_column("item_type"), request.meta.trace_item_type)
    ]
    groupby = [
        attribute_key_to_expression_eap_items(attr_key) for attr_key in request.group_by
    ]
    res = Query(
        from_clause=entity,
        selected_columns=selected_columns,
        condition=base_conditions_and(
            request.meta,
            trace_item_filters_to_expression(
                request.filter,
                attribute_key_to_expression_eap_items,
            ),
            *item_type_conds,
        ),
        order_by=_convert_order_by(
            groupby,
            request.order_by,
            request.meta,
        ),
        groupby=groupby,
        # Only support offset page tokens for now
        offset=request.page_token.offset,
        # protobuf sets limit to 0 by default if it is not set,
        # give it a default value that will actually return data
        limit=request.limit if request.limit > 0 else _DEFAULT_ROW_LIMIT,
        having=(
            aggregation_filter_to_expression(request.aggregation_filter, request.meta)
            if request.HasField("aggregation_filter")
            else None
        ),
    )
    treeify_or_and_conditions(res)
    apply_virtual_columns_eap_items(res, request.virtual_column_contexts)
    add_existence_check_to_subscriptable_references(res)
    return res


def _get_page_token(
    request: TraceItemTableRequest, response: list[TraceItemColumnValues]
) -> PageToken:
    if not response:
        return PageToken(offset=0)
    num_rows = len(response[0].results)
    return PageToken(offset=request.page_token.offset + num_rows)


class ResolverTraceItemTableEAPItems:
    def resolve(
        self,
        in_msg: TraceItemTableRequest,
        timer: Timer,
        metrics_backend: MetricsBackend,
    ) -> TraceItemTableResponse:
        query_settings = (
            setup_trace_query_settings() if in_msg.meta.debug else HTTPQuerySettings()
        )

        res = run_query_to_correct_tier(
            in_msg, query_settings, timer, build_query  # type: ignore
        )
        column_values = convert_results(in_msg, res.result.get("data", []))
        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            [res],
            [timer],
        )
        return TraceItemTableResponse(
            column_values=column_values,
            page_token=_get_page_token(in_msg, column_values),
            meta=response_meta,
        )
