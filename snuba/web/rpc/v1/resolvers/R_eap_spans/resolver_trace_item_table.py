import uuid
from dataclasses import replace
from typing import Sequence

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    AggregationComparisonFilter,
    AggregationFilter,
    Column,
    TraceItemColumnValues,
    TraceItemTableRequest,
    TraceItemTableResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import PageToken, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import ExtrapolationMode

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
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import (
    extract_response_meta,
    setup_trace_query_settings,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers import ResolverTraceItemTable
from snuba.web.rpc.v1.resolvers.common.aggregation import (
    aggregation_to_expression,
    get_average_sample_rate_column,
    get_confidence_interval_column,
    get_count_column,
)
from snuba.web.rpc.v1.resolvers.common.trace_item_table import convert_results
from snuba.web.rpc.v1.resolvers.R_eap_spans.common.common import (
    apply_virtual_columns,
    attribute_key_to_expression,
)

_DEFAULT_ROW_LIMIT = 10_000

OP_TO_EXPR = {
    Column.BinaryFormula.OP_ADD: f.plus,
    Column.BinaryFormula.OP_SUBTRACT: f.minus,
    Column.BinaryFormula.OP_MULTIPLY: f.multiply,
    Column.BinaryFormula.OP_DIVIDE: f.divide,
}


def aggregation_filter_to_expression(agg_filter: AggregationFilter) -> Expression:
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
                aggregation_to_expression(agg_filter.comparison_filter.aggregation),
                agg_filter.comparison_filter.val,
            )
        case "and_filter":
            if len(agg_filter.and_filter.filters) < 2:
                raise BadSnubaRPCRequestException(
                    f"AND filter must have at least two filters, only got {len(agg_filter.and_filter.filters)}"
                )
            return and_cond(
                *(
                    aggregation_filter_to_expression(x)
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
                    aggregation_filter_to_expression(x)
                    for x in agg_filter.or_filter.filters
                )
            )
        case default:
            raise BadSnubaRPCRequestException(
                f"Unsupported aggregation filter type: {default}"
            )


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
        elif x.column.HasField("formula"):
            res.append(
                OrderBy(
                    direction=direction,
                    expression=_formula_to_expression(x.column.formula),
                )
            )
    return res


def _get_reliability_context_columns(column: Column) -> list[SelectedExpression]:
    """
    extrapolated aggregates need to request extra columns to calculate the reliability of the result.
    this function returns the list of columns that need to be requested.
    """
    if not column.HasField("aggregation"):
        return []

    if (
        column.aggregation.extrapolation_mode
        == ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED
    ):
        context_columns = []
        confidence_interval_column = get_confidence_interval_column(column.aggregation)
        if confidence_interval_column is not None:
            context_columns.append(
                SelectedExpression(
                    name=confidence_interval_column.alias,
                    expression=confidence_interval_column,
                )
            )

        average_sample_rate_column = get_average_sample_rate_column(column.aggregation)
        count_column = get_count_column(column.aggregation)
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


def _formula_to_expression(formula: Column.BinaryFormula) -> Expression:
    return OP_TO_EXPR[formula.op](
        _column_to_expression(formula.left),
        _column_to_expression(formula.right),
    )


def _column_to_expression(column: Column) -> Expression:
    """
    Given a column protobuf object, translates it into a Expression object and returns it.
    """
    if column.HasField("key"):
        return attribute_key_to_expression(column.key)
    elif column.HasField("aggregation"):
        function_expr = aggregation_to_expression(column.aggregation)
        # aggregation label may not be set and the column label takes priority anyways.
        function_expr = replace(function_expr, alias=column.label)
        return function_expr
    elif column.HasField("formula"):
        formula_expr = _formula_to_expression(column.formula)
        formula_expr = replace(formula_expr, alias=column.label)
        return formula_expr
    else:
        raise BadSnubaRPCRequestException(
            "Column is not one of: aggregate, attribute key, or formula"
        )


def _build_query(request: TraceItemTableRequest) -> Query:
    # TODO: This is hardcoded still
    entity = Entity(
        key=EntityKey("eap_spans"),
        schema=get_entity(EntityKey("eap_spans")).get_data_model(),
        sample=None,
    )

    selected_columns = []
    for column in request.columns:
        # The key_col expression alias may differ from the column label. That is okay
        # the attribute key name is used in the groupby, the column label is just the name of
        # the returned attribute value
        selected_columns.append(
            SelectedExpression(
                name=column.label, expression=_column_to_expression(column)
            )
        )
        selected_columns.extend(_get_reliability_context_columns(column))

    res = Query(
        from_clause=entity,
        selected_columns=selected_columns,
        condition=base_conditions_and(
            request.meta,
            trace_item_filters_to_expression(
                request.filter, attribute_key_to_expression
            ),
        ),
        order_by=_convert_order_by(request.order_by),
        groupby=[
            attribute_key_to_expression(attr_key) for attr_key in request.group_by
        ],
        # Only support offset page tokens for now
        offset=request.page_token.offset,
        # protobuf sets limit to 0 by default if it is not set,
        # give it a default value that will actually return data
        limit=request.limit if request.limit > 0 else _DEFAULT_ROW_LIMIT,
        having=aggregation_filter_to_expression(request.aggregation_filter)
        if request.HasField("aggregation_filter")
        else None,
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
            parent_api="eap_span_samples",
        ),
    )


def _get_page_token(
    request: TraceItemTableRequest, response: list[TraceItemColumnValues]
) -> PageToken:
    if not response:
        return PageToken(offset=0)
    num_rows = len(response[0].results)
    return PageToken(offset=request.page_token.offset + num_rows)


class ResolverTraceItemTableEAPSpans(ResolverTraceItemTable):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_SPAN

    def resolve(self, in_msg: TraceItemTableRequest) -> TraceItemTableResponse:
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
