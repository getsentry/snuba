import uuid
from dataclasses import replace
from itertools import islice
from typing import List, Optional, Sequence

import sentry_sdk
from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    AggregationComparisonFilter,
    AggregationFilter,
    Column,
    TraceItemColumnValues,
    TraceItemTableRequest,
    TraceItemTableResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import (
    PageToken,
    RequestMeta,
    TraceItemType,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import ExtrapolationMode

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond
from snuba.query.dsl import column as snuba_column
from snuba.query.dsl import in_cond, literal, literals_array, or_cond
from snuba.query.expressions import Expression
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.settings import ENABLE_FORMULA_RELIABILITY_DEFAULT
from snuba.state import get_int_config
from snuba.utils.metrics.timer import Timer
from snuba.web.query import run_query
from snuba.web.rpc.common.common import (
    add_existence_check_to_subscriptable_references,
    base_conditions_and,
    timestamp_in_range_condition,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
    use_sampling_factor,
)
from snuba.web.rpc.common.debug_info import (
    extract_response_meta,
    setup_trace_query_settings,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.common.pagination import FlexibleTimeWindowPage
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingDecision,
    TimeWindow,
)
from snuba.web.rpc.v1.resolvers import ResolverTraceItemTable
from snuba.web.rpc.v1.resolvers.common.aggregation import (
    aggregation_to_expression,
    get_average_sample_rate_column,
    get_confidence_interval_column,
    get_count_column,
)
from snuba.web.rpc.v1.resolvers.common.cross_item_queries import (
    get_trace_ids_for_cross_item_query,
)
from snuba.web.rpc.v1.resolvers.common.trace_item_table import convert_results
from snuba.web.rpc.v1.resolvers.R_eap_items.common.common import (
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
            if agg_filter.comparison_filter.HasField(
                "formula"
            ) and agg_filter.comparison_filter.HasField("conditional_aggregation"):
                raise BadSnubaRPCRequestException(
                    "Cannot use formula and conditional aggregation in the same ComparisonFilter"
                )
            elif agg_filter.comparison_filter.HasField("formula"):
                return op_expr(
                    _formula_to_expression(agg_filter.comparison_filter.formula, request_meta),
                    agg_filter.comparison_filter.val,
                )
            return op_expr(
                aggregation_to_expression(
                    agg_filter.comparison_filter.conditional_aggregation,
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
            raise BadSnubaRPCRequestException(f"Unsupported aggregation filter type: {default}")


def _convert_order_by(
    groupby: List[Expression],
    order_by: Sequence[TraceItemTableRequest.OrderBy],
    request_meta: RequestMeta,
) -> Sequence[OrderBy]:
    res: list[OrderBy] = []
    for i, x in enumerate(order_by):
        direction = OrderByDirection.DESC if x.descending else OrderByDirection.ASC

        # OPTIMIZATION: If the first ORDER BY is timestamp and there is only 1
        # project and no group bys, it means we can replace it with the following
        # ORDER BY which matches the table's ORDER BY to take advantage of the
        # `optimize_read_in_order` setting.
        if (
            i == 0
            and len(request_meta.project_ids) == 1
            and len(groupby) == 0
            and x.column.HasField("key")
            and x.column.key.name == "sentry.timestamp"
        ):
            res.extend(
                [
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
            )
        elif x.column.HasField("key"):
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

    If alias_prefix is provided, it will be prepended to the alias of the returned columns.
    """

    if column.HasField("formula"):
        if not get_int_config("enable_formula_reliability", ENABLE_FORMULA_RELIABILITY_DEFAULT):
            return []
        # also query for the left and right parts of the formula separately
        # this will be used later to calculate the reliability of the formula
        # ex: SELECT agg1/agg2 will become SELECT agg1/agg2, agg1, agg2
        context_cols = []
        for col in [column.formula.left, column.formula.right]:
            if not col.HasField("formula"):
                context_cols.append(
                    SelectedExpression(
                        name=col.label,
                        expression=_column_to_expression(col, request_meta),
                    )
                )
            context_cols.extend(_get_reliability_context_columns(col, request_meta))

        return context_cols

    if not (column.HasField("conditional_aggregation")):
        return []

    if (
        column.conditional_aggregation.extrapolation_mode
        == ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED
    ):
        context_columns = []
        confidence_interval_column = get_confidence_interval_column(
            column.conditional_aggregation,
            attribute_key_to_expression,
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
            attribute_key_to_expression,
        )
        context_columns.append(
            SelectedExpression(
                name=average_sample_rate_column.alias,
                expression=average_sample_rate_column,
            )
        )

        count_column = get_count_column(
            column.conditional_aggregation,
            attribute_key_to_expression,
        )
        context_columns.append(SelectedExpression(name=count_column.alias, expression=count_column))
        return context_columns
    return []


def _formula_to_expression(formula: Column.BinaryFormula, request_meta: RequestMeta) -> Expression:
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
        return attribute_key_to_expression(column.key)
    elif column.HasField("conditional_aggregation"):
        function_expr = aggregation_to_expression(
            column.conditional_aggregation,
            attribute_key_to_expression,
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


def _get_offset_from_page_token(page_token: PageToken | None) -> int:
    if page_token is None:
        return 0
    page = FlexibleTimeWindowPage.decode(page_token)
    if page.offset is not None:
        return page.offset
    return 0


def build_query(
    request: TraceItemTableRequest,
    time_window: TimeWindow | None = None,
    timer: Optional[Timer] = None,
) -> Query:
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

    item_type_conds = [f.equals(snuba_column("item_type"), request.meta.trace_item_type)]

    # Handle cross item queries by first getting trace IDs
    additional_conditions: List[Expression] = []
    if request.trace_filters and timer is not None:
        trace_ids = get_trace_ids_for_cross_item_query(
            request, request.meta, list(request.trace_filters), timer
        )
        additional_conditions.append(
            in_cond(
                snuba_column("trace_id"),
                literals_array(None, [literal(trace_id) for trace_id in trace_ids]),
            )
        )
    if time_window is not None:
        additional_conditions.append(
            timestamp_in_range_condition(
                time_window.start_timestamp.seconds,
                time_window.end_timestamp.seconds,
            )
        )
    groupby = [attribute_key_to_expression(attr_key) for attr_key in request.group_by]

    res = Query(
        from_clause=entity,
        selected_columns=selected_columns,
        condition=base_conditions_and(
            request.meta,
            trace_item_filters_to_expression(
                request.filter,
                attribute_key_to_expression,
            ),
            *item_type_conds,
            *additional_conditions,
        ),
        order_by=_convert_order_by(
            groupby,
            request.order_by,
            request.meta,
        ),
        groupby=groupby,
        # Only support offset page tokens for now
        offset=_get_offset_from_page_token(request.page_token),
        # protobuf sets limit to 0 by default if it is not set,
        # give it a default value that will actually return data
        # we add 1 to the limit to know if there are more rows to fetch
        limit=(request.limit if request.limit > 0 else _DEFAULT_ROW_LIMIT) + 1,
        having=(
            aggregation_filter_to_expression(request.aggregation_filter, request.meta)
            if request.HasField("aggregation_filter")
            else None
        ),
    )
    treeify_or_and_conditions(res)
    apply_virtual_columns(res, request.virtual_column_contexts)
    add_existence_check_to_subscriptable_references(res)
    return res


def _get_page_token(
    request: TraceItemTableRequest,
    response: list[TraceItemColumnValues],
    # amount of rows returned in the DB request (which can be one more than the limit)
    num_rows_returned: int,
    # time window of the original request without any adjustments by routing strategies
    original_time_window: TimeWindow,
    # time window of the current request after any adjustments by routing strategies
    time_window: TimeWindow | None,
) -> PageToken:
    if not response:
        return PageToken(offset=0)
    current_offset = _get_offset_from_page_token(request.page_token)
    num_rows_in_response = len(response[0].results)
    if time_window is not None:
        if num_rows_returned > request.limit:
            # there are more rows in this window so we maintain the same time window and advance the offset
            return FlexibleTimeWindowPage(
                time_window.start_timestamp,
                time_window.end_timestamp,
                current_offset + num_rows_in_response,
            ).encode()
        else:
            # there are no more rows in this window so we return the next window
            # return the next window where the end timestamp is the start timestamp and the start timestamp is the original start timestamp
            # the routing strategy will properly truncate the time window of the next request
            return FlexibleTimeWindowPage(
                original_time_window.start_timestamp, time_window.start_timestamp, 0
            ).encode()
    else:
        return PageToken(offset=request.page_token.offset + num_rows_in_response)


def _build_snuba_request(
    request: TraceItemTableRequest,
    query_settings: HTTPQuerySettings,
    time_window: TimeWindow | None = None,
    timer: Optional[Timer] = None,
) -> SnubaRequest:
    if request.meta.trace_item_type == TraceItemType.TRACE_ITEM_TYPE_LOG:
        team = "ourlogs"
        feature = "ourlogs"
        parent_api = "ourlog_trace_item_table"
    else:
        team = "eap"
        feature = "eap"
        parent_api = "eap_span_samples"

    return SnubaRequest(
        id=uuid.UUID(request.meta.request_id),
        original_body=MessageToDict(request),
        query=build_query(request, time_window, timer),
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            referrer=request.meta.referrer,
            team=team,
            feature=feature,
            tenant_ids={
                "organization_id": request.meta.organization_id,
                "referrer": request.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api=parent_api,
        ),
    )


class ResolverTraceItemTableEAPItems(ResolverTraceItemTable):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED

    def resolve(
        self,
        in_msg: TraceItemTableRequest,
        routing_decision: RoutingDecision,
    ) -> TraceItemTableResponse:
        query_settings = setup_trace_query_settings() if in_msg.meta.debug else HTTPQuerySettings()
        try:
            routing_decision.strategy.merge_clickhouse_settings(routing_decision, query_settings)
            query_settings.set_sampling_tier(routing_decision.tier)
        except Exception as e:
            sentry_sdk.capture_message(f"Error merging clickhouse settings: {e}")
        original_time_window = TimeWindow(
            start_timestamp=in_msg.meta.start_timestamp, end_timestamp=in_msg.meta.end_timestamp
        )
        snuba_request = _build_snuba_request(
            in_msg, query_settings, routing_decision.time_window, self._timer
        )
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=self._timer,
        )
        routing_decision.routing_context.query_result = res
        # we added 1 to the limit to know if there are more rows to fetch
        # so we need to remove the last row
        total_rows = len(res.result.get("data", []))
        data = iter(res.result.get("data", []))

        if in_msg.limit > 0 and total_rows > in_msg.limit:
            data = islice(data, in_msg.limit)
        column_values = convert_results(in_msg, data)
        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            [res],
            [self._timer],
        )
        return TraceItemTableResponse(
            column_values=column_values,
            page_token=_get_page_token(
                in_msg,
                column_values,
                total_rows,
                original_time_window,
                routing_decision.time_window,
            ),
            meta=response_meta,
        )
