import uuid
from collections import defaultdict
from dataclasses import replace
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, Optional

import sentry_sdk
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    DataPoint,
    TimeSeries,
    TimeSeriesRequest,
    TimeSeriesResponse,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    Expression as ProtoExpression,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeKey,
    ExtrapolationMode,
)

from snuba import state
from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.downsampled_storage_tiers import Tier
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, in_cond, literal
from snuba.query.expressions import DangerousRawSQL, Expression
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.utils.metrics.timer import Timer
from snuba.web.query import run_query
from snuba.web.rpc.common.common import (
    add_existence_check_to_subscriptable_references,
    attribute_key_to_expression,
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
    use_sampling_factor,
    valid_sampling_factor_conditions,
)
from snuba.web.rpc.common.debug_info import (
    extract_response_meta,
    setup_trace_query_settings,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingDecision,
)
from snuba.web.rpc.v1.resolvers import ResolverTimeSeries
from snuba.web.rpc.v1.resolvers.common.aggregation import (
    ExtrapolationContext,
    aggregation_to_expression,
    get_average_sample_rate_column,
    get_confidence_interval_column,
    get_count_column,
)
from snuba.web.rpc.v1.resolvers.common.cross_item_queries import (
    get_trace_ids_sql_for_cross_item_query,
)
from snuba.web.rpc.v1.resolvers.common.formula_reliability import (
    FormulaReliabilityCalculator,
)

OP_TO_EXPR = {
    ProtoExpression.BinaryFormula.OP_ADD: f.plus,
    ProtoExpression.BinaryFormula.OP_SUBTRACT: f.minus,
    ProtoExpression.BinaryFormula.OP_MULTIPLY: f.multiply,
    ProtoExpression.BinaryFormula.OP_DIVIDE: f.divide,
}


def _get_attribute_key_to_expression_function(
    request_meta: RequestMeta,
) -> Callable[[AttributeKey], Expression]:
    return attribute_key_to_expression


def _convert_result_timeseries(
    request: TimeSeriesRequest, data: list[Dict[str, Any]]
) -> Iterable[TimeSeries]:
    """This function takes the results of the clickhouse query and converts it to a list of TimeSeries objects. It also handles
    zerofilling data points where data was not present for a specific bucket.

    Example:
    data is a list of dictionaries that look like this:

    >>> [
    >>>     {"time": "2024-4-20 16:20:00", "sum(sentry.duration)": 1235, "p95(sentry.duration)": 123456, "group_by_attr_1": "g1", "group_by_attr_2": "a1"}
    >>>     {"time": "2024-4-20 16:20:00", "sum(sentry.duration)": 1235, "p95(sentry.duration)": 123456, "group_by_attr_1": "g1", "group_by_attr_2": "a2"}
    >>>     {"time": "2024-4-20 16:20:00", "sum(sentry.duration)": 1235, "p95(sentry.duration)": 123456, "group_by_attr_1": "g2", "group_by_attr_2": "a1"}
    >>>     {"time": "2024-4-20 16:20:00", "sum(sentry.duration)": 1235, "p95(sentry.duration)": 123456, "group_by_attr_1": "g2", "group_by_attr_2": "a2"}
    >>>     # next time bucket starts below

    >>>     {"time": "2024-4-20 16:21:00", "sum(sentry.duration)": 1235, "p95(sentry.duration)": 123456, "group_by_attr_1": "g1", "group_by_attr_2": "a1"}
    >>>     # here you can see that not every timeseries had data in every time bucket
    >>>     {"time": "2024-4-20 16:22:00", "sum(sentry.duration)": 1235, "p95(sentry.duration)": 123456, "group_by_attr_1": "g1", "group_by_attr_2": "a2"}
    >>>     {"time": "2024-4-20 16:23:00", "sum(sentry.duration)": 1235, "p95(sentry.duration)": 123456, "group_by_attr_1": "g2", "group_by_attr_2": "a1"}
    >>>     {"time": "2024-4-20 16:24:00", "sum(sentry.duration)": 1235, "p95(sentry.duration)": 123456, "group_by_attr_1": "g2", "group_by_attr_2": "a2"}

    >>>     ...
    >>> ]

    In this example we have 8 different timeseries and they are all sparse:

        sum(sentry.duration), group_by_attributes = {"group_by_attr_1": "g1", "group_by_attr_2": "a1"}
        sum(sentry.duration), group_by_attributes = {"group_by_attr_1": "g1", "group_by_attr_2": "a2"}
        sum(sentry.duration), group_by_attributes = {"group_by_attr_1": "g2", "group_by_attr_2": "a1"}
        sum(sentry.duration), group_by_attributes = {"group_by_attr_1": "g2", "group_by_attr_2": "a2"}


        p95(sentry.duration), group_by_attributes = {"group_by_attr_1": "g1", "group_by_attr_2": "a1"}
        p95(sentry.duration), group_by_attributes = {"group_by_attr_1": "g1", "group_by_attr_2": "a2"}
        p95(sentry.duration), group_by_attributes = {"group_by_attr_1": "g2", "group_by_attr_2": "a1"}
        p95(sentry.duration), group_by_attributes = {"group_by_attr_1": "g2", "group_by_attr_2": "a2"}

    Returns:
        an Iterable of TimeSeries objects where each possible bucket has a DataPoint with `data_present` set correctly

    """

    # the aggregations that we will include in the result
    aggregation_labels = set([expr.label for expr in request.expressions])

    group_by_labels = set([attr.name for attr in request.group_by])

    # create a mapping with (all the group by attribute key,val pairs as strs, label name)
    # In the example in the docstring it would look like:
    # { ("group_by_attr_1,g1|group_by_attr_2,g2", "sum(sentry.duration"): TimeSeries()}
    result_timeseries: dict[tuple[str, str], TimeSeries] = {}

    # create a mapping for each timeseries of timestamp: row to fill data points not returned in the query
    # {
    #   ("group_by_attr_1,g1|group_by_attr_2,g2", "sum(sentry.duration"): {
    #       time_converted_to_integer_timestamp: row_data_for_that_time_bucket
    #   }
    # }
    result_timeseries_timestamp_to_row: defaultdict[tuple[str, str], dict[int, Dict[str, Any]]] = (
        defaultdict(dict)
    )

    query_duration = request.meta.end_timestamp.seconds - request.meta.start_timestamp.seconds
    time_buckets = [
        Timestamp(seconds=(request.meta.start_timestamp.seconds) + secs)
        for secs in range(0, query_duration, request.granularity_secs)
    ]

    # this loop fill in our pre-computed dictionaries so that we can zerofill later
    for row in data:
        group_by_map = {}

        for col_name, col_value in row.items():
            if col_name in group_by_labels:
                group_by_map[col_name] = str(col_value)

        group_by_key = "|".join([f"{k},{v}" for k, v in group_by_map.items()])
        for col_name in aggregation_labels:
            if not result_timeseries.get((group_by_key, col_name), None):
                result_timeseries[(group_by_key, col_name)] = TimeSeries(
                    group_by_attributes=group_by_map,
                    label=col_name,
                    buckets=time_buckets,
                )
            result_timeseries_timestamp_to_row[(group_by_key, col_name)][
                int(datetime.fromisoformat(row["time"]).timestamp())
            ] = row

    # Go through every possible time bucket in the query, if there's row data for it, fill in its data
    # otherwise put a dummy datapoint in
    for bucket in time_buckets:
        for timeseries_key, timeseries in result_timeseries.items():
            row_data = result_timeseries_timestamp_to_row.get(timeseries_key, {}).get(
                bucket.seconds
            )
            if not row_data:
                timeseries.data_points.append(DataPoint(data=0, data_present=False))
            else:
                extrapolation_context = ExtrapolationContext.from_row(timeseries.label, row_data)
                if row_data.get(timeseries.label, None) is not None:
                    timeseries.data_points.append(
                        DataPoint(
                            data=row_data[timeseries.label],
                            data_present=True,
                            avg_sampling_rate=extrapolation_context.average_sample_rate,
                            sample_count=extrapolation_context.sample_count,
                            reliability=extrapolation_context.reliability,
                        )
                    )
                else:
                    timeseries.data_points.append(DataPoint(data=0, data_present=False))

    frc = FormulaReliabilityCalculator(request, data, time_buckets)
    for timeseries in result_timeseries.values():
        if timeseries.label in frc:
            extrapolation_contexts = frc.get(timeseries.label)

            for i in range(len(timeseries.data_points)):
                context = extrapolation_contexts[i]
                timeseries.data_points[i].avg_sampling_rate = context.average_sample_rate
                timeseries.data_points[i].sample_count = context.sample_count
                timeseries.data_points[i].reliability = context.reliability
    _remove_non_requested_expressions(request.expressions, result_timeseries)

    return result_timeseries.values()


def _remove_non_requested_expressions(
    expressions: Iterable[ProtoExpression],
    result_timeseries: dict[tuple[str, str], TimeSeries],
) -> None:
    requested_expressions = set([expr.label for expr in expressions])
    to_remove = []
    for timeseries_key in result_timeseries.keys():
        if timeseries_key[1] not in requested_expressions:
            to_remove.append(timeseries_key)
    for timeseries_key in to_remove:
        del result_timeseries[timeseries_key]


def _get_reliability_context_columns(
    expr: ProtoExpression,
    request_meta: RequestMeta,
) -> list[SelectedExpression]:
    # this reliability logic ignores formulas, meaning formulas may not properly support reliability
    additional_context_columns = []

    if (
        expr.WhichOneof("expression") == "conditional_aggregation"
        or expr.WhichOneof("expression") == "aggregation"
    ):
        which_oneof = expr.WhichOneof("expression")
        assert which_oneof in ["conditional_aggregation", "aggregation"]
        aggregation = getattr(expr, which_oneof)
        if aggregation.extrapolation_mode in [
            ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
            ExtrapolationMode.EXTRAPOLATION_MODE_CLIENT_ONLY,
            ExtrapolationMode.EXTRAPOLATION_MODE_SERVER_ONLY,
        ]:
            confidence_interval_column = get_confidence_interval_column(
                aggregation, _get_attribute_key_to_expression_function(request_meta)
            )
            if confidence_interval_column is not None:
                additional_context_columns.append(
                    SelectedExpression(
                        name=confidence_interval_column.alias,
                        expression=confidence_interval_column,
                    )
                )

            average_sample_rate_column = get_average_sample_rate_column(
                aggregation, _get_attribute_key_to_expression_function(request_meta)
            )
            additional_context_columns.append(
                SelectedExpression(
                    name=average_sample_rate_column.alias,
                    expression=average_sample_rate_column,
                )
            )
        count_column = get_count_column(
            aggregation, _get_attribute_key_to_expression_function(request_meta)
        )
        additional_context_columns.append(
            SelectedExpression(name=count_column.alias, expression=count_column)
        )
    elif expr.WhichOneof("expression") == "formula":
        # also query for the left and right parts of the formula separately
        # this will be used later to calculate the reliability of the formula
        # ex: SELECT agg1/agg2 will become SELECT agg1/agg2, agg1, agg2
        for e in [expr.formula.left, expr.formula.right]:
            if not e.HasField("formula"):
                additional_context_columns.append(
                    SelectedExpression(
                        name=e.label,
                        expression=_proto_expression_to_ast_expression(e, request_meta),
                    )
                )
            additional_context_columns.extend(_get_reliability_context_columns(e, request_meta))
    return additional_context_columns


def _proto_expression_to_ast_expression(
    expr: ProtoExpression, request_meta: RequestMeta
) -> Expression:
    match expr.WhichOneof("expression"):
        case "conditional_aggregation":
            return aggregation_to_expression(
                expr.conditional_aggregation,
                (attribute_key_to_expression),
                use_sampling_factor(request_meta),
            )
        case "formula":
            formula_expr = OP_TO_EXPR[expr.formula.op](
                _proto_expression_to_ast_expression(expr.formula.left, request_meta),
                _proto_expression_to_ast_expression(expr.formula.right, request_meta),
            )
            match expr.formula.WhichOneof("default_value"):
                case None:
                    pass
                case "default_value_double":
                    formula_expr = f.coalesce(formula_expr, expr.formula.default_value_double)
                case "default_value_int64":
                    formula_expr = f.coalesce(formula_expr, expr.formula.default_value_int64)
                case default:
                    raise BadSnubaRPCRequestException(
                        f"Unknown default_value in formula. Expected default_value_double or default_value_int64 but got {default}"
                    )
            return replace(formula_expr, alias=expr.label)
        case "literal":
            return literal(expr.literal.val_double)
        case default:
            raise ValueError(f"Unknown expression type: {default}")


def build_query(
    request: TimeSeriesRequest, sampling_tier: Optional[Tier] = None, timer: Optional[Timer] = None
) -> Query:
    entity = Entity(
        key=EntityKey("eap_items"),
        schema=get_entity(EntityKey("eap_items")).get_data_model(),
        sample=None,
    )

    aggregation_columns = [
        SelectedExpression(
            name=expr.label,
            expression=_proto_expression_to_ast_expression(expr, request.meta),
        )
        for expr in request.expressions
    ]

    additional_context_columns = []
    for expr in request.expressions:
        additional_context_columns.extend(_get_reliability_context_columns(expr, request.meta))

    groupby_columns = [
        SelectedExpression(
            name=attr_key.name,
            expression=_get_attribute_key_to_expression_function(request.meta)(attr_key),
        )
        for attr_key in request.group_by
    ]
    item_type_conds = [f.equals(column("item_type"), request.meta.trace_item_type)]

    # Handle cross item queries by first getting trace IDs
    additional_conditions = []
    if request.trace_filters and timer is not None and sampling_tier is not None:
        trace_ids_sql, _ = get_trace_ids_sql_for_cross_item_query(
            request, request.meta, list(request.trace_filters), sampling_tier, timer
        )
        additional_conditions.append(
            in_cond(
                column("trace_id"),
                DangerousRawSQL(None, f"({trace_ids_sql})"),
            )
        )

    res = Query(
        from_clause=entity,
        selected_columns=[
            # buckets time by granularity according to the start time of the request.
            # time_slot = start_time + (((timestamp - start_time) // granularity) * granularity)
            # Example:
            #   start_time = 1001
            #   end_time = 1901
            #   granularity = 300
            #   timestamps = [1201, 1002, 1302, 1400, 1700]
            #   buckets = [1001, 1301, 1601] # end time not included because it would be filtered out by the request
            SelectedExpression(
                name="time",
                expression=f.toDateTime(
                    f.plus(
                        request.meta.start_timestamp.seconds,
                        f.multiply(
                            f.intDiv(
                                f.minus(
                                    f.toUnixTimestamp(column("timestamp")),
                                    request.meta.start_timestamp.seconds,
                                ),
                                request.granularity_secs,
                            ),
                            request.granularity_secs,
                        ),
                    ),
                    alias="time_slot",
                ),
            ),
            *aggregation_columns,
            *groupby_columns,
            *additional_context_columns,
        ],
        granularity=request.granularity_secs,
        condition=base_conditions_and(
            request.meta,
            trace_item_filters_to_expression(
                request.filter, _get_attribute_key_to_expression_function(request.meta)
            ),
            valid_sampling_factor_conditions(),
            *item_type_conds,
            *additional_conditions,
        ),
        groupby=[
            column("time_slot"),
            *[
                _get_attribute_key_to_expression_function(request.meta)(attr_key)
                for attr_key in request.group_by
            ],
        ],
        order_by=[OrderBy(expression=column("time_slot"), direction=OrderByDirection.ASC)],
    )
    treeify_or_and_conditions(res)
    add_existence_check_to_subscriptable_references(res)
    return res


def _build_snuba_request(
    request: TimeSeriesRequest,
    query_settings: HTTPQuerySettings,
    sampling_tier: Optional[Tier] = None,
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
        query=build_query(request, sampling_tier, timer),
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


class ResolverTimeSeriesEAPItems(ResolverTimeSeries):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED

    def resolve(
        self,
        in_msg: TimeSeriesRequest,
        routing_decision: RoutingDecision,
    ) -> TimeSeriesResponse:
        # aggregations field is deprecated, it gets converted to request.expressions
        # if the user passes it in
        assert len(in_msg.aggregations) == 0

        # aggregation is deprecated, it gets converted to conditional_aggregation
        if state.get_int_config("aggregation_deprecation_enabled", 1):
            for expr in in_msg.expressions:
                if expr.WhichOneof("expression") == "aggregation":
                    raise RuntimeError(
                        "Unexpected state: aggregation is deprecated and should have been converted to conditional_aggregation"
                    )

        query_settings = setup_trace_query_settings() if in_msg.meta.debug else HTTPQuerySettings()
        try:
            routing_decision.strategy.merge_clickhouse_settings(routing_decision, query_settings)
            # When trace_filters are present and the feature is enabled, don't use sampling on the outer query
            # The inner query (getting trace IDs) will use sampling
            cross_item_queries_no_sample_outer = state.get_int_config(
                "cross_item_queries_no_sample_outer", 0
            )
            if not (in_msg.trace_filters and cross_item_queries_no_sample_outer):
                query_settings.set_sampling_tier(routing_decision.tier)
        except Exception as e:
            sentry_sdk.capture_message(f"Error merging clickhouse settings: {e}")

        snuba_request = _build_snuba_request(
            in_msg, query_settings, routing_decision.tier, self._timer
        )
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=self._timer,
        )

        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            [res],
            [self._timer],
        )

        routing_decision.routing_context.query_result = res
        return TimeSeriesResponse(
            result_timeseries=list(
                _convert_result_timeseries(
                    in_msg,
                    res.result.get("data", []),
                )
            ),
            meta=response_meta,
        )
