from collections import defaultdict
from dataclasses import replace
from datetime import datetime
from typing import Any, Callable, Dict, Iterable

from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import DataPoint
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    Expression as ProtoExpression,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    TimeSeries,
    TimeSeriesRequest,
    TimeSeriesResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeKey,
    ExtrapolationMode,
)

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal
from snuba.query.expressions import Expression
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.web.rpc.common.common import (
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
from snuba.web.rpc.v1.resolvers import ResolverTimeSeries
from snuba.web.rpc.v1.resolvers.common.aggregation import (
    ExtrapolationContext,
    aggregation_to_expression,
    get_average_sample_rate_column,
    get_confidence_interval_column,
    get_count_column,
)
from snuba.web.rpc.v1.resolvers.R_eap_items.common.common import (
    attribute_key_to_expression_eap_items,
)
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.sampling_in_storage_util import (
    run_query_to_correct_tier,
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
    return attribute_key_to_expression_eap_items


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

    # to convert the results, need to know which were the groupby columns and which ones
    # were aggregations
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
    result_timeseries_timestamp_to_row: defaultdict[
        tuple[str, str], dict[int, Dict[str, Any]]
    ] = defaultdict(dict)

    query_duration = (
        request.meta.end_timestamp.seconds - request.meta.start_timestamp.seconds
    )
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
                extrapolation_context = ExtrapolationContext.from_row(
                    timeseries.label, row_data
                )
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
    return result_timeseries.values()


def _get_reliability_context_columns(
    expressions: Iterable[ProtoExpression],
    request_meta: RequestMeta,
) -> list[SelectedExpression]:
    # this reliability logic ignores formulas, meaning formulas may not properly support reliability
    additional_context_columns = []

    aggregates = []
    for e in expressions:
        if e.WhichOneof("expression") == "conditional_aggregation":
            # ignore formulas
            aggregates.append(e.conditional_aggregation)

    for aggregation in aggregates:
        if (
            aggregation.extrapolation_mode
            == ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED
        ):
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
    return additional_context_columns


def _proto_expression_to_ast_expression(
    expr: ProtoExpression, request_meta: RequestMeta
) -> Expression:
    match expr.WhichOneof("expression"):
        case "conditional_aggregation":
            return aggregation_to_expression(
                expr.conditional_aggregation,
                (attribute_key_to_expression_eap_items),
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
                    formula_expr = f.coalesce(
                        formula_expr, expr.formula.default_value_double
                    )
                case "default_value_int64":
                    formula_expr = f.coalesce(
                        formula_expr, expr.formula.default_value_int64
                    )
                case default:
                    raise BadSnubaRPCRequestException(
                        f"Unknown default_value in formula. Expected default_value_double or default_value_int64 but got {default}"
                    )
            return replace(formula_expr, alias=expr.label)
        case "literal":
            return literal(expr.literal.val_double)
        case default:
            raise ValueError(f"Unknown expression type: {default}")


def build_query(request: TimeSeriesRequest) -> Query:
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

    additional_context_columns = _get_reliability_context_columns(
        request.expressions, request.meta
    )

    groupby_columns = [
        SelectedExpression(
            name=attr_key.name,
            expression=_get_attribute_key_to_expression_function(request.meta)(
                attr_key
            ),
        )
        for attr_key in request.group_by
    ]
    item_type_conds = [f.equals(column("item_type"), request.meta.trace_item_type)]

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
            *item_type_conds,
        ),
        groupby=[
            column("time_slot"),
            *[
                _get_attribute_key_to_expression_function(request.meta)(attr_key)
                for attr_key in request.group_by
            ],
        ],
        order_by=[
            OrderBy(expression=column("time_slot"), direction=OrderByDirection.ASC)
        ],
    )
    treeify_or_and_conditions(res)
    return res


class ResolverTimeSeriesEAPItems(ResolverTimeSeries):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED

    def resolve(
        self,
        in_msg: TimeSeriesRequest,
    ) -> TimeSeriesResponse:
        # aggregations field is deprecated, it gets converted to request.expressions
        # if the user passes it in
        assert len(in_msg.aggregations) == 0

        query_settings = (
            setup_trace_query_settings() if in_msg.meta.debug else HTTPQuerySettings()
        )
        res = run_query_to_correct_tier(
            in_msg,
            query_settings,
            self._timer,
            build_query,  # type: ignore
        )

        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            [res],
            [self._timer],
        )

        return TimeSeriesResponse(
            result_timeseries=list(
                _convert_result_timeseries(
                    in_msg,
                    res.result.get("data", []),
                )
            ),
            meta=response_meta,
        )
