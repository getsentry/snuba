from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    Function,
)

from snuba.query.dsl import CurriedFunctions as cf
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal
from snuba.query.expressions import (
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    SubscriptableReference,
)
from snuba.web.rpc.common.common import attribute_key_to_expression

sampling_weight_column = column("sampling_weight")

# Z value for 95% confidence interval is 1.96
z_value = 1.96


def get_extrapolated_function(
    aggregation: AttributeAggregation,
) -> CurriedFunctionCall | FunctionCall | None:
    field = attribute_key_to_expression(aggregation.key)
    alias = aggregation.label if aggregation.label else None
    alias_dict = {"alias": alias} if alias else {}
    function_map_sample_weighted: dict[
        Function.ValueType, CurriedFunctionCall | FunctionCall
    ] = {
        Function.FUNCTION_SUM: f.sum(
            f.multiply(field, sampling_weight_column), **alias_dict
        ),
        Function.FUNCTION_AVERAGE: f.avgWeighted(
            field, sampling_weight_column, **alias_dict
        ),
        Function.FUNCTION_COUNT: (
            f.sumIf(
                sampling_weight_column,
                f.mapContains(field.column, field.key),
                **alias_dict,
            )  # this is ugly, but we do this because the optional attribute aggregation processor can't handle this case as we are not summing up the actual attribute
            if isinstance(field, SubscriptableReference)
            else f.sum(sampling_weight_column, **alias_dict)
        ),
        Function.FUNCTION_P50: cf.quantileTDigestWeighted(0.5)(
            field, sampling_weight_column, **alias_dict
        ),
        Function.FUNCTION_P90: cf.quantileTDigestWeighted(0.9)(
            field, sampling_weight_column, **alias_dict
        ),
        Function.FUNCTION_P95: cf.quantileTDigestWeighted(0.95)(
            field, sampling_weight_column, **alias_dict
        ),
        Function.FUNCTION_P99: cf.quantileTDigestWeighted(0.99)(
            field, sampling_weight_column, **alias_dict
        ),
        Function.FUNCTION_AVG: f.weightedAvg(
            field, sampling_weight_column, **alias_dict
        ),
        Function.FUNCTION_MAX: f.max(field, **alias_dict),
        Function.FUNCTION_MIN: f.min(field, **alias_dict),
        Function.FUNCTION_UNIQ: f.uniq(field, **alias_dict),
    }

    return function_map_sample_weighted.get(aggregation.aggregate)


def get_attribute_confidence_interval_alias(
    aggregation: AttributeAggregation,
) -> str | None:
    function_alias_map = {
        Function.FUNCTION_COUNT: "count",
        Function.FUNCTION_AVERAGE: "avg",
        Function.FUNCTION_SUM: "sum",
        Function.FUNCTION_P50: "p50",
        Function.FUNCTION_P90: "p90",
        Function.FUNCTION_P95: "p95",
        Function.FUNCTION_P99: "p99",
    }

    if function_alias_map.get(aggregation.aggregate) is not None:
        return f"{aggregation.label}_upper_confidence_{function_alias_map.get(aggregation.aggregate)}"

    return None


def get_upper_confidence_column(aggregation: AttributeAggregation) -> Expression | None:
    """
    Returns the expression for calculating the upper confidence limit for a given aggregation
    """
    field = attribute_key_to_expression(aggregation.key)
    alias = get_attribute_confidence_interval_alias(aggregation)
    alias_dict = {"alias": alias} if alias else {}
    count_column_default = (
        f.sumIf(sampling_weight_column, f.mapContains(field.column, field.key))
        if isinstance(field, SubscriptableReference)
        else f.sum(sampling_weight_column)
    )
    count_column = (
        column(aggregation.label) if aggregation.label else count_column_default
    )
    # avg_column = (
    #     column(aggregation.label)
    #     if aggregation.label
    #     else f.weightedAvg(field, sampling_weight_column)
    # )
    # sum_column = (
    #     column(aggregation.label)
    #     if aggregation.label
    #     else f.sum(f.multiply(field, sampling_weight_column))
    # )

    function_map_upper_confidence_limit = {
        Function.FUNCTION_COUNT: f.plus(
            field,
            f.multiply(z_value, f.sqrt(f.minus(count_column, f.count(field)))),
            **alias_dict,
        ),
        # Splitting up confidence interval implementation so commenting for now
        # Function.FUNCTION_AVERAGE: f.plus(
        #     avg_column,
        #     f.multiply(
        #         z_value,
        #         f.divide(
        #             f.sqrt(
        #                 f.divide(
        #                     f.sum(
        #                         f.multiply(
        #                             f.minus(field, avg_column),
        #                             f.minus(field, avg_column),
        #                         )
        #                     ),
        #                     f.minus(count_column, literal(1)),
        #                 )
        #             ),
        #             f.sqrt(
        #                 f.divide(
        #                     f.multiply(
        #                         f.count(field), f.minus(count_column, literal(1))
        #                     ),
        #                     count_column,
        #                 )
        #             ),
        #         ),
        #     ),
        #     alias=confidence_column_alias,
        # ),
        # Function.FUNCTION_SUM: f.plus(
        #     f.sum(sum_column),
        #     f.multiply(
        #         z_value,
        #         f.sqrt(
        #             f.sum(
        #                 f.multiply(
        #                     f.multiply(field, field),
        #                     f.minus(sampling_weight_column, literal(1)),
        #                 )
        #             )
        #         ),
        #     ),
        #     alias=confidence_column_alias,
        # ),
    }

    return function_map_upper_confidence_limit.get(aggregation.aggregate)


# Commenting for now
# def get_percentile_extrapolation_columns(
#     aggregation: AttributeAggregation,
# ) -> list[Expression]:
#     """
#     Percentiles require special handling as they require sorting the data by the attribute value
#     """
#     field = attribute_key_to_expression(aggregation.key)
#     percentile_confidence_lower_alias = (
#         f"{aggregation.label}_percentile_confidence_lower"
#         if aggregation.label
#         else None
#     )
#     percentile_confidence_upper_alias = (
#         f"{aggregation.label}_percentile_confidence_upper"
#         if aggregation.label
#         else None
#     )

#     def get_percentile_expressions(percentile: float) -> list[Expression]:
#         n = f.count(field)
#         p = literal(percentile)
#         lower = f.floor(
#             f.minus(
#                 f.multiply(n, p),
#                 f.multiply(
#                     z_value,
#                     f.sqrt(f.multiply(f.multiply(p, f.minus(literal(1), p)), n)),
#                 ),
#             ),
#             alias=percentile_confidence_lower_alias,
#         )
#         upper = f.ceil(
#             f.plus(
#                 f.plus(
#                     f.multiply(n, p),
#                     f.multiply(
#                         z_value,
#                         f.sqrt(f.multiply(f.multiply(p, f.minus(literal(1), p)), n)),
#                     ),
#                 ),
#                 literal(1),
#             ),
#             alias=percentile_confidence_upper_alias,
#         )

#         return [lower, upper]

#     function_map_percentile_extrapolation = {
#         Function.FUNCTION_P50: get_percentile_expressions(0.5),
#         Function.FUNCTION_P90: get_percentile_expressions(0.9),
#         Function.FUNCTION_P95: get_percentile_expressions(0.95),
#         Function.FUNCTION_P99: get_percentile_expressions(0.99),
#     }

#     confidence_interval_expressions = function_map_percentile_extrapolation.get(
#         aggregation.aggregate
#     )
#     if confidence_interval_expressions is not None:
#         return confidence_interval_expressions

#     return []


def get_average_sample_rate_column(aggregation: AttributeAggregation) -> Expression:
    field = attribute_key_to_expression(aggregation.key)
    if isinstance(field, SubscriptableReference):
        return f.avgIf(
            f.divide(literal(1), sampling_weight_column),
            f.mapContains(field.column, field.key),
            alias=f"{aggregation.label}_average_sample_rate",
        )

    return f.avg(
        f.divide(literal(1), sampling_weight_column),
        alias=f"{aggregation.label}_average_sample_rate",
    )


def calculate_reliability(
    estimate: float,
    upper_confidence_limit: float,
    sample_count: int,
    confidence_interval_multiplier: float = 1.5,
    sample_count_threshold: int = 100,
) -> bool:
    """
    A reliability check to determine if the sample count is large enough to be reliable and the confidence interval is small enough.
    """
    return (
        upper_confidence_limit > estimate * confidence_interval_multiplier
        and sample_count >= sample_count_threshold
    )


def get_total_sample_count_column(aggregation: AttributeAggregation) -> Expression:
    field = attribute_key_to_expression(aggregation.key)
    count_column_default = (
        f.countIf(
            sampling_weight_column,
            f.mapContains(field.column, field.key),
            alias=f"{aggregation.label}_total_sample_count",
        )
        if isinstance(field, SubscriptableReference)
        else f.sum(sampling_weight_column)
    )
    return column(aggregation.label) if aggregation.label else count_column_default
