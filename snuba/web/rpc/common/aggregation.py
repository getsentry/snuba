from dataclasses import dataclass
from typing import Optional

from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    ExtrapolationMode,
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
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException

sampling_weight_column = column("sampling_weight")

# Z value for 95% confidence interval is 1.96
z_value = 1.96


@dataclass(frozen=True)
class CustomColumnInformation:
    column_type: str
    referenced_column: Optional[str]
    metadata: dict[str, str]


def _generate_custom_column_alias(
    custom_column_information: CustomColumnInformation,
) -> str:
    alias = custom_column_information.column_type
    if custom_column_information.referenced_column is not None:
        alias += f"${custom_column_information.referenced_column}"
    if custom_column_information.metadata:
        alias += "$" + ",".join(
            [
                f"{key}:{value}"
                for key, value in custom_column_information.metadata.items()
            ]
        )
    return alias


def get_custom_column_information(alias: str) -> CustomColumnInformation:
    parts = alias.split("$")
    column_type = parts[0]
    referenced_column = parts[1] if len(parts) > 1 else None
    metadata_parts = parts[2].split(",") if len(parts) > 2 else []
    metadata = {}
    for metadata_part in metadata_parts:
        key, value = metadata_part.split(":")
        metadata[key] = value

    return CustomColumnInformation(column_type, referenced_column, metadata)


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
        return _generate_custom_column_alias(
            CustomColumnInformation(
                column_type="upper_confidence",
                referenced_column=aggregation.label,
                metadata={
                    "function_type": function_alias_map.get(aggregation.aggregate)
                },
            )
        )

    return None


def get_field_existence_expression(aggregation: AttributeAggregation) -> Expression:
    def get_subscriptable_field(field: Expression) -> SubscriptableReference | None:
        """
        Check if the field is a subscriptable reference or a function call with a subscriptable reference as the first parameter to handle the case
        where the field is casting a subscriptable reference (e.g. for integers). If so, return the subscriptable reference.
        """
        if isinstance(field, SubscriptableReference):
            return field
        if isinstance(field, FunctionCall) and len(field.parameters) > 0:
            if len(field.parameters) > 0 and isinstance(
                field.parameters[0], SubscriptableReference
            ):
                return field.parameters[0]

        return None

    field = attribute_key_to_expression(aggregation.key)
    subscriptable_field = get_subscriptable_field(field)
    if subscriptable_field is not None:
        return f.mapContains(subscriptable_field.column, subscriptable_field.key)

    return f.isNotNull(field)


def get_average_sample_rate_column(aggregation: AttributeAggregation) -> Expression:
    alias = _generate_custom_column_alias(
        CustomColumnInformation(
            column_type="average_sample_rate",
            referenced_column=aggregation.label,
            metadata={},
        )
    )
    return f.avgIf(
        f.divide(literal(1), sampling_weight_column),
        get_field_existence_expression(aggregation),
        alias=alias,
    )


def get_extrapolated_function(
    aggregation: AttributeAggregation,
) -> CurriedFunctionCall | FunctionCall | None:
    sampling_weight_column = column("sampling_weight")
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
        Function.FUNCTION_COUNT: f.sumIf(
            sampling_weight_column,
            get_field_existence_expression(aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_P50: cf.quantileTDigestWeighted(0.5)(
            field, sampling_weight_column, **alias_dict
        ),
        Function.FUNCTION_P75: cf.quantileTDigestWeighted(0.75)(
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


def get_upper_confidence_column(aggregation: AttributeAggregation) -> Expression | None:
    """
    Returns the expression for calculating the upper confidence limit for a given aggregation
    """
    alias = get_attribute_confidence_interval_alias(aggregation)
    alias_dict = {"alias": alias} if alias else {}

    function_map_upper_confidence_limit = {
        Function.FUNCTION_COUNT: f.multiply(
            z_value,
            f.sqrt(
                f.multiply(
                    f.negate(f.log(get_average_sample_rate_column(aggregation))),
                    f.sumIf(
                        f.minus(
                            f.multiply(sampling_weight_column, sampling_weight_column),
                            sampling_weight_column,
                        ),
                        get_field_existence_expression(aggregation),
                    ),
                )
            ),
            **alias_dict,
        ),
    }

    return function_map_upper_confidence_limit.get(aggregation.aggregate)


def get_count_column(aggregation: AttributeAggregation) -> Expression:
    field = attribute_key_to_expression(aggregation.key)
    alias = _generate_custom_column_alias(
        CustomColumnInformation(
            column_type="count", referenced_column=aggregation.label, metadata={}
        )
    )
    return f.count(field, alias=alias)


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
        estimate + upper_confidence_limit <= estimate * confidence_interval_multiplier
        and sample_count >= sample_count_threshold
    )


def aggregation_to_expression(aggregation: AttributeAggregation) -> Expression:
    field = attribute_key_to_expression(aggregation.key)
    alias = aggregation.label if aggregation.label else None
    alias_dict = {"alias": alias} if alias else {}
    function_map: dict[Function.ValueType, CurriedFunctionCall | FunctionCall] = {
        Function.FUNCTION_SUM: f.sum(field, **alias_dict),
        Function.FUNCTION_AVERAGE: f.avg(field, **alias_dict),
        Function.FUNCTION_COUNT: f.count(field, **alias_dict),
        Function.FUNCTION_P50: cf.quantile(0.5)(field, **alias_dict),
        Function.FUNCTION_P75: cf.quantile(0.75)(field, **alias_dict),
        Function.FUNCTION_P90: cf.quantile(0.9)(field, **alias_dict),
        Function.FUNCTION_P95: cf.quantile(0.95)(field, **alias_dict),
        Function.FUNCTION_P99: cf.quantile(0.99)(field, **alias_dict),
        Function.FUNCTION_AVG: f.avg(field, **alias_dict),
        Function.FUNCTION_MAX: f.max(field, **alias_dict),
        Function.FUNCTION_MIN: f.min(field, **alias_dict),
        Function.FUNCTION_UNIQ: f.uniq(field, **alias_dict),
    }

    if (
        aggregation.extrapolation_mode
        == ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED
    ):
        agg_func_expr = get_extrapolated_function(aggregation)
    else:
        agg_func_expr = function_map.get(aggregation.aggregate)

    if agg_func_expr is None:
        raise BadSnubaRPCRequestException(
            f"Aggregation not specified for {aggregation.key.name}"
        )

    return agg_func_expr
