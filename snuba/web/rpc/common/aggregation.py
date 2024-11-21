from dataclasses import dataclass
from typing import Any, Dict, Optional

from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    ExtrapolationMode,
    Function,
    Reliability,
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

# Z value for 95% confidence interval is 1.96 which comes from the normal distribution z score.
z_value = 1.96

CUSTOM_COLUMN_PREFIX = "__snuba_custom_column__"


@dataclass(frozen=True)
class ExtrapolationMeta:
    reliability: Reliability.ValueType
    avg_sampling_rate: float


@dataclass(frozen=True)
class CustomColumnInformation:
    """
    In order to support extrapolation, we need to be able to create a new column in clickhouse that computes some value, potentially based on some existing column.
    This class holds the information needed to generate alias for the column so we can know what the column represents when getting results.
    """

    # A string identifier for the custom column that can be used to determine what the column represents
    custom_column_id: str

    # A column that this custom column depends on or attached to.
    # For example, if we are computing the confidence interval for an aggregation column, we need to know for which column we are computing a confidence interval.
    referenced_column: Optional[str]

    # Metadata about the custom column that can be used to encode additional information in the column.
    # E.g. the aggregation function type for the confidence interval column.
    metadata: dict[str, str]

    def to_alias(self) -> str:
        alias = CUSTOM_COLUMN_PREFIX + self.custom_column_id
        if self.referenced_column is not None:
            alias += f"${self.referenced_column}"
        if self.metadata:
            alias += "$" + ",".join(
                [f"{key}:{value}" for key, value in self.metadata.items()]
            )
        return alias

    @staticmethod
    def from_alias(alias: str) -> "CustomColumnInformation":
        if not alias.startswith(CUSTOM_COLUMN_PREFIX):
            raise ValueError(
                f"Alias {alias} does not start with {CUSTOM_COLUMN_PREFIX}"
            )

        alias = alias[len(CUSTOM_COLUMN_PREFIX) :]
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

    function_type = function_alias_map.get(aggregation.aggregate)
    if function_type is not None:
        return CustomColumnInformation(
            custom_column_id="upper_confidence",
            referenced_column=aggregation.label,
            metadata={
                "function_type": function_type,
            },
        ).to_alias()

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
    alias = CustomColumnInformation(
        custom_column_id="average_sample_rate",
        referenced_column=aggregation.label,
        metadata={},
    ).to_alias()
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
    Returns the expression for calculating the upper confidence limit for a given aggregation. If the aggregation cannot be extrapolated, returns None.
    Calculations are based on https://github.com/getsentry/extrapolation-math/blob/main/2024-10-04%20Confidence%20-%20Final%20Approach.ipynb
    Note that in the above notebook, the formulas are based on the sampling rate, while we perform calculations based on the sampling weight (1 / sampling rate).
    """
    alias = get_attribute_confidence_interval_alias(aggregation)
    alias_dict = {"alias": alias} if alias else {}

    function_map_upper_confidence_limit = {
        # upper confidence = Z cdot sqrt{-log{(frac{sum_{i=1}^n frac{1}{w_i}}{n})} cdot sigma^2(sum_{i=1}^n w_i^2 - w_i)}
        # where w_i is the sampling weight for the i-th event and n is the number of events.
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
    alias = CustomColumnInformation(
        custom_column_id="count",
        referenced_column=aggregation.label,
        metadata={},
    ).to_alias()
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


def get_extrapolation_meta(
    row_data: Dict[str, Any], column_label: str
) -> ExtrapolationMeta:
    """
    Computes the reliability and average sample rate for a column based on the extrapolation columns.
    """
    upper_confidence_limit = None
    average_sample_rate = 0
    sample_count = None
    for col_name, col_value in row_data.items():
        # we ignore non-custom columns
        if col_name.startswith(CUSTOM_COLUMN_PREFIX):
            custom_column_information = CustomColumnInformation.from_alias(col_name)
            if (
                custom_column_information.referenced_column is None
                or custom_column_information.referenced_column != column_label
            ):
                continue

            if custom_column_information.custom_column_id == "upper_confidence":
                upper_confidence_limit = col_value
            elif custom_column_information.custom_column_id == "average_sample_rate":
                average_sample_rate = col_value
            elif custom_column_information.custom_column_id == "count":
                sample_count = col_value

    reliability = Reliability.RELIABILITY_UNSPECIFIED
    if upper_confidence_limit is not None and sample_count is not None:
        is_reliable = calculate_reliability(
            row_data[column_label],
            upper_confidence_limit,
            sample_count,
        )
        reliability = (
            Reliability.RELIABILITY_HIGH if is_reliable else Reliability.RELIABILITY_LOW
        )

    return ExtrapolationMeta(reliability, average_sample_rate)
