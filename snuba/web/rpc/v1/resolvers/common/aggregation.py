from __future__ import annotations

import math
from abc import ABC, abstractmethod
from bisect import bisect_left
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Callable, Dict, List, Optional

from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    AttributeKeyExpression,
    ExtrapolationMode,
    Function,
    Reliability,
)

from snuba.query.dsl import CurriedFunctions as cf
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, column, literal
from snuba.query.expressions import (
    CurriedFunctionCall,
    Expression,
    FunctionCall,
)
from snuba.web.rpc.common.common import (
    get_field_existence_expression,
    trace_item_filters_to_expression,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException

sampling_factor_column = column("sampling_factor")
client_sample_rate_column = column("client_sample_rate")
server_sample_rate_column = column("server_sample_rate")


Z_VALUE_P95 = 1.96  # Z value for 95% confidence interval is 1.96 which comes from the normal distribution z score.
Z_VALUE_P975 = 2.24  # Z value for 97.5% confidence interval used for the avg() CI

PERCENTILE_PRECISION = 100000
PERCENTILE_SAMPLE_COUNT_THRESHOLD = 50

# We multiply the sampling weight by this factor to reduce rounding error.
# The idea is that for quantiles, scaling the weights has no effect on the result as the distribution is preserved.
PERCENTILE_CORRECTION_FACTOR = 1000

CONFIDENCE_INTERVAL_THRESHOLD = 0.5

CUSTOM_COLUMN_PREFIX = "__snuba_custom_column__"

_FLOATING_POINT_PRECISION = 9

# Attribute names for sample rates
CLIENT_SAMPLE_RATE_ATTRIBUTE = "client_sample_rate"
SERVER_SAMPLE_RATE_ATTRIBUTE = "server_sample_rate"


def _get_condition_in_aggregation(
    aggregation: AttributeAggregation | AttributeConditionalAggregation,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
) -> Expression:
    condition_in_aggregation: Expression = literal(True)
    if isinstance(aggregation, AttributeConditionalAggregation):
        condition_in_aggregation = trace_item_filters_to_expression(
            aggregation.filter, attribute_key_to_expression
        )
    return condition_in_aggregation


_ATTR_KEY_EXPR_OP_TO_FUNC = {
    AttributeKeyExpression.OP_ADD: f.plus,
    AttributeKeyExpression.OP_SUB: f.minus,
    AttributeKeyExpression.OP_MULT: f.multiply,
    AttributeKeyExpression.OP_DIV: f.divide,
}


def _attribute_key_expression_to_expression(
    expr: AttributeKeyExpression,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
) -> tuple[Expression, list[AttributeKey]]:
    """Recursively converts an AttributeKeyExpression proto to a Snuba AST Expression,
    also collecting all leaf AttributeKey nodes encountered during traversal."""
    match expr.WhichOneof("expression"):
        case "key":
            return attribute_key_to_expression(expr.key), [expr.key]
        case "formula":
            func = _ATTR_KEY_EXPR_OP_TO_FUNC.get(expr.formula.op)
            if func is None:
                raise BadSnubaRPCRequestException(
                    f"Unknown AttributeKeyExpression op: {expr.formula.op}"
                )
            left_expr, left_keys = _attribute_key_expression_to_expression(
                expr.formula.left, attribute_key_to_expression
            )
            right_expr, right_keys = _attribute_key_expression_to_expression(
                expr.formula.right, attribute_key_to_expression
            )
            return func(left_expr, right_expr), left_keys + right_keys
        case _:
            raise BadSnubaRPCRequestException("AttributeKeyExpression must have key or formula set")


def _resolve_field_and_existence(
    aggregation: AttributeConditionalAggregation,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
) -> tuple[Expression, Expression]:
    """Given a protobuf aggregation, returns (aggregation expression, existence expression)
    Aggregation expression - the actual aggregation ex: sum(attr1 + attr2)
    Existence expression - an expression that is true iff all referenced attributes in the aggregation are present.
    """
    if aggregation.HasField("expression"):
        field, keys = _attribute_key_expression_to_expression(
            aggregation.expression, attribute_key_to_expression
        )
        existence_checks = [
            get_field_existence_expression(attribute_key_to_expression(k)) for k in keys
        ]

        existence: Expression = existence_checks[0]
        if len(existence_checks) >= 2:
            existence = and_cond(*existence_checks)
        elif len(existence_checks) == 1:
            existence = existence_checks[0]
        else:
            raise RuntimeError("expected existence_checks to never be empty, but it is")
        return field, existence
    else:
        field = attribute_key_to_expression(aggregation.key)
        return field, get_field_existence_expression(field)


@dataclass(frozen=True)
class ExtrapolationContext(ABC):
    value: float
    confidence_interval: Any
    average_sample_rate: float
    sample_count: int
    is_extrapolated: bool

    @property
    def is_data_present(self) -> bool:
        return self.sample_count > 0

    @property
    @abstractmethod
    def reliability(self) -> Reliability.ValueType:
        raise NotImplementedError

    @staticmethod
    def from_row(
        column_label: str,
        row_data: Dict[str, Any],
    ) -> ExtrapolationContext:
        value = row_data[column_label]
        is_extrapolated = False

        confidence_interval = None
        average_sample_rate = 0
        sample_count = 0

        percentile = 0.0
        granularity = 0.0
        width = 0.0

        is_percentile = False

        for col_name, col_value in row_data.items():
            # we ignore non-custom columns
            if not col_name.startswith(CUSTOM_COLUMN_PREFIX):
                continue

            custom_column_information = CustomColumnInformation.from_alias(col_name)
            if (
                custom_column_information.referenced_column is None
                or custom_column_information.referenced_column != column_label
            ):
                continue

            if custom_column_information.custom_column_id == "confidence_interval":
                is_extrapolated = True
                confidence_interval = col_value

                is_percentile = custom_column_information.metadata.get(
                    "function_type", ""
                ).startswith("p")
                if is_percentile:
                    percentile = (
                        float(custom_column_information.metadata["function_type"][1:]) / 100
                    )
                    granularity = float(custom_column_information.metadata.get("granularity", "0"))
                    width = float(custom_column_information.metadata.get("width", "0"))
            elif custom_column_information.custom_column_id == "average_sample_rate":
                average_sample_rate = col_value
            elif custom_column_information.custom_column_id == "count":
                sample_count = col_value

        if is_percentile:
            return PercentileExtrapolationContext(
                value=value,
                confidence_interval=confidence_interval,
                average_sample_rate=average_sample_rate,
                sample_count=sample_count,
                percentile=percentile,
                granularity=granularity,
                width=width,
                is_extrapolated=is_extrapolated,
            )

        return GenericExtrapolationContext(
            value=value,
            confidence_interval=confidence_interval,
            average_sample_rate=average_sample_rate,
            sample_count=sample_count,
            is_extrapolated=is_extrapolated,
        )


@dataclass(frozen=True)
class GenericExtrapolationContext(ExtrapolationContext):
    @cached_property
    def reliability(self) -> Reliability.ValueType:
        if not self.is_extrapolated or not self.is_data_present:
            return Reliability.RELIABILITY_UNSPECIFIED

        # We determine reliability relative to the returned value. If the
        # confidence interval (CI) is 0, we're not sampling and have high
        # reliability. Otherwise, if the value is 0, the CI is infinitely larger
        # than the value, so reliability is low.
        if self.confidence_interval == 0:
            return Reliability.RELIABILITY_HIGH
        elif self.value == 0:
            return Reliability.RELIABILITY_LOW

        if abs(self.confidence_interval / self.value) <= CONFIDENCE_INTERVAL_THRESHOLD:
            return Reliability.RELIABILITY_HIGH
        return Reliability.RELIABILITY_LOW


@dataclass(frozen=True)
class PercentileExtrapolationContext(ExtrapolationContext):
    percentile: float
    granularity: float
    width: float

    @cached_property
    def reliability(self) -> Reliability.ValueType:
        if not self.is_extrapolated or not self.is_data_present:
            return Reliability.RELIABILITY_UNSPECIFIED

        lower_bound, upper_bound = _calculate_approximate_ci_percentile_levels(
            self.sample_count, self.percentile
        )
        percentile_index_lower = _get_closest_percentile_index(
            lower_bound, self.percentile, self.granularity, self.width
        )
        percentile_index_upper = _get_closest_percentile_index(
            upper_bound, self.percentile, self.granularity, self.width
        )
        ci_lower = self.confidence_interval[percentile_index_lower]
        ci_upper = self.confidence_interval[percentile_index_upper]

        max_err = max(self.value - ci_lower, ci_upper - self.value)
        relative_confidence = abs(max_err / self.value) if self.value != 0 else float("inf")

        if self.sample_count <= PERCENTILE_SAMPLE_COUNT_THRESHOLD:
            return Reliability.RELIABILITY_LOW

        if relative_confidence <= CONFIDENCE_INTERVAL_THRESHOLD:
            return Reliability.RELIABILITY_HIGH
        return Reliability.RELIABILITY_LOW


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
            alias += "$" + ",".join([f"{key}:{value}" for key, value in self.metadata.items()])
        return alias

    @staticmethod
    def from_alias(alias: str) -> "CustomColumnInformation":
        if not alias.startswith(CUSTOM_COLUMN_PREFIX):
            raise ValueError(f"Alias {alias} does not start with {CUSTOM_COLUMN_PREFIX}")

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


def _get_sampling_weight_expression(
    use_sampling_factor: bool,
    extrapolation_mode: ExtrapolationMode.ValueType,
) -> Expression:
    if extrapolation_mode == ExtrapolationMode.EXTRAPOLATION_MODE_CLIENT_ONLY:
        # Use client sample rate attribute, convert to weight (1/rate)
        return f.divide(1, client_sample_rate_column)
    elif extrapolation_mode == ExtrapolationMode.EXTRAPOLATION_MODE_SERVER_ONLY:
        # Use server sample rate attribute, convert to weight (1/rate)
        return f.divide(1, server_sample_rate_column)
    else:
        # Default behavior for existing modes - always use sampling_factor now
        return f.divide(1, sampling_factor_column)


def get_attribute_confidence_interval_alias(
    aggregation: AttributeAggregation | AttributeConditionalAggregation,
    additional_metadata: dict[str, str] = {},
) -> str | None:
    function_alias_map = {
        Function.FUNCTION_COUNT: "count",
        Function.FUNCTION_AVG: "avg",
        Function.FUNCTION_SUM: "sum",
        Function.FUNCTION_P50: "p50",
        Function.FUNCTION_P75: "p75",
        Function.FUNCTION_P90: "p90",
        Function.FUNCTION_P95: "p95",
        Function.FUNCTION_P99: "p99",
    }

    function_type = function_alias_map.get(aggregation.aggregate)
    if function_type is not None:
        return CustomColumnInformation(
            custom_column_id="confidence_interval",
            referenced_column=aggregation.label,
            metadata={
                "function_type": function_type,
                **additional_metadata,
            },
        ).to_alias()

    return None


def get_average_sample_rate_column(
    aggregation: AttributeConditionalAggregation,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
    use_sampling_factor: bool = False,
) -> Expression:
    alias = CustomColumnInformation(
        custom_column_id="average_sample_rate",
        referenced_column=aggregation.label,
        metadata={},
    ).to_alias()
    sampling_weight = _get_sampling_weight_expression(
        use_sampling_factor,
        aggregation.extrapolation_mode,
    )
    field, field_exists = _resolve_field_and_existence(aggregation, attribute_key_to_expression)
    condition_in_aggregation = _get_condition_in_aggregation(
        aggregation, attribute_key_to_expression
    )
    return f.divide(
        f.countIf(
            field,
            and_cond(field_exists, condition_in_aggregation),
        ),
        f.sumIf(
            sampling_weight,
            and_cond(field_exists, condition_in_aggregation),
        ),
        alias=alias,
    )


def _get_count_column_alias(
    aggregation: AttributeConditionalAggregation,
) -> str:
    return CustomColumnInformation(
        custom_column_id="count",
        referenced_column=aggregation.label,
        metadata={},
    ).to_alias()


def get_count_column(
    aggregation: AttributeConditionalAggregation,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
) -> Expression:
    field, field_exists = _resolve_field_and_existence(aggregation, attribute_key_to_expression)
    return f.countIf(
        field,
        and_cond(
            field_exists,
            _get_condition_in_aggregation(aggregation, attribute_key_to_expression),
        ),
        alias=_get_count_column_alias(aggregation),
    )


def _get_possible_percentiles(percentile: float, granularity: float, width: float) -> List[float]:
    """
    Returns a list of possible percentiles to use for the confidence interval calculation from the range percentile - width to percentile + width,
    with a granularity of granularity.
    """
    # we multiply by PERCENTILE_PRECISION to get a precision of 5 decimal places
    return [
        x / PERCENTILE_PRECISION
        for x in range(
            int(max(granularity, percentile - width) * PERCENTILE_PRECISION),
            int(min(1, percentile + width) * PERCENTILE_PRECISION),
            int(granularity * PERCENTILE_PRECISION),
        )
    ]


def _get_possible_percentiles_expression(
    aggregation: AttributeConditionalAggregation,
    percentile: float,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
    use_sampling_factor: bool = False,
    granularity: float = 0.005,
    width: float = 0.1,
) -> Expression:
    """
    In order to approximate the confidence intervals, we calculate a bunch of quantiles around the desired percentile, using the given granularity and width.
    We then use this to approximate the bounds of the confidence interval. Increasing granularity will increase the percision of the confidence interval, but will also increase the number of quantiles we need to calculate.

    In the worst case, with the minimum sample size required to be reliable (100), for the lowest percentile (0.5), we need to calculate values in the range [0.45, 0.56], so we set the width to 0.1 to be safe.

    The granularity was arbitrarily decided. This constants aren't very important as all of these calculations are very rough and approximate.
    The width isn't important as long as it contains the range of values that we actually care about, I just added it as a performance optimization.
    The granularity might need to be adjusted, but I think this is a good starting value.
    """
    field, _field_exists = _resolve_field_and_existence(aggregation, attribute_key_to_expression)
    possible_percentiles = _get_possible_percentiles(percentile, granularity, width)
    alias = get_attribute_confidence_interval_alias(
        aggregation, {"granularity": str(granularity), "width": str(width)}
    )
    alias_dict = {"alias": alias} if alias else {}
    sampling_weight = _get_sampling_weight_expression(
        use_sampling_factor,
        aggregation.extrapolation_mode,
    )
    return cf.quantilesTDigestWeighted(*possible_percentiles)(
        field,
        f.cast(f.round(f.multiply(sampling_weight, PERCENTILE_CORRECTION_FACTOR)), "UInt64"),
        **alias_dict,
    )


def get_extrapolated_function(
    aggregation: AttributeConditionalAggregation,
    field: Expression,
    field_exists: Expression,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
    use_sampling_factor: bool = False,
) -> CurriedFunctionCall | FunctionCall | None:
    alias = aggregation.label if aggregation.label else None
    alias_dict = {"alias": alias} if alias else {}
    condition_in_aggregation = _get_condition_in_aggregation(
        aggregation, attribute_key_to_expression
    )

    sampling_weight = _get_sampling_weight_expression(
        use_sampling_factor,
        aggregation.extrapolation_mode,
    )
    function_map_sample_weighted: dict[Function.ValueType, CurriedFunctionCall | FunctionCall] = {
        Function.FUNCTION_SUM: f.sumIfOrNull(
            f.multiply(field, sampling_weight),
            and_cond(field_exists, condition_in_aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_AVERAGE: f.divide(
            f.sumIfOrNull(
                f.multiply(field, sampling_weight),
                and_cond(field_exists, condition_in_aggregation),
            ),
            f.sumIfOrNull(
                sampling_weight,
                and_cond(field_exists, condition_in_aggregation),
            ),
            **alias_dict,
        ),
        Function.FUNCTION_AVG: f.divide(
            f.sumIfOrNull(
                f.multiply(field, sampling_weight),
                and_cond(field_exists, condition_in_aggregation),
            ),
            f.sumIfOrNull(
                sampling_weight,
                and_cond(field_exists, condition_in_aggregation),
            ),
            **alias_dict,
        ),
        Function.FUNCTION_COUNT: f.round(
            f.sumIfOrNull(
                sampling_weight,
                and_cond(field_exists, condition_in_aggregation),
            ),
            **alias_dict,
        ),
        Function.FUNCTION_P50: cf.quantileTDigestWeightedIfOrNull(0.5)(
            field,
            f.cast(
                f.round(f.multiply(sampling_weight, PERCENTILE_CORRECTION_FACTOR)),
                "UInt64",
            ),
            and_cond(field_exists, condition_in_aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_P75: cf.quantileTDigestWeightedIfOrNull(0.75)(
            field,
            f.cast(
                f.round(f.multiply(sampling_weight, PERCENTILE_CORRECTION_FACTOR)),
                "UInt64",
            ),
            and_cond(field_exists, condition_in_aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_P90: cf.quantileTDigestWeightedIfOrNull(0.9)(
            field,
            f.cast(
                f.round(f.multiply(sampling_weight, PERCENTILE_CORRECTION_FACTOR)),
                "UInt64",
            ),
            and_cond(field_exists, condition_in_aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_P95: cf.quantileTDigestWeightedIfOrNull(0.95)(
            field,
            f.cast(
                f.round(f.multiply(sampling_weight, PERCENTILE_CORRECTION_FACTOR)),
                "UInt64",
            ),
            and_cond(field_exists, condition_in_aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_P99: cf.quantileTDigestWeightedIfOrNull(0.99)(
            field,
            f.cast(
                f.round(f.multiply(sampling_weight, PERCENTILE_CORRECTION_FACTOR)),
                "UInt64",
            ),
            and_cond(field_exists, condition_in_aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_MAX: f.maxIfOrNull(
            field,
            and_cond(field_exists, condition_in_aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_MIN: f.minIfOrNull(
            field,
            and_cond(field_exists, condition_in_aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_UNIQ: f.uniqIfOrNull(
            field,
            and_cond(field_exists, condition_in_aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_ANY: f.anyIfOrNull(
            field,
            and_cond(field_exists, condition_in_aggregation),
            **alias_dict,
        ),
    }

    return function_map_sample_weighted.get(aggregation.aggregate)


def _get_ci_count(
    aggregation: AttributeConditionalAggregation,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
    alias: str | None = None,
    z_value: float = Z_VALUE_P95,
    use_sampling_factor: bool = False,
) -> Expression:
    r"""
    confidence interval = Z \cdot \sqrt{\sum_{i=1}^n w_i^2 - w_i}

          ┌───────────┐
          │ ₙ
       ╲  │ ⎲   2
    Z ⋅ ╲ │ ⎳  wᵢ - wᵢ
         ╲│ⁱ⁼¹

    where w_i is the sampling weight for the i-th event and n is the number of
    events. This is based on the Horvitz-Thompson estimator for totals of
    weighted samples:
     1. Since we're counting, the value of each event is 1, which removes the
        value from the formula.
     2. The sampling weight is the inverse of the inclusion probability
        (sampling rate).
     3. Samples undergo independent Bernoulli trials, which means the term for
        dependent inclusion probabilities from the HT formula zero out.
    """

    field, field_exists = _resolve_field_and_existence(aggregation, attribute_key_to_expression)
    condition_in_aggregation = _get_condition_in_aggregation(
        aggregation, attribute_key_to_expression
    )
    alias_dict = {"alias": alias} if alias else {}
    sampling_weight = _get_sampling_weight_expression(
        use_sampling_factor,
        aggregation.extrapolation_mode,
    )
    variance = f.sumIf(
        f.minus(
            f.multiply(sampling_weight, sampling_weight),
            sampling_weight,
        ),
        and_cond(
            field_exists,
            condition_in_aggregation,
        ),
    )

    return f.multiply(z_value, f.sqrt(variance), **alias_dict)


def _get_ci_sum(
    aggregation: AttributeConditionalAggregation,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
    alias: str | None = None,
    z_value: float = Z_VALUE_P95,
    use_sampling_factor: bool = False,
) -> Expression:
    r"""
    confidence interval = Z \cdot \sqrt{\sum_{i=1}^n x_i^2 \cdot (w_i^2 - w_i)}

          ┌─────────────────┐
          │ ₙ
       ╲  │ ⎲   2    2
    Z ⋅ ╲ │ ⎳  xᵢ ⋅(wᵢ - wᵢ)
         ╲│ⁱ⁼¹

    Just like for counts, we use the Horvitz-Thompson estimator for totals of
    weighted samples. In this case, the value of each event contributes to the
    variance of the total.
    """

    field, field_exists = _resolve_field_and_existence(aggregation, attribute_key_to_expression)
    condition_in_aggregation = _get_condition_in_aggregation(
        aggregation, attribute_key_to_expression
    )
    alias_dict = {"alias": alias} if alias else {}
    sampling_weight = _get_sampling_weight_expression(
        use_sampling_factor,
        aggregation.extrapolation_mode,
    )
    variance = f.sumIf(
        f.multiply(
            f.multiply(field, field),
            f.minus(
                f.multiply(sampling_weight, sampling_weight),
                sampling_weight,
            ),
        ),
        and_cond(
            field_exists,
            condition_in_aggregation,
        ),
    )

    return f.multiply(z_value, f.sqrt(variance), **alias_dict)


def _get_ci_avg(
    aggregation: AttributeConditionalAggregation,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
    alias: str | None = None,
    use_sampling_factor: bool = False,
) -> Expression:
    """
    confidence interval = (\\frac{t + err_t}{c - err_c} - \\frac{t - err_t}{c + err_c}) \\cdot 0.5

     t + err_t   t - err_t
    (───────── - ─────────) ⋅ 0.5
     c - err_c   c + err_c

    where t is the estimated sum, c is the estimated count, and err_* are the
    confidence intervals for the sum and count respectively.

    Since the average is `total / count`, we can combine the two individual
    confidence intervals into bounds for the average using the Bonferroni
    Method:
     - Upper bound: `total_upper / count_lower`
     - Lower bound: `total_lower / count_upper`

    However, since the intervals are combined, they each have to be half as
    wide, thus 97.5%, to combine into a 95% confidence interval.

    Finally, we take half of the range between the upper and lower bounds as the
    average between the upper and lower error.
    """

    field, field_exists = _resolve_field_and_existence(aggregation, attribute_key_to_expression)
    condition_in_aggregation = _get_condition_in_aggregation(
        aggregation, attribute_key_to_expression
    )
    alias_dict = {"alias": alias} if alias else {}
    sampling_weight = _get_sampling_weight_expression(
        use_sampling_factor,
        aggregation.extrapolation_mode,
    )

    expr_sum = f.sumIfOrNull(
        f.multiply(field, sampling_weight),
        and_cond(field_exists, condition_in_aggregation),
        alias=f"{alias}__sum",
    )
    expr_count = f.sumIfOrNull(
        sampling_weight,
        and_cond(field_exists, condition_in_aggregation),
        alias=f"{alias}__count",
    )

    expr_sum_err = _get_ci_sum(
        aggregation,
        attribute_key_to_expression,
        f"{alias}__sum_err",
        Z_VALUE_P975,
        use_sampling_factor,
    )
    expr_count_err = _get_ci_count(
        aggregation,
        attribute_key_to_expression,
        f"{alias}__count_err",
        Z_VALUE_P975,
        use_sampling_factor,
    )

    return f.divide(
        f.abs(
            f.minus(
                f.divide(
                    f.plus(expr_sum, expr_sum_err),
                    f.minus(expr_count, expr_count_err),
                ),
                f.divide(
                    f.minus(column(f"{alias}__sum"), column(f"{alias}__sum_err")),
                    f.plus(column(f"{alias}__count"), column(f"{alias}__count_err")),
                ),
            ),
        ),
        2,
        **alias_dict,
    )


def get_confidence_interval_column(
    aggregation: AttributeConditionalAggregation,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
    use_sampling_factor: bool = False,
) -> Expression | None:
    """
    Returns the expression for calculating the upper confidence limit for a given aggregation. If the aggregation cannot be extrapolated, returns None.
    Calculations are based on https://github.com/getsentry/extrapolation-math/blob/main/2024-10-04%20Confidence%20-%20Final%20Approach.ipynb
    Note that in the above notebook, the formulas are based on the sampling rate, while we perform calculations based on the sampling weight (1 / sampling rate).
    """
    alias = get_attribute_confidence_interval_alias(aggregation)

    function_map_confidence_interval = {
        Function.FUNCTION_COUNT: _get_ci_count(
            aggregation,
            attribute_key_to_expression,
            alias,
            use_sampling_factor=use_sampling_factor,
        ),
        Function.FUNCTION_SUM: _get_ci_sum(
            aggregation,
            attribute_key_to_expression,
            alias,
            use_sampling_factor=use_sampling_factor,
        ),
        Function.FUNCTION_AVG: _get_ci_avg(
            aggregation, attribute_key_to_expression, alias, use_sampling_factor
        ),
        Function.FUNCTION_P50: _get_possible_percentiles_expression(
            aggregation, 0.5, attribute_key_to_expression, use_sampling_factor
        ),
        Function.FUNCTION_P75: _get_possible_percentiles_expression(
            aggregation, 0.75, attribute_key_to_expression, use_sampling_factor
        ),
        Function.FUNCTION_P90: _get_possible_percentiles_expression(
            aggregation, 0.9, attribute_key_to_expression, use_sampling_factor
        ),
        Function.FUNCTION_P95: _get_possible_percentiles_expression(
            aggregation, 0.95, attribute_key_to_expression, use_sampling_factor
        ),
        Function.FUNCTION_P99: _get_possible_percentiles_expression(
            aggregation, 0.99, attribute_key_to_expression, use_sampling_factor
        ),
    }

    return function_map_confidence_interval.get(aggregation.aggregate)


def _get_closest_percentile_index(
    value: float, percentile: float, granularity: float, width: float
) -> int:
    possible_percentiles = _get_possible_percentiles(percentile, granularity, width)
    index = bisect_left(possible_percentiles, value)
    if index == 0:
        return 0
    if index == len(possible_percentiles):
        return len(possible_percentiles) - 1

    if possible_percentiles[index] - value < value - possible_percentiles[index - 1]:
        return index
    return index - 1


def _calculate_approximate_ci_percentile_levels(
    sample_count: int, percentile: float
) -> tuple[float, float]:
    # We calculate the approximate percentile levels we want to use for the confidence interval bounds
    n = sample_count
    p = percentile
    lower_index = n * p - Z_VALUE_P95 * math.sqrt(n * p * (1 - p))
    upper_index = 1 + n * p + Z_VALUE_P95 * math.sqrt(n * p * (1 - p))
    return (lower_index / n, upper_index / n)


def _array_aggregation_to_expression(
    aggregation: AttributeAggregation | AttributeConditionalAggregation,
    field: Expression,
    condition_in_aggregation: Expression,
    alias_dict: dict[str, str],
) -> Expression:
    if aggregation.aggregate == Function.FUNCTION_UNIQ:
        return f.round(
            f.uniqArrayIfOrNull(
                field,
                and_cond(get_field_existence_expression(field), condition_in_aggregation),
            ),
            _FLOATING_POINT_PRECISION,
            **alias_dict,
        )
    raise BadSnubaRPCRequestException(
        f"Aggregation {Function.Name(aggregation.aggregate)} "
        f"not supported for array attribute {aggregation.key.name}"
    )


def aggregation_to_expression(
    aggregation: AttributeConditionalAggregation,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
    use_sampling_factor: bool = False,
) -> Expression:
    field, field_exists = _resolve_field_and_existence(aggregation, attribute_key_to_expression)
    alias = aggregation.label if aggregation.label else None
    alias_dict = {"alias": alias} if alias else {}
    condition_in_aggregation = _get_condition_in_aggregation(
        aggregation, attribute_key_to_expression
    )

    if aggregation.key.type == AttributeKey.Type.TYPE_ARRAY:
        return _array_aggregation_to_expression(
            aggregation, field, condition_in_aggregation, alias_dict
        )

    function_map: dict[Function.ValueType, CurriedFunctionCall | FunctionCall] = {
        Function.FUNCTION_SUM: f.sumIfOrNull(
            field,
            and_cond(field_exists, condition_in_aggregation),
        ),
        Function.FUNCTION_AVERAGE: f.avgIfOrNull(
            field,
            and_cond(field_exists, condition_in_aggregation),
        ),
        Function.FUNCTION_COUNT: f.countIfOrNull(
            field,
            and_cond(field_exists, condition_in_aggregation),
        ),
        Function.FUNCTION_P50: cf.quantileIfOrNull(0.5)(
            field,
            and_cond(field_exists, condition_in_aggregation),
        ),
        Function.FUNCTION_P75: cf.quantileIfOrNull(0.75)(
            field,
            and_cond(field_exists, condition_in_aggregation),
        ),
        Function.FUNCTION_P90: cf.quantileIfOrNull(0.9)(
            field,
            and_cond(field_exists, condition_in_aggregation),
        ),
        Function.FUNCTION_P95: cf.quantileIfOrNull(0.95)(
            field,
            and_cond(field_exists, condition_in_aggregation),
        ),
        Function.FUNCTION_P99: cf.quantileIfOrNull(0.99)(
            field,
            and_cond(field_exists, condition_in_aggregation),
        ),
        Function.FUNCTION_AVG: f.avgIfOrNull(
            field,
            and_cond(field_exists, condition_in_aggregation),
        ),
        Function.FUNCTION_MAX: f.maxIfOrNull(
            field,
            and_cond(field_exists, condition_in_aggregation),
        ),
        Function.FUNCTION_MIN: f.minIfOrNull(
            field,
            and_cond(field_exists, condition_in_aggregation),
        ),
        Function.FUNCTION_UNIQ: f.uniqIfOrNull(
            field,
            and_cond(field_exists, condition_in_aggregation),
        ),
        Function.FUNCTION_ANY: f.anyIfOrNull(
            field,
            and_cond(field_exists, condition_in_aggregation),
        ),
    }

    if aggregation.extrapolation_mode in [
        ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
        ExtrapolationMode.EXTRAPOLATION_MODE_CLIENT_ONLY,
        ExtrapolationMode.EXTRAPOLATION_MODE_SERVER_ONLY,
    ]:
        agg_func_expr = get_extrapolated_function(
            aggregation, field, field_exists, attribute_key_to_expression, use_sampling_factor
        )
    else:
        agg_func_expr = function_map.get(aggregation.aggregate)
        if agg_func_expr is not None:
            # Don't apply round() to FUNCTION_ANY since it can return non-numeric types (e.g., strings)
            if aggregation.aggregate == Function.FUNCTION_ANY:
                agg_func_expr = f.anyIfOrNull(
                    field,
                    and_cond(field_exists, condition_in_aggregation),
                    **alias_dict,
                )
            else:
                agg_func_expr = f.round(agg_func_expr, _FLOATING_POINT_PRECISION, **alias_dict)

    if agg_func_expr is None:
        raise BadSnubaRPCRequestException(f"Aggregation not specified for {aggregation.key.name}")

    return agg_func_expr
