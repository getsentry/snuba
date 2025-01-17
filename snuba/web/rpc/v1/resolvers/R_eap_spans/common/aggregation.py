from __future__ import annotations

import math
from abc import ABC, abstractmethod
from bisect import bisect_left
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Dict, List, Optional

from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    ExtrapolationMode,
    Function,
    Reliability,
)

from snuba.query.dsl import CurriedFunctions as cf
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column
from snuba.query.expressions import (
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    SubscriptableReference,
)
from snuba.web.rpc.common.common import attribute_key_to_expression
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException

sampling_weight_column = column("sampling_weight")
sign_column = column("sign")

# Z value for 95% confidence interval is 1.96 which comes from the normal distribution z score.
z_value = 1.96

PERCENTILE_PRECISION = 100000
CONFIDENCE_INTERVAL_THRESHOLD = 1.5

CUSTOM_COLUMN_PREFIX = "__snuba_custom_column__"

_FLOATING_POINT_PRECISION = 8


@dataclass(frozen=True)
class ExtrapolationContext(ABC):
    value: float
    confidence_interval: Any
    average_sample_rate: float
    sample_count: int

    @property
    def is_data_present(self) -> bool:
        return self.sample_count > 0

    @property
    @abstractmethod
    def is_extrapolated(self) -> bool:
        raise NotImplementedError

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
                confidence_interval = col_value

                is_percentile = custom_column_information.metadata.get(
                    "function_type", ""
                ).startswith("p")
                if is_percentile:
                    percentile = (
                        float(custom_column_information.metadata["function_type"][1:])
                        / 100
                    )
                    granularity = float(
                        custom_column_information.metadata.get("granularity", "0")
                    )
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
            )

        return GenericExtrapolationContext(
            value=value,
            confidence_interval=confidence_interval,
            average_sample_rate=average_sample_rate,
            sample_count=sample_count,
        )


@dataclass(frozen=True)
class GenericExtrapolationContext(ExtrapolationContext):
    @property
    def is_extrapolated(self) -> bool:
        # We infer if a column is extrapolated or not by the presence of the
        # confidence interval. It will be present for extrapolated aggregates
        # but not for non-extrapolated aggregates and scalars.
        return self.confidence_interval is not None

    @cached_property
    def reliability(self) -> Reliability.ValueType:
        if not self.is_extrapolated or not self.is_data_present:
            return Reliability.RELIABILITY_UNSPECIFIED

        relative_confidence = (
            (self.value + self.confidence_interval) / self.value
            if self.value != 0
            else float("inf")
        )
        is_reliable = calculate_reliability(
            relative_confidence,
            self.sample_count,
            CONFIDENCE_INTERVAL_THRESHOLD,
        )
        if is_reliable:
            return Reliability.RELIABILITY_HIGH
        return Reliability.RELIABILITY_LOW


@dataclass(frozen=True)
class PercentileExtrapolationContext(ExtrapolationContext):
    percentile: float
    granularity: float
    width: float

    @property
    def is_extrapolated(self) -> bool:
        # We infer if a column is extrapolated or not by the presence of the
        # confidence interval. It will be present for extrapolated aggregates
        # but not for non-extrapolated aggregates and scalars.
        return self.confidence_interval is not None

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
        relative_confidence = max(
            self.value / ci_lower if ci_lower != 0 else float("inf"),
            ci_upper / self.value if self.value != 0 else float("inf"),
        )
        is_reliable = calculate_reliability(
            relative_confidence,
            self.sample_count,
            CONFIDENCE_INTERVAL_THRESHOLD,
        )
        if is_reliable:
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
    aggregation: AttributeAggregation, additional_metadata: dict[str, str] = {}
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
    return f.divide(
        f.sumIf(sign_column, get_field_existence_expression(aggregation)),
        f.sumIf(
            f.multiply(sign_column, sampling_weight_column),
            get_field_existence_expression(aggregation),
        ),
        alias=alias,
    )


def _get_count_column_alias(aggregation: AttributeAggregation) -> str:
    return CustomColumnInformation(
        custom_column_id="count",
        referenced_column=aggregation.label,
        metadata={},
    ).to_alias()


def get_count_column(aggregation: AttributeAggregation) -> Expression:
    return f.sumIf(
        sign_column,
        get_field_existence_expression(aggregation),
        alias=_get_count_column_alias(aggregation),
    )


def _get_possible_percentiles(
    percentile: float, granularity: float, width: float
) -> List[float]:
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
    aggregation: AttributeAggregation,
    percentile: float,
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
    field = attribute_key_to_expression(aggregation.key)
    possible_percentiles = _get_possible_percentiles(percentile, granularity, width)
    alias = get_attribute_confidence_interval_alias(
        aggregation, {"granularity": str(granularity), "width": str(width)}
    )
    alias_dict = {"alias": alias} if alias else {}
    return cf.quantilesTDigestWeighted(*possible_percentiles)(
        field,
        sampling_weight_column,
        **alias_dict,
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
            f.multiply(field, f.multiply(sign_column, sampling_weight_column)),
            **alias_dict,
        ),
        Function.FUNCTION_AVERAGE: f.divide(
            f.sum(f.multiply(field, f.multiply(sign_column, sampling_weight_column))),
            f.sumIf(
                f.multiply(sign_column, sampling_weight_column),
                get_field_existence_expression(aggregation),
            ),
            **alias_dict,
        ),
        Function.FUNCTION_AVG: f.divide(
            f.sum(f.multiply(field, f.multiply(sign_column, sampling_weight_column))),
            f.sumIf(
                f.multiply(sign_column, sampling_weight_column),
                get_field_existence_expression(aggregation),
            ),
            **alias_dict,
        ),
        Function.FUNCTION_COUNT: f.sumIf(
            f.multiply(sign_column, sampling_weight_column),
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
        Function.FUNCTION_MAX: f.max(field, **alias_dict),
        Function.FUNCTION_MIN: f.min(field, **alias_dict),
        Function.FUNCTION_UNIQ: f.uniq(field, **alias_dict),
    }

    return function_map_sample_weighted.get(aggregation.aggregate)


def get_confidence_interval_column(
    aggregation: AttributeAggregation,
) -> Expression | None:
    """
    Returns the expression for calculating the upper confidence limit for a given aggregation. If the aggregation cannot be extrapolated, returns None.
    Calculations are based on https://github.com/getsentry/extrapolation-math/blob/main/2024-10-04%20Confidence%20-%20Final%20Approach.ipynb
    Note that in the above notebook, the formulas are based on the sampling rate, while we perform calculations based on the sampling weight (1 / sampling rate).
    """
    field = attribute_key_to_expression(aggregation.key)
    alias = get_attribute_confidence_interval_alias(aggregation)
    alias_dict = {"alias": alias} if alias else {}

    function_map_confidence_interval = {
        # confidence interval = Z \cdot \sqrt{-log{(\frac{\sum_{i=1}^n \frac{1}{w_i}}{n})} \cdot \sum_{i=1}^n w_i^2 - w_i}
        #        ┌─────────────────────────┐
        #        │      ₙ
        #        │      ⎲  1
        #    ╲   │      ⎳  ──    ₙ
        #     ╲  │     ⁱ⁼¹ wᵢ    ⎲   2
        # Z *  ╲ │-log(──────) * ⎳  wᵢ - wᵢ
        #       ╲│       n      ⁱ⁼¹
        #
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
        # confidence interval = Z * \sqrt{-log{(\frac{\sum_{i=1}^n \frac{1}{w_i}}{n})} * \sum_{i=1}^n x_i^2w_i^2 - x_i^2w_i}
        #        ┌──────────────────────────────┐
        #        │      ₙ
        #        │      ⎲  1
        #    ╲   │      ⎳  ──     ₙ
        #     ╲  │     ⁱ⁼¹ wᵢ     ⎲   2 2    2
        # Z *   ╲ │-log(──────) *  ⎳  xᵢwᵢ - xᵢwᵢ
        #       ╲│       n       ⁱ⁼¹
        Function.FUNCTION_SUM: f.multiply(
            z_value,
            f.sqrt(
                f.multiply(
                    f.negate(f.log(get_average_sample_rate_column(aggregation))),
                    f.sumIf(
                        f.minus(
                            f.multiply(
                                f.multiply(field, field),
                                f.multiply(
                                    sampling_weight_column, sampling_weight_column
                                ),
                            ),
                            f.multiply(
                                f.multiply(field, field), sampling_weight_column
                            ),
                        ),
                        get_field_existence_expression(aggregation),
                    ),
                )
            ),
            **alias_dict,
        ),
        # confidence interval = Z * \sqrt{\frac{\sum_{i=1}^n w_ix_i^2 - \frac{(\sum_{i=1}^n w_ix_i)^2}{N}}{n * N}}
        #          ┌────────────────────────────┐
        #          │              ₙ
        #          │              ⎲
        #          │  ₙ         ( ⎳  wᵢxᵢ)²
        #     ╲    │  ⎲     2    ⁱ⁼¹
        #      ╲   │( ⎳  wᵢxᵢ - ───────────)
        #       ╲  │     ⁱ⁼¹         N
        # Z *    ╲ │────────────────────────────
        #         ╲│              n * N
        Function.FUNCTION_AVG: f.multiply(
            z_value,
            f.sqrt(
                f.divide(
                    f.minus(
                        f.sumIf(
                            f.multiply(
                                sampling_weight_column,
                                f.multiply(field, field),
                            ),
                            get_field_existence_expression(aggregation),
                        ),
                        f.divide(
                            f.multiply(
                                f.sumIf(
                                    f.multiply(sampling_weight_column, field),
                                    get_field_existence_expression(aggregation),
                                ),
                                f.sumIf(
                                    f.multiply(sampling_weight_column, field),
                                    get_field_existence_expression(aggregation),
                                ),
                            ),
                            column(f"{alias}_N"),
                        ),
                    ),
                    f.multiply(
                        f.sumIf(
                            sampling_weight_column,
                            get_field_existence_expression(aggregation),
                            alias=f"{alias}_N",
                        ),
                        f.sumIf(
                            sign_column, get_field_existence_expression(aggregation)
                        ),
                    ),
                )
            ),
            **alias_dict,
        ),
        Function.FUNCTION_P50: _get_possible_percentiles_expression(aggregation, 0.5),
        Function.FUNCTION_P75: _get_possible_percentiles_expression(aggregation, 0.75),
        Function.FUNCTION_P90: _get_possible_percentiles_expression(aggregation, 0.9),
        Function.FUNCTION_P95: _get_possible_percentiles_expression(aggregation, 0.95),
        Function.FUNCTION_P99: _get_possible_percentiles_expression(aggregation, 0.99),
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
    lower_index = n * p - z_value * math.sqrt(n * p * (1 - p))
    upper_index = 1 + n * p + z_value * math.sqrt(n * p * (1 - p))
    return (lower_index / n, upper_index / n)


def calculate_reliability(
    relative_confidence: float,
    sample_count: int,
    confidence_interval_threshold: float,
    sample_count_threshold: int = 100,
) -> bool:
    """
    A reliability check to determine if the sample count is large enough to be reliable and the confidence interval is small enough.
    """
    if sample_count < sample_count_threshold:
        return False

    return relative_confidence <= confidence_interval_threshold


def aggregation_to_expression(aggregation: AttributeAggregation) -> Expression:
    field = attribute_key_to_expression(aggregation.key)
    alias = aggregation.label if aggregation.label else None
    alias_dict = {"alias": alias} if alias else {}
    function_map: dict[Function.ValueType, CurriedFunctionCall | FunctionCall] = {
        Function.FUNCTION_SUM: f.round(
            f.sum(f.multiply(field, sign_column)),
            _FLOATING_POINT_PRECISION,
            **alias_dict,
        ),
        Function.FUNCTION_AVERAGE: f.divide(
            f.sum(f.multiply(field, sign_column)),
            f.sumIf(sign_column, get_field_existence_expression(aggregation)),
            **alias_dict,
        ),
        Function.FUNCTION_COUNT: f.sumIf(
            sign_column,
            get_field_existence_expression(aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_P50: cf.quantile(0.5)(field, **alias_dict),
        Function.FUNCTION_P75: cf.quantile(0.75)(field, **alias_dict),
        Function.FUNCTION_P90: cf.quantile(0.9)(field, **alias_dict),
        Function.FUNCTION_P95: cf.quantile(0.95)(field, **alias_dict),
        Function.FUNCTION_P99: cf.quantile(0.99)(field, **alias_dict),
        Function.FUNCTION_AVG: f.round(
            f.avg(field), _FLOATING_POINT_PRECISION, **alias_dict
        ),
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
