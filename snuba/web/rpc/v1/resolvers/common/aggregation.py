from __future__ import annotations

import math
from abc import ABC, abstractmethod
from bisect import bisect_left
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Dict, List, Optional

from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    Expression as TimeSeriesExpression,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    AggregationComparisonFilter,
    AggregationFilter,
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    ExtrapolationMode,
    Function,
    Reliability,
)

from snuba.query.dsl import CurriedFunctions as cf
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, column, literal
from snuba.query.expressions import CurriedFunctionCall, Expression, FunctionCall
from snuba.web.rpc.common.common import (
    get_field_existence_expression,
    trace_item_filters_to_expression,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers.R_eap_spans.common.common import (
    attribute_key_to_expression,
)

sampling_weight_column = column("sampling_weight")

# Z value for 95% confidence interval is 1.96 which comes from the normal distribution z score.
z_value = 1.96

PERCENTILE_PRECISION = 100000
CONFIDENCE_INTERVAL_THRESHOLD = 1.5

CUSTOM_COLUMN_PREFIX = "__snuba_custom_column__"

_FLOATING_POINT_PRECISION = 9


def _get_condition_in_aggregation(
    aggregation: AttributeAggregation | AttributeConditionalAggregation,
) -> Expression:
    condition_in_aggregation: Expression = literal(True)
    if isinstance(aggregation, AttributeConditionalAggregation):
        condition_in_aggregation = trace_item_filters_to_expression(
            aggregation.filter, attribute_key_to_expression
        )
    return condition_in_aggregation


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

        relative_confidence = (
            (self.value + self.confidence_interval) / self.value
            if self.value != 0
            else float("inf")
        )
        if relative_confidence <= CONFIDENCE_INTERVAL_THRESHOLD:
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
        relative_confidence = max(
            self.value / ci_lower if ci_lower != 0 else float("inf"),
            ci_upper / self.value if self.value != 0 else float("inf"),
        )
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


def convert_to_conditional_aggregation(
    in_msg: TraceItemTableRequest | TimeSeriesRequest,
) -> None:
    """
    Up to this point we support aggregation, but now we want to support conditional aggregation, which only aggregates
    if the field satisfies the condition: https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators#-if

    For messages that don't have conditional aggregation, this function replaces the aggregation with a conditional aggregation,
    where the filter is null, and every field is the same. This allows code elsewhere to set the default condition to always
    be true.

    The reason we do this "transformation" is to avoid code fragmentation down the line, where we constantly have to check
    if the request contains `AttributeAggregation` or `AttributeConditionalAggregation`
    """

    def _add_conditional_aggregation(
        input: Column | AggregationComparisonFilter | TimeSeriesExpression,
    ) -> None:
        aggregation = input.aggregation
        input.ClearField("aggregation")
        input.conditional_aggregation.CopyFrom(
            AttributeConditionalAggregation(
                aggregate=aggregation.aggregate,
                key=aggregation.key,
                label=aggregation.label,
                extrapolation_mode=aggregation.extrapolation_mode,
            )
        )

    def _convert(input: Column | AggregationFilter | TimeSeriesExpression) -> None:
        if isinstance(input, Column):
            if input.HasField("aggregation"):
                _add_conditional_aggregation(input)

            if input.HasField("formula"):
                _convert(input.formula.left)
                _convert(input.formula.right)

        if isinstance(input, AggregationFilter):
            if input.HasField("and_filter"):
                for aggregation_filter in input.and_filter.filters:
                    _convert(aggregation_filter)
            if input.HasField("or_filter"):
                for aggregation_filter in input.or_filter.filters:
                    _convert(aggregation_filter)
            if input.HasField("comparison_filter"):
                if input.comparison_filter.HasField("aggregation"):
                    _add_conditional_aggregation(input.comparison_filter)

        if isinstance(input, TimeSeriesExpression):
            if input.HasField("aggregation"):
                _add_conditional_aggregation(input)
            if input.HasField("formula"):
                _convert(input.formula.left)
                _convert(input.formula.right)

    if isinstance(in_msg, TraceItemTableRequest):
        for col in in_msg.columns:
            _convert(col)
        for ob in in_msg.order_by:
            _convert(ob.column)
        if in_msg.HasField("aggregation_filter"):
            _convert(in_msg.aggregation_filter)

    if isinstance(in_msg, TimeSeriesRequest):
        for expression in in_msg.expressions:
            _convert(expression)


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
    aggregation: AttributeAggregation | AttributeConditionalAggregation,
) -> Expression:
    alias = CustomColumnInformation(
        custom_column_id="average_sample_rate",
        referenced_column=aggregation.label,
        metadata={},
    ).to_alias()
    field = attribute_key_to_expression(aggregation.key)
    condition_in_aggregation = _get_condition_in_aggregation(aggregation)
    return f.divide(
        f.countIf(
            field,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
        ),
        f.sumIf(
            sampling_weight_column,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
        ),
        alias=alias,
    )


def _get_count_column_alias(
    aggregation: AttributeAggregation | AttributeConditionalAggregation,
) -> str:
    return CustomColumnInformation(
        custom_column_id="count",
        referenced_column=aggregation.label,
        metadata={},
    ).to_alias()


def get_count_column(
    aggregation: AttributeAggregation | AttributeConditionalAggregation,
) -> Expression:
    field = attribute_key_to_expression(aggregation.key)
    return f.countIf(
        field,
        and_cond(
            get_field_existence_expression(field),
            _get_condition_in_aggregation(aggregation),
        ),
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
    aggregation: AttributeAggregation | AttributeConditionalAggregation,
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
    aggregation: AttributeAggregation | AttributeConditionalAggregation,
    field: Expression,
) -> CurriedFunctionCall | FunctionCall | None:
    sampling_weight_column = column("sampling_weight")
    alias = aggregation.label if aggregation.label else None
    alias_dict = {"alias": alias} if alias else {}
    condition_in_aggregation = _get_condition_in_aggregation(aggregation)
    function_map_sample_weighted: dict[
        Function.ValueType, CurriedFunctionCall | FunctionCall
    ] = {
        Function.FUNCTION_SUM: f.sumIfOrNull(
            f.multiply(field, sampling_weight_column),
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_AVERAGE: f.divide(
            f.sumIfOrNull(
                f.multiply(field, sampling_weight_column),
                and_cond(
                    get_field_existence_expression(field), condition_in_aggregation
                ),
            ),
            f.sumIfOrNull(
                sampling_weight_column,
                and_cond(
                    get_field_existence_expression(field), condition_in_aggregation
                ),
            ),
            **alias_dict,
        ),
        Function.FUNCTION_AVG: f.divide(
            f.sumIfOrNull(
                f.multiply(field, sampling_weight_column),
                and_cond(
                    get_field_existence_expression(field), condition_in_aggregation
                ),
            ),
            f.sumIfOrNull(
                sampling_weight_column,
                and_cond(
                    get_field_existence_expression(field), condition_in_aggregation
                ),
            ),
            **alias_dict,
        ),
        Function.FUNCTION_COUNT: f.sumIfOrNull(
            sampling_weight_column,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_P50: cf.quantileTDigestWeightedIfOrNull(0.5)(
            field,
            sampling_weight_column,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_P75: cf.quantileTDigestWeightedIfOrNull(0.75)(
            field,
            sampling_weight_column,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_P90: cf.quantileTDigestWeightedIfOrNull(0.9)(
            field,
            sampling_weight_column,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_P95: cf.quantileTDigestWeightedIfOrNull(0.95)(
            field,
            sampling_weight_column,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_P99: cf.quantileTDigestWeightedIfOrNull(0.99)(
            field,
            sampling_weight_column,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_MAX: f.maxIfOrNull(
            field,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_MIN: f.minIfOrNull(
            field,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_UNIQ: f.uniqIfOrNull(
            field,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
            **alias_dict,
        ),
    }

    return function_map_sample_weighted.get(aggregation.aggregate)


def get_confidence_interval_column(
    aggregation: AttributeAggregation | AttributeConditionalAggregation,
) -> Expression | None:
    """
    Returns the expression for calculating the upper confidence limit for a given aggregation. If the aggregation cannot be extrapolated, returns None.
    Calculations are based on https://github.com/getsentry/extrapolation-math/blob/main/2024-10-04%20Confidence%20-%20Final%20Approach.ipynb
    Note that in the above notebook, the formulas are based on the sampling rate, while we perform calculations based on the sampling weight (1 / sampling rate).
    """
    field = attribute_key_to_expression(aggregation.key)
    alias = get_attribute_confidence_interval_alias(aggregation)
    alias_dict = {"alias": alias} if alias else {}

    condition_in_aggregation = _get_condition_in_aggregation(aggregation)

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
                        and_cond(
                            get_field_existence_expression(field),
                            condition_in_aggregation,
                        ),
                    ),
                )
            ),
            **alias_dict,
        ),
        # confidence interval = N * Z * \sqrt{\frac{\sum_{i=1}^n w_ix_i^2 - \frac{(\sum_{i=1}^n w_ix_i)^2}{N}}{n * N}}
        #              ┌────────────────────────────┐
        #              │              ₙ
        #              │              ⎲
        #              │  ₙ         ( ⎳  wᵢxᵢ)²
        #         ╲    │  ⎲     2    ⁱ⁼¹
        #          ╲   │( ⎳  wᵢxᵢ - ───────────)
        #           ╲  │     ⁱ⁼¹         N
        # N * Z *    ╲ │────────────────────────────
        #             ╲│              n * N
        Function.FUNCTION_SUM: f.multiply(
            column(f"{alias}_N"),
            f.multiply(
                z_value,
                f.sqrt(
                    f.divide(
                        f.minus(
                            f.sumIf(
                                f.multiply(
                                    sampling_weight_column,
                                    f.multiply(field, field),
                                ),
                                and_cond(
                                    get_field_existence_expression(field),
                                    condition_in_aggregation,
                                ),
                            ),
                            f.divide(
                                f.multiply(
                                    f.sumIf(
                                        f.multiply(sampling_weight_column, field),
                                        get_field_existence_expression(field),
                                    ),
                                    f.sumIf(
                                        f.multiply(sampling_weight_column, field),
                                        and_cond(
                                            get_field_existence_expression(field),
                                            condition_in_aggregation,
                                        ),
                                    ),
                                ),
                                column(f"{alias}_N"),
                            ),
                        ),
                        f.multiply(
                            f.sumIf(
                                sampling_weight_column,
                                and_cond(
                                    get_field_existence_expression(field),
                                    condition_in_aggregation,
                                ),
                                alias=f"{alias}_N",
                            ),
                            f.countIf(
                                field,
                                and_cond(
                                    get_field_existence_expression(field),
                                    condition_in_aggregation,
                                ),
                            ),
                        ),
                    )
                ),
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
                            and_cond(
                                get_field_existence_expression(field),
                                condition_in_aggregation,
                            ),
                        ),
                        f.divide(
                            f.multiply(
                                f.sumIf(
                                    f.multiply(sampling_weight_column, field),
                                    and_cond(
                                        get_field_existence_expression(field),
                                        condition_in_aggregation,
                                    ),
                                ),
                                f.sumIf(
                                    f.multiply(sampling_weight_column, field),
                                    and_cond(
                                        get_field_existence_expression(field),
                                        condition_in_aggregation,
                                    ),
                                ),
                            ),
                            column(f"{alias}_N"),
                        ),
                    ),
                    f.multiply(
                        f.sumIf(
                            sampling_weight_column,
                            and_cond(
                                get_field_existence_expression(field),
                                condition_in_aggregation,
                            ),
                            alias=f"{alias}_N",
                        ),
                        f.countIf(
                            field,
                            and_cond(
                                get_field_existence_expression(field),
                                condition_in_aggregation,
                            ),
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


def aggregation_to_expression(
    aggregation: AttributeAggregation | AttributeConditionalAggregation,
    field: Expression | None = None,
) -> Expression:
    field = field or attribute_key_to_expression(aggregation.key)
    alias = aggregation.label if aggregation.label else None
    alias_dict = {"alias": alias} if alias else {}
    condition_in_aggregation = _get_condition_in_aggregation(aggregation)
    function_map: dict[Function.ValueType, CurriedFunctionCall | FunctionCall] = {
        Function.FUNCTION_SUM: f.sumIfOrNull(
            field,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
        ),
        Function.FUNCTION_AVERAGE: f.avgIfOrNull(
            field,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
        ),
        Function.FUNCTION_COUNT: f.countIfOrNull(
            field,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
        ),
        Function.FUNCTION_P50: cf.quantileIfOrNull(0.5)(
            field,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
        ),
        Function.FUNCTION_P75: cf.quantileIfOrNull(0.75)(
            field,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
        ),
        Function.FUNCTION_P90: cf.quantileIfOrNull(0.9)(
            field,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
        ),
        Function.FUNCTION_P95: cf.quantileIfOrNull(0.95)(
            field,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
        ),
        Function.FUNCTION_P99: cf.quantileIfOrNull(0.99)(
            field,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
        ),
        Function.FUNCTION_AVG: f.avgIfOrNull(
            field,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
        ),
        Function.FUNCTION_MAX: f.maxIfOrNull(
            field,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
        ),
        Function.FUNCTION_MIN: f.minIfOrNull(
            field,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
        ),
        Function.FUNCTION_UNIQ: f.uniqIfOrNull(
            field,
            and_cond(get_field_existence_expression(field), condition_in_aggregation),
        ),
    }

    if (
        aggregation.extrapolation_mode
        == ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED
    ):
        agg_func_expr = get_extrapolated_function(aggregation, field)
    else:
        agg_func_expr = function_map.get(aggregation.aggregate)
        if agg_func_expr is not None:
            agg_func_expr = f.round(
                agg_func_expr, _FLOATING_POINT_PRECISION, **alias_dict
            )

    if agg_func_expr is None:
        raise BadSnubaRPCRequestException(
            f"Aggregation not specified for {aggregation.key.name}"
        )

    return agg_func_expr
