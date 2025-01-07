from typing import Any

import pytest
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    ExtrapolationMode,
    Function,
    Reliability,
)

from snuba.web.rpc.common.aggregation import (
    CUSTOM_COLUMN_PREFIX,
    CustomColumnInformation,
    ExtrapolationContext,
    _get_closest_percentile_index,
    get_confidence_interval_column,
)


def test_generate_custom_column_alias() -> None:
    custom_column_information_with_metadata = CustomColumnInformation(
        custom_column_id="confidence_interval",
        referenced_column="count",
        metadata={"function_type": "count", "additional_metadata": "value"},
    )

    assert custom_column_information_with_metadata.to_alias() == (
        CUSTOM_COLUMN_PREFIX
        + "confidence_interval$count$function_type:count,additional_metadata:value"
    )


def test_generate_custom_column_alias_without_metadata() -> None:
    custom_column_information_without_metadata = CustomColumnInformation(
        custom_column_id="column_id",
        referenced_column="count",
        metadata={},
    )

    assert (
        custom_column_information_without_metadata.to_alias()
        == CUSTOM_COLUMN_PREFIX + "column_id$count"
    )


def test_generate_custom_column_alias_without_referenced_column() -> None:
    custom_column_information_without_referenced_column = CustomColumnInformation(
        custom_column_id="column_id",
        referenced_column=None,
        metadata={},
    )

    assert (
        custom_column_information_without_referenced_column.to_alias()
        == CUSTOM_COLUMN_PREFIX + "column_id"
    )


def test_get_custom_column_information() -> None:
    alias = CUSTOM_COLUMN_PREFIX + "column_type$count$meta1:value1,meta2:value2"
    custom_column_information = CustomColumnInformation.from_alias(alias)
    assert custom_column_information == CustomColumnInformation(
        custom_column_id="column_type",
        referenced_column="count",
        metadata={"meta1": "value1", "meta2": "value2"},
    )


def test_get_confidence_interval_column_for_non_extrapolatable_column() -> None:
    assert (
        get_confidence_interval_column(
            AttributeAggregation(
                aggregate=Function.FUNCTION_MIN,
                key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test"),
                label="min(test)",
                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
            )
        )
        is None
    )


@pytest.mark.parametrize(
    (
        "row_data",
        "column_name",
        "average_sample_rate",
        "reliability",
        "extrapolated_data_present",
        "is_extrapolated",
    ),
    [
        (
            {
                "time": "2024-4-20 16:20:00",
                "count(sentry.duration)": 100,
                "p95(sentry.duration)": 123456,
                CustomColumnInformation(
                    custom_column_id="confidence_interval",
                    referenced_column="count(sentry.duration)",
                    metadata={"function_type": "count"},
                ).to_alias(): 20,
                CustomColumnInformation(
                    custom_column_id="average_sample_rate",
                    referenced_column="count(sentry.duration)",
                    metadata={},
                ).to_alias(): 0.5,
                CustomColumnInformation(
                    custom_column_id="count",
                    referenced_column="count(sentry.duration)",
                    metadata={},
                ).to_alias(): 120,
                "group_by_attr_1": "g1",
                "group_by_attr_2": "a1",
            },
            "count(sentry.duration)",
            0.5,
            Reliability.RELIABILITY_HIGH,
            True,
            True,
        ),
        (
            {
                "time": "2024-4-20 16:20:00",
                "count(sentry.duration)": 100,
                "min(sentry.duration)": 123456,
                CustomColumnInformation(
                    custom_column_id="confidence_interval",
                    referenced_column="count(sentry.duration)",
                    metadata={"function_type": "count"},
                ).to_alias(): 20,
                CustomColumnInformation(
                    custom_column_id="average_sample_rate",
                    referenced_column="count(sentry.duration)",
                    metadata={},
                ).to_alias(): 0.5,
                CustomColumnInformation(
                    custom_column_id="count",
                    referenced_column="count(sentry.duration)",
                    metadata={},
                ).to_alias(): 120,
                "group_by_attr_1": "g1",
                "group_by_attr_2": "a1",
            },
            "min(sentry.duration)",
            0,
            Reliability.RELIABILITY_UNSPECIFIED,
            False,
            False,
        ),
        (
            {
                "time": "2024-4-20 16:20:00",
                "count(sentry.duration)": 100,
                "p95(sentry.duration)": 123456,
                CustomColumnInformation(
                    custom_column_id="confidence_interval",
                    referenced_column="count(sentry.duration)",
                    metadata={"function_type": "count"},
                ).to_alias(): 0,
                CustomColumnInformation(
                    custom_column_id="average_sample_rate",
                    referenced_column="count(sentry.duration)",
                    metadata={},
                ).to_alias(): 0,
                CustomColumnInformation(
                    custom_column_id="count",
                    referenced_column="count(sentry.duration)",
                    metadata={},
                ).to_alias(): 0,
                "group_by_attr_1": "g1",
                "group_by_attr_2": "a1",
            },
            "count(sentry.duration)",
            0,
            Reliability.RELIABILITY_UNSPECIFIED,
            False,
            True,
        ),
    ],
)
def test_get_extrapolation_meta(
    row_data: dict[str, Any],
    column_name: str,
    average_sample_rate: float,
    reliability: Reliability.ValueType,
    extrapolated_data_present: bool,
    is_extrapolated: bool,
) -> None:
    extrapolation_context = ExtrapolationContext.from_row(column_name, row_data)
    assert extrapolation_context.average_sample_rate == average_sample_rate
    assert extrapolation_context.reliability == reliability
    assert extrapolation_context.extrapolated_data_present == extrapolated_data_present
    assert extrapolation_context.is_extrapolated == is_extrapolated


@pytest.mark.parametrize(
    ("value", "percentile", "granularity", "width", "expected_index"),
    [
        (
            0,
            0.5,
            0.05,
            0.1,
            0,
        ),  # possible percentiles are [0.4, 0.45, 0.5, 0.55], closest to 0 is 0.4
        (
            0.43,
            0.5,
            0.05,
            0.1,
            1,
        ),  # possible percentiles are [0.4, 0.45, 0.5, 0.55], closest to 0.43 is 0.45
        (
            0.52,
            0.5,
            0.05,
            0.1,
            2,
        ),  # possible percentiles are [0.4, 0.45, 0.5, 0.55], closest to 0.52 is 0.5
        (
            0.8,
            0.5,
            0.05,
            0.1,
            3,
        ),  # possible percentiles are [0.4, 0.45, 0.5, 0.55], closest to 0.8 is 0.55
    ],
)
def test_get_closest_percentile_index(
    value: float,
    percentile: float,
    granularity: float,
    width: float,
    expected_index: int,
) -> None:
    assert (
        _get_closest_percentile_index(value, percentile, granularity, width)
        == expected_index
    )
