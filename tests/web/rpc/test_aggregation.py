import pytest
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    ExtrapolationMode,
    Function,
)

from snuba.web.rpc.common.aggregation import (
    CUSTOM_COLUMN_PREFIX,
    CustomColumnInformation,
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
