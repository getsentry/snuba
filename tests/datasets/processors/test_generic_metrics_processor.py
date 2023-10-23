from typing import Any, Mapping

import pytest

from snuba.datasets.processors.generic_metrics_processor import (
    GenericCountersMetricsProcessor,
    GenericDistributionsMetricsProcessor,
    GenericGaugesMetricsProcessor,
    GenericSetsMetricsProcessor,
)


@pytest.mark.parametrize(
    "message, expected_output",
    [
        pytest.param({"type": "c", "metric_id": 1, "value": 10}, True, id="counter"),
        pytest.param(
            {"type": "d", "metric_id": 2, "value": 20},
            False,
            id="distribution",
        ),
    ],
)
def test__counters_should_process(
    message: Mapping[str, Any], expected_output: Mapping[str, Any]
) -> None:
    counters_processor = GenericCountersMetricsProcessor()
    assert counters_processor._should_process(message) == expected_output


@pytest.mark.parametrize(
    "message, expected_output",
    [
        pytest.param(
            {
                "type": "g",
                "metric_id": 1,
                "value": {"min": 0, "max": 3, "sum": 3, "count": 2, "last": 3},
            },
            True,
            id="gauge",
        ),
        pytest.param(
            {"type": "d", "metric_id": 2, "value": 20},
            False,
            id="distribution",
        ),
    ],
)
def test__gauges_should_process(
    message: Mapping[str, Any], expected_output: Mapping[str, Any]
) -> None:
    gauges_processor = GenericGaugesMetricsProcessor()
    assert gauges_processor._should_process(message) == expected_output


@pytest.mark.parametrize(
    "message, expected_output, retention_days",
    [
        pytest.param(
            {
                "type": "s",
                "metric_id": 3,
                "value": [0],
                "aggregation_option": "ten_second",
            },
            {
                "min_retention_days": 90,
                "materialization_version": 2,
                "granularities": [1, 2, 3, 0],
            },
            90,
            id="ten_second",
        ),
    ],
)
def test__sets_aggregation_options(
    message: Mapping[str, Any], expected_output: Mapping[str, Any], retention_days: int
) -> None:
    processor = GenericSetsMetricsProcessor()
    assert processor._aggregation_options(message, retention_days) == expected_output


@pytest.mark.parametrize(
    "message, expected_output, retention_days",
    [
        pytest.param(
            {
                "type": "d",
                "metric_id": 3,
                "value": [5, 7, 10],
                "aggregation_option": "ten_second",
            },
            {
                "min_retention_days": 90,
                "materialization_version": 2,
                "granularities": [1, 2, 3, 0],
            },
            90,
            id="ten_second",
        ),
        pytest.param(
            {
                "type": "d",
                "metric_id": 4,
                "value": [5, 7, 10],
                "aggregation_option": "hist",
            },
            {
                "min_retention_days": 90,
                "materialization_version": 2,
                "granularities": [1, 2, 3],
                "enable_histogram": 1,
            },
            90,
            id="hist",
        ),
    ],
)
def test__dists_aggregation_options(
    message: Mapping[str, Any], expected_output: Mapping[str, Any], retention_days: int
) -> None:
    processor = GenericDistributionsMetricsProcessor()
    assert processor._aggregation_options(message, retention_days) == expected_output


@pytest.mark.parametrize(
    "message, expected_output, retention_days",
    [
        pytest.param(
            {
                "type": "s",
                "metric_id": 3,
                "value": 1,
                "aggregation_option": "ten_second",
            },
            {
                "min_retention_days": 90,
                "materialization_version": 2,
                "granularities": [1, 2, 3, 0],
            },
            90,
            id="ten_second",
        ),
    ],
)
def test__counters_aggregation_options(
    message: Mapping[str, Any], expected_output: Mapping[str, Any], retention_days: int
) -> None:
    processor = GenericCountersMetricsProcessor()
    assert processor._aggregation_options(message, retention_days) == expected_output


@pytest.mark.parametrize(
    "message, expected_output, retention_days",
    [
        pytest.param(
            {
                "type": "g",
                "metric_id": 3,
                "value": {"min": 0.0, "max": 3.0, "sum": 3.0, "count": 2, "last": 3.0},
                "aggregation_option": "ten_second",
            },
            {
                "min_retention_days": 90,
                "materialization_version": 2,
                "granularities": [1, 2, 3, 0],
            },
            90,
            id="ten_second",
        ),
    ],
)
def test__gauges_aggregation_options(
    message: Mapping[str, Any], expected_output: Mapping[str, Any], retention_days: int
) -> None:
    processor = GenericGaugesMetricsProcessor()
    assert processor._aggregation_options(message, retention_days) == expected_output


@pytest.mark.parametrize(
    "message, expected_output, should_raise_exception",
    [
        pytest.param(
            {"type": "c", "name": "my_counter", "value": 10},
            {"count_value": 10, "metric_type": "counter"},
            False,
            id="counter_int",
        ),
        pytest.param(
            {"type": "c", "name": "my_counter", "value": 10.99},
            {"count_value": 10.99, "metric_type": "counter"},
            False,
            id="counter_float",
        ),
        pytest.param(
            {"type": "c", "name": "my_counter", "value": [1, 2, 3]},
            None,
            True,
            id="counter_list",
        ),
    ],
)
def test__counters_process_values(
    message: Mapping[str, Any],
    expected_output: Mapping[str, Any],
    should_raise_exception: bool,
) -> None:
    processor = GenericCountersMetricsProcessor()
    if should_raise_exception:
        with pytest.raises(Exception):
            processor._process_values(message)
    else:
        assert processor._process_values(message) == expected_output


@pytest.mark.parametrize(
    "message, expected_output, should_raise_exception",
    [
        pytest.param(
            {
                "type": "g",
                "name": "my_gauge1",
                "value": {"min": 0.0, "max": 3.0, "sum": 3.0, "count": 2, "last": 3.0},
            },
            {
                "gauges_values.min": [0.0],
                "gauges_values.max": [3.0],
                "gauges_values.sum": [3.0],
                "gauges_values.count": [2],
                "gauges_values.avg": [1.5],
                "gauges_values.last": [3.0],
                "metric_type": "gauge",
            },
            False,
            id="simple_gauge",
        ),
        pytest.param(
            {
                "type": "g",
                "name": "my_gauge2",
                "value": {"min": 1.0, "max": 3.0, "sum": 7.0, "count": 3, "last": 1.0},
            },
            {
                "gauges_values.min": [1.0],
                "gauges_values.max": [3.0],
                "gauges_values.sum": [7.0],
                "gauges_values.count": [3],
                "gauges_values.avg": [7.0 / 3],
                "gauges_values.last": [1.0],
                "metric_type": "gauge",
            },
            False,
            id="less_simple_gauge",
        ),
        pytest.param(
            {"type": "g", "name": "my_gauge3", "value": [1, 2, 3]},
            None,
            True,
            id="gauge_list",
        ),
    ],
)
def test__gauges_process_values(
    message: Mapping[str, Any],
    expected_output: Mapping[str, Any],
    should_raise_exception: bool,
) -> None:
    processor = GenericGaugesMetricsProcessor()
    if should_raise_exception:
        with pytest.raises(Exception):
            processor._process_values(message)
    else:
        assert processor._process_values(message) == expected_output
