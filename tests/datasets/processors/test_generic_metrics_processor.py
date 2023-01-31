import pytest

from snuba.datasets.processors.generic_metrics_processor import (
    GenericCountersMetricsProcessor,
)


@pytest.mark.parametrize(
    "message, expected_output",
    [
        pytest.param(
            {"type": "c", "name": "my_counter", "value": 10}, True, id="counter"
        ),
        pytest.param(
            {"type": "d", "name": "my_distribution", "value": 20},
            False,
            id="distribution",
        ),
    ],
)
def test__should_process(message, expected_output):
    processor = GenericCountersMetricsProcessor()
    assert processor._should_process(message) == expected_output


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
def test__process_values(message, expected_output, should_raise_exception):
    processor = GenericCountersMetricsProcessor()
    if should_raise_exception:
        with pytest.raises(Exception):
            processor._process_values(message)
    else:
        assert processor._process_values(message) == expected_output
