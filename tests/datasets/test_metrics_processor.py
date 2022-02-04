from datetime import datetime
from typing import Any, Mapping, Optional, Sequence

import pytest

from snuba import settings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.metrics_aggregate_processor import (
    CounterAggregateProcessor,
    DistributionsAggregateProcessor,
    MetricsAggregateProcessor,
    SetsAggregateProcessor,
    _array_literal,
    _call,
    _literal,
)
from snuba.datasets.metrics_bucket_processor import (
    CounterMetricsProcessor,
    DistributionsMetricsProcessor,
    SetsMetricsProcessor,
)
from snuba.processor import AggregateInsertBatch, InsertBatch

TEST_CASES_BUCKETS = [
    pytest.param(
        {
            "org_id": 1,
            "project_id": 2,
            "metric_id": 1232341,
            "type": "s",
            "timestamp": 1619225296,
            "tags": {"10": 11, "20": 22, "30": 33},
            "value": [324234, 345345, 456456, 567567],
            "retention_days": 30,
        },
        [
            {
                "org_id": 1,
                "project_id": 2,
                "metric_id": 1232341,
                "timestamp": datetime(2021, 4, 24, 0, 48, 16),
                "tags.key": [10, 20, 30],
                "tags.value": [11, 22, 33],
                "set_values": [324234, 345345, 456456, 567567],
                "materialization_version": 0,
                "retention_days": 30,
                "partition": 1,
                "offset": 100,
            }
        ],
        None,
        None,
        id="Simple set with valid content",
    ),
    pytest.param(
        {
            "org_id": 1,
            "project_id": 2,
            "metric_id": 1232341,
            "type": "c",
            "timestamp": 1619225296,
            "tags": {"10": 11, "20": 22, "30": 33},
            "value": 123.123,
            "retention_days": 30,
        },
        None,
        [
            {
                "org_id": 1,
                "project_id": 2,
                "metric_id": 1232341,
                "timestamp": datetime(2021, 4, 24, 0, 48, 16),
                "tags.key": [10, 20, 30],
                "tags.value": [11, 22, 33],
                "value": 123.123,
                "materialization_version": 0,
                "retention_days": 30,
                "partition": 1,
                "offset": 100,
            }
        ],
        None,
        id="Simple counter with valid content",
    ),
    pytest.param(
        {
            "org_id": 1,
            "project_id": 2,
            "metric_id": 1232341,
            "type": "d",
            "timestamp": 1619225296,
            "tags": {"10": 11, "20": 22, "30": 33},
            "value": [324.12, 345.23, 4564.56, 567567],
            "retention_days": 30,
        },
        None,
        None,
        [
            {
                "org_id": 1,
                "project_id": 2,
                "metric_id": 1232341,
                "timestamp": datetime(2021, 4, 24, 0, 48, 16),
                "tags.key": [10, 20, 30],
                "tags.value": [11, 22, 33],
                "values": [324.12, 345.23, 4564.56, 567567],
                "materialization_version": 0,
                "retention_days": 30,
                "partition": 1,
                "offset": 100,
            }
        ],
        id="Simple distribution with valid content",
    ),
]


@pytest.mark.parametrize(
    "message, expected_set, expected_counter, expected_distributions",
    TEST_CASES_BUCKETS,
)
def test_metrics_processor(
    message: Mapping[str, Any],
    expected_set: Optional[Sequence[Mapping[str, Any]]],
    expected_counter: Optional[Sequence[Mapping[str, Any]]],
    expected_distributions: Optional[Sequence[Mapping[str, Any]]],
) -> None:
    settings.DISABLED_DATASETS = set()

    meta = KafkaMessageMetadata(offset=100, partition=1, timestamp=datetime(1970, 1, 1))

    expected_set_result = (
        InsertBatch(expected_set, None) if expected_set is not None else None
    )
    assert SetsMetricsProcessor().process_message(message, meta) == expected_set_result

    expected_counter_result = (
        InsertBatch(expected_counter, None) if expected_counter is not None else None
    )
    assert (
        CounterMetricsProcessor().process_message(message, meta)
        == expected_counter_result
    )

    expected_distributions_result = (
        InsertBatch(expected_distributions, None)
        if expected_distributions is not None
        else None
    )
    assert (
        DistributionsMetricsProcessor().process_message(message, meta)
        == expected_distributions_result
    )


DIST_VALUES = [324.12, 345.23, 4564.56, 567567]
TEST_CASES_AGGREGATES = [
    pytest.param(
        {
            "org_id": 1,
            "project_id": 2,
            "metric_id": 1232341,
            "type": "s",
            "timestamp": 1619225296,
            "tags": {"10": 11, "20": 22, "30": 33},
            "value": [324234, 345345, 456456, 567567],
            "retention_days": 30,
        },
        [
            {
                "org_id": _literal(1),
                "project_id": _literal(2),
                "metric_id": _literal(1232341),
                "timestamp": _call(
                    "toStartOfInterval",
                    (
                        _call("toDateTime", (_literal("2021-04-24T00:48:16"),)),
                        _call("toIntervalSecond", (_literal(granularity),)),
                    ),
                ),
                "tags.key": _array_literal([10, 20, 30]),
                "tags.value": _array_literal([11, 22, 33]),
                "value": _call(
                    "arrayReduce",
                    (
                        _literal("uniqState"),
                        _array_literal([324234, 345345, 456456, 567567]),
                    ),
                ),
                "retention_days": _literal(30),
                "granularity": _literal(granularity),
                "partition": 1,
                "offset": 100,
            }
            for granularity in MetricsAggregateProcessor.GRANULARITIES_SECONDS
        ],
        None,
        None,
        id="Simple set with valid content",
    ),
    pytest.param(
        {
            "org_id": 1,
            "project_id": 2,
            "metric_id": 1232341,
            "type": "c",
            "timestamp": 1619225296,
            "tags": {"10": 11, "20": 22, "30": 33},
            "value": 123.123,
            "retention_days": 30,
        },
        None,
        [
            {
                "org_id": _literal(1),
                "project_id": _literal(2),
                "metric_id": _literal(1232341),
                "timestamp": _call(
                    "toStartOfInterval",
                    (
                        _call("toDateTime", (_literal("2021-04-24T00:48:16"),)),
                        _call("toIntervalSecond", (_literal(granularity),)),
                    ),
                ),
                "tags.key": _array_literal([10, 20, 30]),
                "tags.value": _array_literal([11, 22, 33]),
                "value": _call(
                    "arrayReduce", (_literal("sumState"), _array_literal([123.123]))
                ),
                "retention_days": _literal(30),
                "granularity": _literal(granularity),
                "partition": 1,
                "offset": 100,
            }
            for granularity in MetricsAggregateProcessor.GRANULARITIES_SECONDS
        ],
        None,
        id="Simple counter with valid content",
    ),
    pytest.param(
        {
            "org_id": 1,
            "project_id": 2,
            "metric_id": 1232341,
            "type": "d",
            "timestamp": 1619225296,
            "tags": {"10": 11, "20": 22, "30": 33},
            "value": DIST_VALUES,
            "retention_days": 30,
        },
        None,
        None,
        [
            {
                "org_id": _literal(1),
                "project_id": _literal(2),
                "metric_id": _literal(1232341),
                "timestamp": _call(
                    "toStartOfInterval",
                    (
                        _call("toDateTime", (_literal("2021-04-24T00:48:16"),)),
                        _call("toIntervalSecond", (_literal(granularity),)),
                    ),
                ),
                "tags.key": _array_literal([10, 20, 30]),
                "tags.value": _array_literal([11, 22, 33]),
                "percentiles": _call(
                    "arrayReduce",
                    (
                        _literal("quantilesState(0.5,0.75,0.9,0.95,0.99)"),
                        _array_literal(DIST_VALUES),
                    ),
                ),
                "min": _call(
                    "arrayReduce", (_literal("minState"), _array_literal([324.12]),)
                ),
                "max": _call(
                    "arrayReduce", (_literal("maxState"), _array_literal([567567]),)
                ),
                "avg": _call(
                    "arrayReduce", (_literal("avgState"), _array_literal(DIST_VALUES),),
                ),
                "sum": _call(
                    "arrayReduce",
                    (_literal("sumState"), _array_literal([sum(DIST_VALUES)]),),
                ),
                "count": _call(
                    "arrayReduce",
                    (
                        _literal("countState"),
                        _array_literal([float(len(DIST_VALUES))]),
                    ),
                ),
                "retention_days": _literal(30),
                "granularity": _literal(granularity),
                "partition": 1,
                "offset": 100,
            }
            for granularity in MetricsAggregateProcessor.GRANULARITIES_SECONDS
        ],
        id="Simple distribution with valid content",
    ),
]


@pytest.mark.parametrize(
    "message, expected_set, expected_counter, expected_distributions",
    TEST_CASES_AGGREGATES,
)
def test_metrics_aggregate_processor(
    message: Mapping[str, Any],
    expected_set: Optional[Sequence[Mapping[str, Any]]],
    expected_counter: Optional[Sequence[Mapping[str, Any]]],
    expected_distributions: Optional[Sequence[Mapping[str, Any]]],
) -> None:
    settings.DISABLED_DATASETS = set()
    settings.WRITE_METRICS_AGG_DIRECTLY = True

    meta = KafkaMessageMetadata(offset=100, partition=1, timestamp=datetime(1970, 1, 1))

    expected_set_result = (
        AggregateInsertBatch(expected_set, None) if expected_set is not None else None
    )
    assert (
        SetsAggregateProcessor().process_message(message, meta) == expected_set_result
    )

    expected_counter_result = (
        AggregateInsertBatch(expected_counter, None)
        if expected_counter is not None
        else None
    )
    assert (
        CounterAggregateProcessor().process_message(message, meta)
        == expected_counter_result
    )

    expected_distributions_result = (
        AggregateInsertBatch(expected_distributions, None)
        if expected_distributions is not None
        else None
    )
    assert (
        DistributionsAggregateProcessor().process_message(message, meta)
        == expected_distributions_result
    )

    settings.WRITE_METRICS_AGG_DIRECTLY = False
