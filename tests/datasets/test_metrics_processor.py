from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence
from unittest.mock import ANY, patch

import pytest

from snuba import settings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.generic_metrics_processor import GenericSetsMetricsProcessor
from snuba.datasets.metrics_aggregate_processor import (
    CounterAggregateProcessor,
    DistributionsAggregateProcessor,
    MetricsAggregateProcessor,
    SetsAggregateProcessor,
    _array_literal,
    _call,
    _literal,
    timestamp_to_bucket,
)
from snuba.datasets.metrics_bucket_processor import (
    CounterMetricsProcessor,
    DistributionsMetricsProcessor,
    PolymorphicMetricsProcessor,
    SetsMetricsProcessor,
)
from snuba.processor import AggregateInsertBatch, InsertBatch

MATERIALIZATION_VERSION = 4

timestamp = int(datetime.now(timezone.utc).timestamp())
# expects that test is run in utc local time
expected_timestamp = datetime.utcfromtimestamp(timestamp)

MAPPING_META_COMMON = {
    "c": {
        "10": "tag-1",
        "20": "tag-2",
        "11": "value-1",
        "22": "value-2",
        "30": "tag-3",
    },
    "d": {"33": "value-3"},
}

MAPPING_META_TAG_VALUES_STRINGS = {
    "c": {
        "10": "tag-1",
        "20": "tag-2",
        "30": "tag-3",
    },
}

MAPPING_META_TAG_VALUES_MIXED_STRINGS_INTS = {
    "c": {
        "10": "tag-1",
        "20": "tag-2",
        "22": "value-2",
        "30": "tag-3",
    },
    "d": {"33": "value-3"},
}

SET_MESSAGE_SHARED = {
    "use_case_id": "release-health",
    "org_id": 1,
    "project_id": 2,
    "metric_id": 1232341,
    "type": "s",
    "timestamp": timestamp,
    "tags": {"10": 11, "20": 22, "30": 33},
    "value": [324234, 345345, 456456, 567567],
    # test enforce retention days of 30
    "retention_days": 22,
    "mapping_meta": MAPPING_META_COMMON,
}

SET_MESSAGE_TAG_VALUES_STRINGS = {
    "use_case_id": "release-health",
    "org_id": 1,
    "project_id": 2,
    "metric_id": 1232341,
    "type": "s",
    "timestamp": timestamp,
    "tags": {"10": "value-1", "20": "value-2", "30": "value-3"},
    "value": [324234, 345345, 456456, 567567],
    # test enforce retention days of 30
    "retention_days": 22,
    "mapping_meta": MAPPING_META_TAG_VALUES_STRINGS,
}

SET_MESSAGE_TAG_VALUES_MIXED_STRINGS_INTS = {
    "use_case_id": "release-health",
    "org_id": 1,
    "project_id": 2,
    "metric_id": 1232341,
    "type": "s",
    "timestamp": timestamp,
    "tags": {"10": "value-1", "20": 22, "30": 33},
    "value": [324234, 345345, 456456, 567567],
    # test enforce retention days of 30
    "retention_days": 22,
    "mapping_meta": MAPPING_META_TAG_VALUES_MIXED_STRINGS_INTS,
}

COUNTER_MESSAGE_SHARED = {
    "use_case_id": "release-health",
    "org_id": 1,
    "project_id": 2,
    "metric_id": 1232341,
    "type": "c",
    "timestamp": timestamp,
    "tags": {"10": 11, "20": 22, "30": 33},
    "value": 123.123,
    # test enforce retention days of 30
    "retention_days": 23,
    "mapping_meta": MAPPING_META_COMMON,
}

DIST_VALUES = [324.12, 345.23, 4564.56, 567567]
DIST_MESSAGE_SHARED = {
    "use_case_id": "release-health",
    "org_id": 1,
    "project_id": 2,
    "metric_id": 1232341,
    "type": "d",
    "timestamp": timestamp,
    "tags": {"10": 11, "20": 22, "30": 33},
    "value": DIST_VALUES,
    # test enforce retention days of 90
    "retention_days": 50,
    "mapping_meta": MAPPING_META_COMMON,
}

TEST_CASES_BUCKETS = [
    pytest.param(
        SET_MESSAGE_SHARED,
        [
            {
                "org_id": 1,
                "project_id": 2,
                "metric_id": 1232341,
                "timestamp": expected_timestamp,
                "tags.key": [10, 20, 30],
                "tags.value": [11, 22, 33],
                "set_values": [324234, 345345, 456456, 567567],
                "materialization_version": MATERIALIZATION_VERSION,
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
        COUNTER_MESSAGE_SHARED,
        None,
        [
            {
                "org_id": 1,
                "project_id": 2,
                "metric_id": 1232341,
                "timestamp": expected_timestamp,
                "tags.key": [10, 20, 30],
                "tags.value": [11, 22, 33],
                "value": 123.123,
                "materialization_version": MATERIALIZATION_VERSION,
                "retention_days": 30,
                "partition": 1,
                "offset": 100,
            }
        ],
        None,
        id="Simple counter with valid content",
    ),
    pytest.param(
        DIST_MESSAGE_SHARED,
        None,
        None,
        [
            {
                "org_id": 1,
                "project_id": 2,
                "metric_id": 1232341,
                "timestamp": expected_timestamp,
                "tags.key": [10, 20, 30],
                "tags.value": [11, 22, 33],
                "values": [324.12, 345.23, 4564.56, 567567],
                "materialization_version": MATERIALIZATION_VERSION,
                "retention_days": 90,
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


MOCK_TIME_BUCKET = expected_timestamp
TEST_CASES_AGGREGATES = [
    pytest.param(
        SET_MESSAGE_SHARED,
        [
            {
                "org_id": _literal(1),
                "project_id": _literal(2),
                "metric_id": _literal(1232341),
                "timestamp": _call(
                    "toDateTime", (_literal(MOCK_TIME_BUCKET.isoformat()),)
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
            }
            for granularity in MetricsAggregateProcessor.GRANULARITIES_SECONDS
        ],
        None,
        None,
        id="Simple set with valid content",
    ),
    pytest.param(
        COUNTER_MESSAGE_SHARED,
        None,
        [
            {
                "org_id": _literal(1),
                "project_id": _literal(2),
                "metric_id": _literal(1232341),
                "timestamp": _call(
                    "toDateTime", (_literal(MOCK_TIME_BUCKET.isoformat()),)
                ),
                "tags.key": _array_literal([10, 20, 30]),
                "tags.value": _array_literal([11, 22, 33]),
                "value": _call(
                    "arrayReduce", (_literal("sumState"), _array_literal([123.123]))
                ),
                "retention_days": _literal(30),
                "granularity": _literal(granularity),
            }
            for granularity in MetricsAggregateProcessor.GRANULARITIES_SECONDS
        ],
        None,
        id="Simple counter with valid content",
    ),
    pytest.param(
        DIST_MESSAGE_SHARED,
        None,
        None,
        [
            {
                "org_id": _literal(1),
                "project_id": _literal(2),
                "metric_id": _literal(1232341),
                "timestamp": _call(
                    "toDateTime", (_literal(MOCK_TIME_BUCKET.isoformat()),)
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
                    "arrayReduce",
                    (
                        _literal("minState"),
                        _array_literal([324.12]),
                    ),
                ),
                "max": _call(
                    "arrayReduce",
                    (
                        _literal("maxState"),
                        _array_literal([567567]),
                    ),
                ),
                "avg": _call(
                    "arrayReduce",
                    (
                        _literal("avgState"),
                        _array_literal(DIST_VALUES),
                    ),
                ),
                "sum": _call(
                    "arrayReduce",
                    (
                        _literal("sumState"),
                        _array_literal([sum(DIST_VALUES)]),
                    ),
                ),
                "count": _call(
                    "arrayReduce",
                    (
                        _literal("countState"),
                        _array_literal([float(len(DIST_VALUES))]),
                    ),
                ),
                "retention_days": _literal(90),
                "granularity": _literal(granularity),
            }
            for granularity in MetricsAggregateProcessor.GRANULARITIES_SECONDS
        ],
        id="Simple distribution with valid content",
    ),
]


def test_time_bucketing() -> None:
    # Verified these output timestamps as rounding down properly
    # from today's date at time of writing
    base_timestamp = 1644349789
    base_datetime = datetime.fromtimestamp(base_timestamp)

    ten_s_bucket = timestamp_to_bucket(base_datetime, 10)
    assert ten_s_bucket.timestamp() == 1644349780

    one_min_bucket = timestamp_to_bucket(base_datetime, 60)
    assert one_min_bucket.timestamp() == 1644349740

    one_hour_bucket = timestamp_to_bucket(base_datetime, 3600)
    assert one_hour_bucket.timestamp() == 1644346800

    one_day_bucket = timestamp_to_bucket(base_datetime, 86400)
    assert one_day_bucket.timestamp() == 1644278400


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
    # test_time_bucketing tests the bucket function, parameterizing the output times here
    # would require repeating the code in the class we're testing
    with patch(
        "snuba.datasets.metrics_aggregate_processor.timestamp_to_bucket",
        lambda _, __: MOCK_TIME_BUCKET,
    ):
        assert (
            SetsAggregateProcessor().process_message(message, meta)
            == expected_set_result
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


TEST_CASES_POLYMORPHIC = [
    pytest.param(
        SET_MESSAGE_SHARED,
        [
            {
                "org_id": 1,
                "project_id": 2,
                "metric_id": 1232341,
                "timestamp": expected_timestamp,
                "tags.key": [10, 20, 30],
                "tags.value": [11, 22, 33],
                "metric_type": "set",
                "set_values": [324234, 345345, 456456, 567567],
                "materialization_version": MATERIALIZATION_VERSION,
                "retention_days": 30,
                "partition": 1,
                "offset": 100,
            }
        ],
    ),
    pytest.param(
        COUNTER_MESSAGE_SHARED,
        [
            {
                "org_id": 1,
                "project_id": 2,
                "metric_id": 1232341,
                "timestamp": expected_timestamp,
                "tags.key": [10, 20, 30],
                "tags.value": [11, 22, 33],
                "metric_type": "counter",
                "count_value": 123.123,
                "materialization_version": MATERIALIZATION_VERSION,
                "retention_days": 30,
                "partition": 1,
                "offset": 100,
            }
        ],
    ),
    pytest.param(
        DIST_MESSAGE_SHARED,
        [
            {
                "org_id": 1,
                "project_id": 2,
                "metric_id": 1232341,
                "timestamp": expected_timestamp,
                "tags.key": [10, 20, 30],
                "tags.value": [11, 22, 33],
                "metric_type": "distribution",
                "distribution_values": [324.12, 345.23, 4564.56, 567567],
                "materialization_version": MATERIALIZATION_VERSION,
                "retention_days": 90,
                "partition": 1,
                "offset": 100,
            }
        ],
    ),
]


@pytest.mark.parametrize(
    "message, expected_output",
    TEST_CASES_POLYMORPHIC,
)
def test_metrics_polymorphic_processor(
    message: Mapping[str, Any],
    expected_output: Optional[Sequence[Mapping[str, Any]]],
) -> None:
    settings.DISABLED_DATASETS = set()

    meta = KafkaMessageMetadata(offset=100, partition=1, timestamp=datetime(1970, 1, 1))
    # test_time_bucketing tests the bucket function, parameterizing the output times here
    # would require repeating the code in the class we're testing
    with patch(
        "snuba.datasets.metrics_aggregate_processor.timestamp_to_bucket",
        lambda _, __: MOCK_TIME_BUCKET,
    ):
        expected_polymorphic_result = (
            AggregateInsertBatch(expected_output, None)
            if expected_output is not None
            else None
        )
        assert (
            PolymorphicMetricsProcessor().process_message(message, meta)
            == expected_polymorphic_result
        )


TEST_CASES_GENERIC = [
    pytest.param(
        SET_MESSAGE_SHARED,
        [
            {
                "use_case_id": "release-health",
                "org_id": 1,
                "project_id": 2,
                "metric_id": 1232341,
                "timestamp": expected_timestamp,
                "tags.key": [10, 20, 30],
                "tags.indexed_value": [11, 22, 33],
                "tags.raw_value": ["value-1", "value-2", "value-3"],
                "metric_type": "set",
                "set_values": [324234, 345345, 456456, 567567],
                "materialization_version": 1,
                "timeseries_id": ANY,
                "retention_days": 30,
                "granularities": [1, 2, 3],
            }
        ],
        id="all tag values ints",
    ),
    pytest.param(
        SET_MESSAGE_TAG_VALUES_STRINGS,
        [
            {
                "use_case_id": "release-health",
                "org_id": 1,
                "project_id": 2,
                "metric_id": 1232341,
                "timestamp": expected_timestamp,
                "tags.key": [10, 20, 30],
                "tags.indexed_value": [0, 0, 0],
                "tags.raw_value": ["value-1", "value-2", "value-3"],
                "metric_type": "set",
                "set_values": [324234, 345345, 456456, 567567],
                "materialization_version": 1,
                "timeseries_id": ANY,
                "retention_days": 30,
                "granularities": [1, 2, 3],
            }
        ],
        id="all tag values strings",
    ),
    pytest.param(
        SET_MESSAGE_TAG_VALUES_MIXED_STRINGS_INTS,
        [
            {
                "use_case_id": "release-health",
                "org_id": 1,
                "project_id": 2,
                "metric_id": 1232341,
                "timestamp": expected_timestamp,
                "tags.key": [10, 20, 30],
                "tags.indexed_value": [0, 22, 33],
                "tags.raw_value": ["value-1", "value-2", "value-3"],
                "metric_type": "set",
                "set_values": [324234, 345345, 456456, 567567],
                "materialization_version": 1,
                "timeseries_id": ANY,
                "retention_days": 30,
                "granularities": [1, 2, 3],
            }
        ],
        id="mixed tag values",
    ),
]


@pytest.mark.parametrize(
    "message, expected_output",
    TEST_CASES_GENERIC,
)
def test_generic_metrics_sets_processor(
    message: Mapping[str, Any], expected_output: Optional[Sequence[Mapping[str, Any]]]
) -> None:
    meta = KafkaMessageMetadata(offset=100, partition=1, timestamp=datetime(1970, 1, 1))

    expected_polymorphic_result = (
        InsertBatch(expected_output, None) if expected_output is not None else None
    )
    assert (
        GenericSetsMetricsProcessor().process_message(message, meta)
        == expected_polymorphic_result
    )
