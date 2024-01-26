from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional, Sequence, Tuple
from unittest.mock import ANY, patch

import pytest

from snuba import settings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors.generic_metrics_processor import (
    GenericSetsMetricsProcessor,
)
from snuba.datasets.processors.metrics_bucket_processor import (
    MetricsBucketProcessor,
    PolymorphicMetricsProcessor,
    timestamp_to_bucket,
)
from snuba.processor import AggregateInsertBatch, InsertBatch

MATERIALIZATION_VERSION = 4

timestamp = int(datetime.now(timezone.utc).timestamp())
# expects that test is run in utc local time
intermediate_timestamp = datetime.utcfromtimestamp(timestamp)
expected_timestamp = int(
    intermediate_timestamp.replace(tzinfo=timezone.utc).timestamp()
)

sentry_received_timestamp = datetime.now(timezone.utc).timestamp()
expected_sentry_received_timestamp = datetime.utcfromtimestamp(
    sentry_received_timestamp
)

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

SET_MESSAGE_SHARED = {
    "use_case_id": "sessions",
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
    "sentry_received_timestamp": sentry_received_timestamp,
}

SET_MESSAGE_TAG_VALUES_STRINGS = {
    "version": 2,
    "use_case_id": "sessions",
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
    "sentry_received_timestamp": sentry_received_timestamp,
}

COUNTER_MESSAGE_SHARED = {
    "use_case_id": "sessions",
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
    "sentry_received_timestamp": sentry_received_timestamp,
}

DIST_VALUES = [324.12, 345.23, 4564.56, 567567]
DIST_MESSAGE_SHARED = {
    "use_case_id": "sessions",
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
    "sentry_received_timestamp": sentry_received_timestamp,
}

MOCK_TIME_BUCKET = expected_timestamp


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


TEST_CASES_POLYMORPHIC = [
    pytest.param(
        SET_MESSAGE_SHARED,
        [
            {
                "org_id": 1,
                "project_id": 2,
                "metric_id": 1232341,
                "use_case_id": "sessions",
                "timestamp": expected_timestamp,
                "tags.key": [10, 20, 30],
                "tags.value": [11, 22, 33],
                "metric_type": "set",
                "set_values": [324234, 345345, 456456, 567567],
                "count_value": None,
                "distribution_values": None,
                "materialization_version": MATERIALIZATION_VERSION,
                "retention_days": 30,
                "timeseries_id": ANY,
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
                "use_case_id": "sessions",
                "timestamp": expected_timestamp,
                "tags.key": [10, 20, 30],
                "tags.value": [11, 22, 33],
                "metric_type": "counter",
                "count_value": 123.123,
                "distribution_values": None,
                "set_values": None,
                "materialization_version": MATERIALIZATION_VERSION,
                "retention_days": 30,
                "timeseries_id": ANY,
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
                "use_case_id": "sessions",
                "timestamp": expected_timestamp,
                "tags.key": [10, 20, 30],
                "tags.value": [11, 22, 33],
                "metric_type": "distribution",
                "distribution_values": [324.12, 345.23, 4564.56, 567567.0],
                "count_value": None,
                "set_values": None,
                "materialization_version": MATERIALIZATION_VERSION,
                "retention_days": 90,
                "timeseries_id": ANY,
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
        "snuba.datasets.processors.metrics_bucket_processor.timestamp_to_bucket",
        lambda _, __: MOCK_TIME_BUCKET,
    ):
        expected_polymorphic_result = (
            AggregateInsertBatch(
                expected_output, None, expected_sentry_received_timestamp
            )
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
                "use_case_id": "sessions",
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
                "min_retention_days": 30,
            }
        ],
        id="all tag values ints",
    ),
    pytest.param(
        SET_MESSAGE_TAG_VALUES_STRINGS,
        [
            {
                "use_case_id": "sessions",
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
                "min_retention_days": 30,
            }
        ],
        id="all tag values strings",
    ),
]


@pytest.mark.parametrize(
    "message, expected_output",
    [
        pytest.param(
            SET_MESSAGE_SHARED,
            [
                {
                    "use_case_id": "sessions",
                    "org_id": 1,
                    "project_id": 2,
                    "metric_id": 1232341,
                    "timestamp": timestamp,
                    "tags.key": [10, 20, 30],
                    "tags.indexed_value": [11, 22, 33],
                    "tags.raw_value": ["value-1", "value-2", "value-3"],
                    "metric_type": "set",
                    "set_values": [324234, 345345, 456456, 567567],
                    "materialization_version": 2,
                    "timeseries_id": 1521156896,
                    "retention_days": 30,
                    "granularities": [1, 2, 3],
                    "min_retention_days": 30,
                }
            ],
            id="all tag values ints",
        ),
        pytest.param(
            SET_MESSAGE_TAG_VALUES_STRINGS,
            [
                {
                    "use_case_id": "sessions",
                    "org_id": 1,
                    "project_id": 2,
                    "metric_id": 1232341,
                    "timestamp": timestamp,
                    "tags.key": [10, 20, 30],
                    "tags.indexed_value": [0, 0, 0],
                    "tags.raw_value": ["value-1", "value-2", "value-3"],
                    "metric_type": "set",
                    "set_values": [324234, 345345, 456456, 567567],
                    "materialization_version": 2,
                    "timeseries_id": 3019115090,
                    "retention_days": 30,
                    "granularities": [1, 2, 3],
                    "min_retention_days": 30,
                }
            ],
            id="all tag values strings",
        ),
    ],
)
def test_generic_metrics_sets_processor(
    message: Mapping[str, Any], expected_output: Optional[Sequence[Mapping[str, Any]]]
) -> None:
    meta = KafkaMessageMetadata(offset=100, partition=1, timestamp=datetime(1970, 1, 1))

    expected_polymorphic_result = (
        InsertBatch(expected_output, None, expected_sentry_received_timestamp)
        if expected_output is not None
        else None
    )
    assert (
        GenericSetsMetricsProcessor().process_message(message, meta)
        == expected_polymorphic_result
    )


@pytest.fixture
def processor() -> MetricsBucketProcessor:
    return GenericSetsMetricsProcessor()


def sorted_tag_items(message: Mapping[str, Any]) -> Iterable[Tuple[str, int]]:
    tags = message["tags"]
    return sorted(tags.items())


def assert_invariant_timeseries_id(
    processor: MetricsBucketProcessor,
    m1: Mapping[str, Any],
    m2: Mapping[str, Any],
) -> None:
    token = processor._timeseries_id_token(m1, sorted_tag_items(m1))
    token2 = processor._timeseries_id_token(m2, sorted_tag_items(m2))
    assert token == token2


def assert_variant_timeseries_id(
    processor: MetricsBucketProcessor,
    m1: Mapping[str, Any],
    m2: Mapping[str, Any],
) -> None:
    token = processor._timeseries_id_token(m1, sorted_tag_items(m1))
    token2 = processor._timeseries_id_token(m2, sorted_tag_items(m2))
    assert token != token2


def test_timeseries_id_token_is_deterministic(
    processor: MetricsBucketProcessor,
) -> None:
    assert_invariant_timeseries_id(processor, SET_MESSAGE_SHARED, SET_MESSAGE_SHARED)


def test_timeseries_id_token_varies_with_org_id(
    processor: MetricsBucketProcessor,
) -> None:
    message_2 = deepcopy(SET_MESSAGE_SHARED)
    message_2["org_id"] = 2

    assert_variant_timeseries_id(processor, SET_MESSAGE_SHARED, message_2)


def test_timeseries_id_token_varies_with_metric_id(
    processor: MetricsBucketProcessor,
) -> None:
    message_2 = deepcopy(SET_MESSAGE_SHARED)
    message_2["metric_id"] = 5

    assert_variant_timeseries_id(processor, SET_MESSAGE_SHARED, message_2)


def test_timeseries_id_token_varies_with_indexed_tag_values(
    processor: MetricsBucketProcessor,
) -> None:
    message_2 = deepcopy(SET_MESSAGE_SHARED)
    message_2["tags"]["10"] = 666

    assert_variant_timeseries_id(processor, SET_MESSAGE_SHARED, message_2)


def test_timeseries_id_token_invariant_to_raw_tag_values(
    processor: MetricsBucketProcessor,
) -> None:
    message_2 = deepcopy(SET_MESSAGE_SHARED)
    message_2["mapping_meta"]["c"]["10"] = "a-new-tag-value"

    assert_invariant_timeseries_id(processor, SET_MESSAGE_SHARED, message_2)
