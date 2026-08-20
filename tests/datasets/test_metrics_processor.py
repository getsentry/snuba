from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any
from unittest.mock import ANY

import pytest

from snuba import settings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors.metrics_bucket_processor import (
    PolymorphicMetricsProcessor,
)
from snuba.processor import InsertBatch

MATERIALIZATION_VERSION = 4

timestamp = int(datetime.now(UTC).timestamp())
# expects that test is run in utc local time
intermediate_timestamp = datetime.utcfromtimestamp(timestamp)
expected_timestamp = int(intermediate_timestamp.replace(tzinfo=UTC).timestamp())

sentry_received_timestamp = datetime.now(UTC).timestamp()
expected_sentry_received_timestamp = datetime.utcfromtimestamp(sentry_received_timestamp)

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
    # test enforce retention days of 30 (50 quantizes down to 30)
    "retention_days": 50,
    "mapping_meta": MAPPING_META_COMMON,
    "sentry_received_timestamp": sentry_received_timestamp,
}

MOCK_TIME_BUCKET = expected_timestamp


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
                "retention_days": 30,
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
    expected_output: Sequence[Mapping[str, Any]] | None,
) -> None:
    settings.DISABLED_DATASETS = set()

    meta = KafkaMessageMetadata(offset=100, partition=1, timestamp=datetime(1970, 1, 1))
    output = PolymorphicMetricsProcessor().process_message(message, meta)
    assert isinstance(output, InsertBatch)
    assert output.rows == expected_output
