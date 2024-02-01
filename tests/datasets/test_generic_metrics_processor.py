from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Tuple

import pytest

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.metrics_messages import InputType
from snuba.datasets.processors.generic_metrics_processor import (
    GenericDistributionsMetricsProcessor,
    GenericSetsMetricsProcessor,
)

timestamp = int(datetime.now(timezone.utc).timestamp())

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

BASE_MESSAGE = {
    "use_case_id": "release-health",
    "org_id": 1,
    "project_id": 2,
    "metric_id": 1232341,
    "type": "s",
    "timestamp": timestamp,
    "tags": {"10": 11, "20": 22, "30": 33},
    "value": [324234, 345345, 456456, 567567],
    "retention_days": 22,
    "mapping_meta": MAPPING_META_COMMON,
}


@pytest.fixture
def processor() -> GenericSetsMetricsProcessor:
    return GenericSetsMetricsProcessor()


@pytest.fixture
def dis_processor() -> GenericDistributionsMetricsProcessor:
    return GenericDistributionsMetricsProcessor()


def sorted_tag_items(message: Mapping[str, Any]) -> Iterable[Tuple[str, int]]:
    tags = message["tags"]
    return sorted(tags.items())


def test_aggregation_option_is_converted_to_column(
    dis_processor: GenericDistributionsMetricsProcessor,
) -> None:
    message = {
        "use_case_id": "performance",
        "org_id": 1,
        "project_id": 2,
        "metric_id": 9223372036854775910,
        "type": InputType.DISTRIBUTION.value,
        "timestamp": timestamp,
        "tags": {"10": "11", "20": "22", "30": "33"},
        "value": [4, 5, 6],
        "retention_days": 22,
        "mapping_meta": MAPPING_META_COMMON,
        "sentry_received_timestamp": 1234,
    }
    message["aggregation_option"] = "hist"

    metadata = KafkaMessageMetadata(0, 0, datetime.now())
    insert_batch = dis_processor.process_message(message, metadata)

    assert insert_batch.rows[0]["enable_histogram"] == 1
