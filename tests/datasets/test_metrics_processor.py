from datetime import datetime
from typing import Any, Mapping, Optional, Sequence

import pytest

from snuba import settings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.metrics_processor import MetricsProcessor
from snuba.processor import InsertBatch, json_encode_insert_batch

TEST_CASES = [
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
                "metric_type": "set",
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
        id="Simple set with valid content",
    ),
    pytest.param(
        {
            "org_id": 1,
            "project_id": 2,
            "metric_id": 1232341,
            "type": "d",
            "timestamp": 1619225296,
            "tags": {"10": 11, "20": 22, "30": 33},
            "value": [324234, 345345, 456456, 567567],
            "retention_days": 30,
        },
        None,
        id="Unsupported metric type. Ignored",
    ),
]


@pytest.mark.parametrize("message, expected", TEST_CASES)
def test_metrics_processor(
    message: Mapping[str, Any], expected: Optional[Sequence[Mapping[str, Any]]]
) -> None:
    settings.DISABLED_DATASETS = set()

    meta = KafkaMessageMetadata(offset=100, partition=1, timestamp=datetime(1970, 1, 1))

    expected_result = (
        json_encode_insert_batch(InsertBatch(expected, None))
        if expected is not None
        else None
    )

    assert MetricsProcessor().process_message(message, meta) == expected_result
