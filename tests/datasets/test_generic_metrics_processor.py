from copy import deepcopy
from datetime import datetime, timezone

import pytest

from snuba.datasets.generic_metrics_processor import (
    GenericMetricsBucketProcessor,
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
    # test enforce retention days of 30
    "retention_days": 22,
    "mapping_meta": MAPPING_META_COMMON,
}


@pytest.fixture
def processor() -> GenericMetricsBucketProcessor:
    return GenericSetsMetricsProcessor()


def test_timeseries_id_token_is_deterministic(
    processor: GenericMetricsBucketProcessor,
) -> None:
    token = processor._timeseries_id_token(BASE_MESSAGE)
    token2 = processor._timeseries_id_token(BASE_MESSAGE)

    assert token == token2


def test_timeseries_id_token_varies_with_org_id(
    processor: GenericMetricsBucketProcessor,
) -> None:
    message_2 = deepcopy(BASE_MESSAGE)
    message_2["org_id"] = 2

    token = processor._timeseries_id_token(BASE_MESSAGE)
    token2 = processor._timeseries_id_token(message_2)

    assert token != token2


def test_timeseries_id_token_varies_with_metric_id(
    processor: GenericMetricsBucketProcessor,
) -> None:
    message_2 = deepcopy(BASE_MESSAGE)
    message_2["metric_id"] = 5

    token = processor._timeseries_id_token(BASE_MESSAGE)
    token2 = processor._timeseries_id_token(message_2)

    assert token != token2


def test_timeseries_id_token_varies_with_indexed_tag_values(
    processor: GenericMetricsBucketProcessor,
) -> None:
    message_2 = deepcopy(BASE_MESSAGE)
    message_2["tags"]["10"] = 666

    token = processor._timeseries_id_token(BASE_MESSAGE)
    token2 = processor._timeseries_id_token(message_2)

    assert token != token2


def test_timeseries_id_token_invariant_to_raw_tag_values(
    processor: GenericMetricsBucketProcessor,
) -> None:
    message_2 = deepcopy(BASE_MESSAGE)
    message_2["mapping_meta"]["c"]["10"] = "a-new-tag-value"

    token = processor._timeseries_id_token(BASE_MESSAGE)
    token2 = processor._timeseries_id_token(message_2)

    assert token == token2
