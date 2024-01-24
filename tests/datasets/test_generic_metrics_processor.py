from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Tuple
from unittest import mock

import pytest
from usageaccountant import UsageUnit

from snuba import settings
from snuba.datasets.metrics_messages import InputType
from snuba.datasets.processors.generic_metrics_processor import (
    GenericDistributionsMetricsProcessor,
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
    "retention_days": 22,
    "mapping_meta": MAPPING_META_COMMON,
}


@pytest.fixture
def processor() -> GenericMetricsBucketProcessor:
    return GenericSetsMetricsProcessor()


@pytest.fixture
def dis_processor() -> GenericDistributionsMetricsProcessor:
    return GenericDistributionsMetricsProcessor()


def sorted_tag_items(message: Mapping[str, Any]) -> Iterable[Tuple[str, int]]:
    tags = message["tags"]
    return sorted(tags.items())


def assert_invariant_timeseries_id(
    processor: GenericMetricsBucketProcessor,
    m1: Mapping[str, Any],
    m2: Mapping[str, Any],
) -> None:
    token = processor._timeseries_id_token(m1, sorted_tag_items(m1))
    token2 = processor._timeseries_id_token(m2, sorted_tag_items(m2))
    assert token == token2


def assert_variant_timeseries_id(
    processor: GenericMetricsBucketProcessor,
    m1: Mapping[str, Any],
    m2: Mapping[str, Any],
) -> None:
    token = processor._timeseries_id_token(m1, sorted_tag_items(m1))
    token2 = processor._timeseries_id_token(m2, sorted_tag_items(m2))
    assert token != token2


def test_timeseries_id_token_is_deterministic(
    processor: GenericMetricsBucketProcessor,
) -> None:
    assert_invariant_timeseries_id(processor, BASE_MESSAGE, BASE_MESSAGE)


def test_timeseries_id_token_varies_with_org_id(
    processor: GenericMetricsBucketProcessor,
) -> None:
    message_2 = deepcopy(BASE_MESSAGE)
    message_2["org_id"] = 2

    assert_variant_timeseries_id(processor, BASE_MESSAGE, message_2)


def test_timeseries_id_token_varies_with_metric_id(
    processor: GenericMetricsBucketProcessor,
) -> None:
    message_2 = deepcopy(BASE_MESSAGE)
    message_2["metric_id"] = 5

    assert_variant_timeseries_id(processor, BASE_MESSAGE, message_2)


def test_timeseries_id_token_varies_with_indexed_tag_values(
    processor: GenericMetricsBucketProcessor,
) -> None:
    message_2 = deepcopy(BASE_MESSAGE)
    message_2["tags"]["10"] = 666

    assert_variant_timeseries_id(processor, BASE_MESSAGE, message_2)


def test_timeseries_id_token_invariant_to_raw_tag_values(
    processor: GenericMetricsBucketProcessor,
) -> None:
    message_2 = deepcopy(BASE_MESSAGE)
    message_2["mapping_meta"]["c"]["10"] = "a-new-tag-value"

    assert_invariant_timeseries_id(processor, BASE_MESSAGE, message_2)


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
        "tags": {"10": 11, "20": 22, "30": 33},
        "value": [4, 5, 6],
        "retention_days": 22,
        "mapping_meta": MAPPING_META_COMMON,
    }
    message["aggregation_option"] = "hist"

    insert_batch = dis_processor.process_message(message, None)

    assert insert_batch.rows[0]["enable_histogram"] == 1


@pytest.mark.redis_db
def test_record_cogs(dis_processor: GenericDistributionsMetricsProcessor) -> None:
    message = {
        "use_case_id": "performance",
        "org_id": 1,
        "project_id": 2,
        "metric_id": 9223372036854775910,
        "type": InputType.DISTRIBUTION.value,
        "timestamp": timestamp,
        "tags": {"10": 11, "20": 22, "30": 33},
        "value": [4, 5, 6],
        "retention_days": 22,
        "mapping_meta": MAPPING_META_COMMON,
    }
    settings.RECORD_COGS = True

    with mock.patch(
        "snuba.datasets.processors.generic_metrics_processor.record_cogs"
    ) as record_cogs:
        dis_processor.process_message(message, None)

    record_cogs.assert_called_once_with(
        resource_id="generic_metrics_processor_distributions",
        app_feature="genericmetrics_performance",
        amount=297,
        usage_type=UsageUnit.BYTES,
    )
