from __future__ import annotations

import pytest
from fastjsonschema.exceptions import JsonSchemaValueException

from snuba.datasets.configuration.json_schema import STORAGE_VALIDATORS
from snuba.datasets.configuration.storage_builder import build_stream_loader
from snuba.datasets.message_filters import KafkaHeaderSelectFilter
from snuba.datasets.processors.generic_metrics_processor import (
    GenericSetsMetricsProcessor,
)
from snuba.subscriptions.utils import SchedulingWatermarkMode
from snuba.utils.streams.topics import Topic


def test_build_stream_loader() -> None:
    loader = build_stream_loader(
        {
            "processor": {
                "name": "GenericSetsMetricsProcessor",
            },
            "default_topic": "snuba-generic-metrics",
            "pre_filter": {
                "type": "KafkaHeaderSelectFilter",
                "args": {"header_key": "metric_type", "header_value": "s"},
            },
            "commit_log_topic": "snuba-generic-metrics-sets-commit-log",
            "subscription_scheduler_mode": "global",
            "subscription_scheduled_topic": "scheduled-subscriptions-generic-metrics-sets",
            "subscription_result_topic": "generic-metrics-subscription-results",
            "dlq_topic": "snuba-dead-letter-generic-metrics",
        }
    )
    assert isinstance(loader.get_processor(), GenericSetsMetricsProcessor)
    assert loader.get_default_topic_spec().topic == Topic.GENERIC_METRICS
    assert isinstance(loader.get_pre_filter(), KafkaHeaderSelectFilter)
    commit_log_topic_spec = loader.get_commit_log_topic_spec()
    assert (
        commit_log_topic_spec is not None
        and commit_log_topic_spec.topic == Topic.GENERIC_METRICS_SETS_COMMIT_LOG
    )
    assert loader.get_subscription_scheduler_mode() == SchedulingWatermarkMode.GLOBAL
    scheduled_topic_spec = loader.get_subscription_scheduled_topic_spec()
    assert (
        scheduled_topic_spec is not None
        and scheduled_topic_spec.topic
        == Topic.SUBSCRIPTION_SCHEDULED_GENERIC_METRICS_SETS
    )
    result_topic_spec = loader.get_subscription_result_topic_spec()
    assert (
        result_topic_spec is not None
        and result_topic_spec.topic == Topic.SUBSCRIPTION_RESULTS_GENERIC_METRICS
    )
    dlq_topic_spec = loader.get_dlq_topic_spec()
    assert (
        dlq_topic_spec is not None
        and dlq_topic_spec.topic == Topic.DEAD_LETTER_GENERIC_METRICS
    )


def test_invalid_storage() -> None:
    config = {
        "version": "v1",
        "kind": "readable_storage",
        "name": "",
        "storage": {"key": 1, "set_key": "x"},
        "readiness_state": "limited",
        "schema": {"columns": []},
        "query_processors": [],
    }
    with pytest.raises(JsonSchemaValueException) as e:
        STORAGE_VALIDATORS["readable_storage"](config)
    assert e.value.message == "data.storage.key must be string"


def test_invalid_query_processor() -> None:
    config = {
        "version": "v1",
        "kind": "readable_storage",
        "name": "",
        "storage": {"key": "x", "set_key": "x"},
        "readiness_state": "limited",
        "schema": {"columns": []},
        "query_processors": [5],
    }
    with pytest.raises(JsonSchemaValueException) as e:
        STORAGE_VALIDATORS["readable_storage"](config)
    assert e.value.message == "data.query_processors[0] must be object"


def test_unexpected_key() -> None:
    config = {
        "version": "v1",
        "kind": "readable_storage",
        "name": "",
        "storage": {"key": "1", "set_key": "x"},
        "readiness_state": "limited",
        "schema": {"columns": []},
        "query_processors": [],
        "extra": "",
    }
    with pytest.raises(JsonSchemaValueException) as e:
        STORAGE_VALIDATORS["readable_storage"](config)
    assert e.value.message == "data must not contain {'extra'} properties"


def test_missing_required_key() -> None:
    config = {
        "version": "v1",
        "name": "",
        "storage": {"key": "1", "set_key": "x"},
        "readiness_state": "limited",
        "schema": {"columns": []},
        "query_processors": [],
    }
    with pytest.raises(JsonSchemaValueException) as e:
        STORAGE_VALIDATORS["readable_storage"](config)
    assert (
        e.value.message
        == "data must contain ['version', 'kind', 'name', 'storage', 'readiness_state', 'schema'] properties"
    )


def test_missing_readiness_state() -> None:
    config = {
        "version": "v1",
        "kind": "readable_storage",
        "name": "",
        "storage": {"key": "1", "set_key": "x"},
        "schema": {"columns": []},
        "query_processors": [],
    }
    with pytest.raises(JsonSchemaValueException) as e:
        STORAGE_VALIDATORS["readable_storage"](config)
    assert (
        e.value.message
        == "data must contain ['version', 'kind', 'name', 'storage', 'readiness_state', 'schema'] properties"
    )


def test_invalid_readiness_state() -> None:
    config = {
        "version": "v1",
        "kind": "readable_storage",
        "name": "",
        "storage": {"key": "1", "set_key": "x"},
        "readiness_state": "blah",
        "schema": {"columns": []},
        "query_processors": [],
    }
    with pytest.raises(JsonSchemaValueException) as e:
        STORAGE_VALIDATORS["readable_storage"](config)
    assert (
        e.value.message
        == "data.readiness_state must be one of ['limited', 'deprecate', 'partial', 'complete']"
    )
