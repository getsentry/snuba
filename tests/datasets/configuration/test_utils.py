from __future__ import annotations

from typing import Callable

import pytest
from arroyo.processing.strategies.dead_letter_queue import (
    DeadLetterQueuePolicy,
    ProduceInvalidMessagePolicy,
)
from jsonschema import validate
from jsonschema.exceptions import ValidationError

from snuba.datasets.configuration.json_schema import V1_READABLE_STORAGE_SCHEMA
from snuba.datasets.configuration.storage_builder import build_stream_loader
from snuba.datasets.configuration.utils import generate_policy_creator
from snuba.datasets.message_filters import KafkaHeaderSelectFilter
from snuba.datasets.processors.generic_metrics_processor import (
    GenericSetsMetricsProcessor,
)
from snuba.subscriptions.utils import SchedulingWatermarkMode
from snuba.utils.streams.topics import Topic


def assert_valid_policy_creator(
    policy_creator: Callable[[], DeadLetterQueuePolicy] | None
) -> None:
    assert policy_creator is not None
    policy = policy_creator()
    assert isinstance(policy, ProduceInvalidMessagePolicy)
    policy.terminate()


def test_generate_policy_creator() -> None:
    assert_valid_policy_creator(
        generate_policy_creator(
            {"type": "produce", "args": [Topic.DEAD_LETTER_GENERIC_METRICS.value]}
        )
    )


def test_build_stream_loader() -> None:
    loader = build_stream_loader(
        {
            "processor": "GenericSetsMetricsProcessor",
            "default_topic": "snuba-generic-metrics",
            "pre_filter": {
                "type": "kafka_header_select_filter",
                "args": {"header_key": "metric_type", "header_value": "s"},
            },
            "commit_log_topic": "snuba-generic-metrics-sets-commit-log",
            "subscription_scheduler_mode": "global",
            "subscription_scheduled_topic": "scheduled-subscriptions-generic-metrics-sets",
            "subscription_result_topic": "generic-metrics-sets-subscription-results",
            "dlq_policy": {
                "type": "produce",
                "args": ["snuba-dead-letter-generic-metrics"],
            },
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
        and result_topic_spec.topic == Topic.SUBSCRIPTION_RESULTS_GENERIC_METRICS_SETS
    )
    assert_valid_policy_creator(loader.get_dead_letter_queue_policy_creator())


def test_invalid_storage() -> None:
    config = {
        "version": "v1",
        "kind": "readable_storage",
        "name": "",
        "storage": {"key": 1, "set_key": "x"},
        "schema": {"columns": []},
        "query_processors": [],
    }
    with pytest.raises(ValidationError) as e:
        validate(config, V1_READABLE_STORAGE_SCHEMA)
    assert e.value.message == "1 is not of type 'string'"


def test_invalid_query_processor() -> None:
    config = {
        "version": "v1",
        "kind": "readable_storage",
        "name": "",
        "storage": {"key": "x", "set_key": "x"},
        "schema": {"columns": []},
        "query_processors": [5],
    }
    with pytest.raises(ValidationError) as e:
        validate(config, V1_READABLE_STORAGE_SCHEMA)
    assert e.value.message == "5 is not of type 'object'"


def test_unexpected_key() -> None:
    config = {
        "version": "v1",
        "kind": "readable_storage",
        "name": "",
        "storage": {"key": "1", "set_key": "x"},
        "schema": {"columns": []},
        "query_processors": [],
        "extra": "",
    }
    with pytest.raises(ValidationError) as e:
        validate(config, V1_READABLE_STORAGE_SCHEMA)
    assert (
        e.value.message
        == "Additional properties are not allowed ('extra' was unexpected)"
    )


def test_missing_required_key() -> None:
    config = {
        "version": "v1",
        "name": "",
        "storage": {"key": "1", "set_key": "x"},
        "schema": {"columns": []},
        "query_processors": [],
    }
    with pytest.raises(ValidationError) as e:
        validate(config, V1_READABLE_STORAGE_SCHEMA)
    assert e.value.message == "'kind' is a required property"
