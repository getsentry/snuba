import importlib
from copy import deepcopy
from typing import Any, Dict
from unittest.mock import patch

import pytest

from snuba import settings
from snuba.settings import validation
from snuba.settings.validation import (
    InvalidTopicError,
    validate_settings,
    validate_slicing_settings,
)
from snuba.utils.streams.topics import Topic


def build_settings_dict() -> Dict[str, Any]:
    # Build a dictionary with all variables defined in settings.
    all_settings = {
        key: value
        for key, value in settings.__dict__.items()
        if not key.startswith("__") and not callable(key)
    }

    return all_settings


def test_invalid_storage() -> None:
    all_settings = build_settings_dict()

    cluster = all_settings["CLUSTERS"]
    cluster[0]["storage_sets"].add("non_existing_storage")
    try:
        validate_settings(all_settings)
    except Exception as exc:
        assert False, f"'validate_settings' raised an exception {exc}"
    finally:
        cluster[0]["storage_sets"].remove("non_existing_storage")


def test_topics_sync_in_settings_validator() -> None:
    all_settings = build_settings_dict()
    # Make a copy of the default Kafka topic map from settings
    default_map = deepcopy(all_settings["KAFKA_TOPIC_MAP"])
    # Overwrite topic map temporarily to include all defined topic names
    all_settings["KAFKA_TOPIC_MAP"] = {t.value: {} for t in Topic}

    # Validate settings with the new topic map to check
    # whether all defined topic names correspond to the
    # topic names in the settings validator
    try:
        validate_settings(all_settings)
    except InvalidTopicError:
        pytest.fail(
            "Defined Kafka Topics are not in sync with topic names in validator"
        )
    # Restore the default settings Kafka topic map
    finally:
        all_settings["KAFKA_TOPIC_MAP"] = default_map


@patch("snuba.datasets.slicing.SENTRY_LOGICAL_PARTITIONS", 2)
def test_validation_catches_bad_partition_mapping() -> None:
    importlib.reload(validation)
    all_settings = build_settings_dict()

    sliced_storage_sets = all_settings["SLICED_STORAGE_SETS"]
    sliced_storage_sets["events"] = 2

    part_mapping = all_settings["LOGICAL_PARTITION_MAPPING"]
    part_mapping["events"] = {0: 2, 1: 0}
    # only slices 0 and 1 are valid in this case
    # since events has 2 slices only

    with pytest.raises(AssertionError):
        validate_slicing_settings(all_settings)

    del part_mapping["events"]
    del sliced_storage_sets["events"]


@patch("snuba.datasets.slicing.SENTRY_LOGICAL_PARTITIONS", 2)
def test_validation_catches_unmapped_logical_parts() -> None:
    importlib.reload(validation)
    all_settings = build_settings_dict()

    sliced_storage_sets = all_settings["SLICED_STORAGE_SETS"]
    sliced_storage_sets["events"] = 2

    part_mapping = all_settings["LOGICAL_PARTITION_MAPPING"]
    part_mapping["events"] = {0: 1, 1: 0}
    del part_mapping["events"][1]

    with pytest.raises(AssertionError):
        validate_slicing_settings(all_settings)

    del part_mapping["events"]
    del sliced_storage_sets["events"]


@patch("snuba.datasets.slicing.SENTRY_LOGICAL_PARTITIONS", 2)
def test_validation_catches_empty_slice_mapping() -> None:
    importlib.reload(validation)
    all_settings = build_settings_dict()

    sliced_storage_sets = all_settings["SLICED_STORAGE_SETS"]
    sliced_storage_sets["events"] = 2

    # We forgot to add logical:slice mapping for events

    with pytest.raises(AssertionError):
        validate_slicing_settings(all_settings)

    del sliced_storage_sets["events"]


def test_validation_catches_unmapped_topic_pair() -> None:
    importlib.reload(validation)
    all_settings = build_settings_dict()

    # We forgot to add broker config for the (topic, slice id) pair
    sliced_topics = all_settings["SLICED_KAFKA_TOPIC_MAP"]
    sliced_topics[("events", 1)] = "events-1"

    with pytest.raises(AssertionError):
        validate_slicing_settings(all_settings)

    del sliced_topics[("events", 1)]


def test_sliced_kafka_broker_config() -> None:
    importlib.reload(validation)
    all_settings = build_settings_dict()

    sliced_topic_map = all_settings["SLICED_KAFKA_TOPIC_MAP"]
    sliced_topic_map[("events", 0)] = "events-0"
    sliced_topic_map[("events", 1)] = "events-1"

    default_broker_config = all_settings["BROKER_CONFIG"]
    kafka_broker_config = all_settings["KAFKA_BROKER_CONFIG"]

    # Mistakenly add a sliced topic to regular Kafka broker config
    kafka_broker_config["events"] = default_broker_config

    with pytest.raises(AssertionError):
        validate_slicing_settings(all_settings)

    del sliced_topic_map[("events", 0)]
    del sliced_topic_map[("events", 1)]
    del kafka_broker_config["events"]

    topic_map = all_settings["KAFKA_TOPIC_MAP"]
    topic_map["events"] = "events-custom"

    sliced_kafka_broker_config = all_settings["SLICED_KAFKA_BROKER_CONFIG"]

    # Mistakenly add an unsliced topic to sliced Kafka broker config
    sliced_kafka_broker_config[("events", 0)] = default_broker_config

    with pytest.raises(AssertionError):
        validate_slicing_settings(all_settings)

    del topic_map[("events")]
    del sliced_kafka_broker_config[("events", 0)]


CLUSTERS_CONFIG = [
    {
        "host": "host",
        "port": 9000,
        "user": "default",
        "password": "",
        "database": "default",
        "http_port": 8122,
        "storage_set_slices": {"generic_metrics_distributions"},
        "single_node": False,
    },
]

SLICED_CLUSTERS_CONFIG = [
    {
        "host": "host_slice",
        "port": 9000,
        "user": "default",
        "password": "",
        "database": "slice_0_default",
        "http_port": 8123,
        "storage_set_slices": {("generic_metrics_distributions", 0)},
        "single_node": True,
    },
    {
        "host": "host_slice",
        "port": 9001,
        "user": "default",
        "password": "",
        "database": "slice_1_default",
        "http_port": 8124,
        "storage_set_slices": {("generic_metrics_distributions", 1)},
        "single_node": True,
    },
]


@patch("snuba.settings.SLICED_CLUSTERS", SLICED_CLUSTERS_CONFIG)
def test_sliced_clusters() -> None:
    importlib.reload(validation)

    all_settings = build_settings_dict()
    sliced_storage_sets = all_settings["SLICED_STORAGE_SETS"]

    # All (storage set, slice id) pairs are not assigned
    # a cluster in SLICED_CLUSTERS
    sliced_storage_sets["generic_metrics_distributions"] = 3

    with pytest.raises(AssertionError):
        validate_slicing_settings(all_settings)

    del sliced_storage_sets["generic_metrics_distributions"]


@patch("snuba.settings.SLICED_CLUSTERS", SLICED_CLUSTERS_CONFIG)
@patch("snuba.settings.CLUSTERS", CLUSTERS_CONFIG)
def test_single_node_vals() -> None:
    importlib.reload(validation)

    all_settings = build_settings_dict()
    sliced_storage_sets = all_settings["SLICED_STORAGE_SETS"]
    sliced_storage_sets["generic_metrics_distributions"] = 2

    # single_node values for this storage set key
    # are not the same across all clusters
    with pytest.raises(AssertionError):
        validate_slicing_settings(all_settings)

    del sliced_storage_sets["generic_metrics_distributions"]
