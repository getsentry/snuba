import importlib
from typing import Any, Dict
from unittest.mock import patch

import pytest

from snuba import settings
from snuba.datasets.storages.factory import _storage_factory
from snuba.datasets.storages.validation import validate_topics_with_settings
from snuba.settings import validation
from snuba.settings.validation import (
    InvalidTopicError,
    validate_settings,
    validate_slicing_settings,
)


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


def test_topics_sync_with_settings() -> None:
    try:
        _storage_factory()
        validate_topics_with_settings()
    except InvalidTopicError:
        pytest.fail("Defined Kafka Topics are not in sync with topic names in settings")


@patch("snuba.datasets.partitioning.SENTRY_LOGICAL_PARTITIONS", 2)
def test_validation_catches_bad_partition_mapping() -> None:
    importlib.reload(validation)
    all_settings = build_settings_dict()

    sliced_storages = all_settings["SLICED_STORAGES"]
    sliced_storages["events"] = 2

    part_mapping = all_settings["LOGICAL_PARTITION_MAPPING"]
    part_mapping["events"] = {0: 2, 1: 0}
    # only slices 0 and 1 are valid in this case
    # since events has 2 slices only

    with pytest.raises(AssertionError):
        validate_slicing_settings(all_settings)

    del part_mapping["events"]
    del sliced_storages["events"]


@patch("snuba.datasets.partitioning.SENTRY_LOGICAL_PARTITIONS", 2)
def test_validation_catches_unmapped_logical_parts() -> None:
    importlib.reload(validation)
    all_settings = build_settings_dict()

    sliced_storages = all_settings["SLICED_STORAGES"]
    sliced_storages["events"] = 2

    part_mapping = all_settings["LOGICAL_PARTITION_MAPPING"]
    part_mapping["events"] = {0: 1, 1: 0}
    del part_mapping["events"][1]

    with pytest.raises(AssertionError):
        validate_slicing_settings(all_settings)

    del part_mapping["events"]
    del sliced_storages["events"]


@patch("snuba.datasets.partitioning.SENTRY_LOGICAL_PARTITIONS", 2)
def test_validation_catches_empty_slice_mapping() -> None:
    importlib.reload(validation)
    all_settings = build_settings_dict()

    sliced_storages = all_settings["SLICED_STORAGES"]
    sliced_storages["events"] = 2

    # We forgot to add logical:slice mapping for events

    with pytest.raises(AssertionError):
        validate_slicing_settings(all_settings)

    del sliced_storages["events"]


def test_validation_catches_unmapped_topic_pair() -> None:
    importlib.reload(validation)
    all_settings = build_settings_dict()

    # We forgot to add broker config for the (topic, slice id) pair
    sliced_topics = all_settings["SLICED_KAFKA_TOPIC_MAP"]
    sliced_topics[("events", 1)] = "events-1"

    with pytest.raises(AssertionError):
        validate_slicing_settings(all_settings)

    del sliced_topics[("events", 1)]
