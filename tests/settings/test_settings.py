from copy import deepcopy
from typing import Any, Dict

import pytest

from snuba import settings
from snuba.settings.validation import InvalidTopicError, validate_settings
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


def test_validation_catches_bad_partition_mapping() -> None:
    all_settings = build_settings_dict()

    assert all_settings["LOCAL_SLICES"] == 1
    part_mapping = all_settings["LOGICAL_PARTITION_MAPPING"]
    part_mapping["2"] = 1  # only slice 0 is valid if LOCAL_PHYSICAL_PARTITIONS is = 1

    with pytest.raises(AssertionError):
        validate_settings(all_settings)
    part_mapping["2"] = 0


def test_validation_catches_unmapped_logical_parts() -> None:
    all_settings = build_settings_dict()

    part_mapping = all_settings["LOGICAL_PARTITION_MAPPING"]
    del part_mapping["2"]

    with pytest.raises(AssertionError):
        validate_settings(all_settings)
    part_mapping["2"] = 0
