import json
import os
from dataclasses import fields

from snuba import settings
from snuba.clusters.load_info import LoadInfo

SCHEMA_PATH = os.path.join(
    settings.ROOT_REPO_PATH, "sentry-options", "schemas", "snuba", "schema.json"
)


def _threshold_properties() -> dict[str, dict]:
    with open(SCHEMA_PATH) as f:
        schema = json.load(f)
    block = schema["properties"]["allocation_policies"]["additionalProperties"]["items"]
    return block["properties"]["thresholds"]["properties"]


def test_threshold_keys_are_subset_of_load_info_fields() -> None:
    """Every threshold key enumerated in schema.json must name a LoadInfo field.

    Keeps the config schema and LoadInfo in sync: the pardon path compares
    LoadInfo attributes against these thresholds, so an unknown key would be
    silently ignored (see LoadInfo.exceeds).
    """
    load_info_fields = {f.name for f in fields(LoadInfo)}
    thresholds = _threshold_properties()
    assert thresholds, "expected reject/throttle threshold groups in schema"
    for action, group in thresholds.items():
        keys = set(group["properties"])
        assert keys <= load_info_fields, (
            f"thresholds.{action} has keys not on LoadInfo: {keys - load_info_fields}"
        )


def test_all_load_info_fields_are_documented_in_schema() -> None:
    """Every LoadInfo field must be configurable as a threshold in schema.json.

    The reverse of the subset test: together they pin the schema threshold keys
    to exactly LoadInfo's fields, so a new LoadInfo field can't be silently
    unconfigurable.
    """
    load_info_fields = {f.name for f in fields(LoadInfo)}
    thresholds = _threshold_properties()
    assert thresholds, "expected reject/throttle threshold groups in schema"
    for action, group in thresholds.items():
        keys = set(group["properties"])
        assert load_info_fields <= keys, (
            f"thresholds.{action} is missing LoadInfo fields: {load_info_fields - keys}"
        )
