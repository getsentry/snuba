from __future__ import annotations

from typing import Any

from jsonschema import validate
from yaml import safe_load

from snuba.datasets.configuration.migration_json_schema import MIGRATION_GROUP_SCHEMA


def load_migration_group(path_to_file: str) -> dict[str, Any]:
    yaml_file = open(path_to_file)
    config = safe_load(yaml_file)
    assert isinstance(config, dict)
    validate(config, MIGRATION_GROUP_SCHEMA)
    return config
