from __future__ import annotations

from typing import Any, Mapping

from jsonschema import validate
from yaml import safe_load


def load_configuration_data(
    path: str, validation_schemas: Mapping[str, Any]
) -> dict[str, Any]:
    """
    Loads a dataset configuration file from the given path

    Returns an untyped dict of dicts
    """
    file = open(path)
    config = safe_load(file)
    assert isinstance(config, dict)
    validate(config, validation_schemas)
    return config
