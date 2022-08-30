from __future__ import annotations

from glob import glob
from typing import Any, Mapping

from jsonschema import validate
from yaml import safe_load

from snuba import settings


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
    validate(config, validation_schemas[config["kind"]])
    return config


def load_config_built_storage_keys() -> dict[str, str]:
    """
    Loads all storage keys from configuration files
    """
    return {
        key.upper(): key
        for key in [
            safe_load(open(config_file))["storage"]["key"]
            for config_file in glob(settings.STORAGE_CONFIG_FILES_GLOB, recursive=True)
        ]
    }
