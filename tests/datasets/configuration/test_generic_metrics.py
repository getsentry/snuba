import pytest
from jsonschema import validate
from jsonschema.exceptions import ValidationError
from yaml import safe_load

from snuba.datasets.configuration.json_schema import (
    readable_storage_schema,
    writable_storage_schema,
)

CONF_PATH = "./snuba/datasets/configuration/generic_metrics/storages"


def test_distributions_storage() -> None:
    file = open(f"{CONF_PATH}/distributions_storage.yaml")
    config = safe_load(file)
    validate(config, readable_storage_schema)


def test_distributions_bucket_storage() -> None:
    file = open(f"{CONF_PATH}/distributions_bucket_storage.yaml")
    config = safe_load(file)
    validate(config, writable_storage_schema)


def test_invalid_storage() -> None:
    config = {
        "storage": {"key": 1, "set_key": "x"},
        "schema": {"columns": []},
        "query_processors": [],
    }
    with pytest.raises(ValidationError) as e:
        validate(config, readable_storage_schema)
    assert e.value.message == "1 is not of type 'string'"
