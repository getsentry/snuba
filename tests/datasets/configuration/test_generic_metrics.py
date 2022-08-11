import pytest
from jsonschema import validate
from jsonschema.exceptions import ValidationError
from yaml import safe_load

from snuba.datasets.configuration.json_schema import (
    READABLE_STORAGE_SCHEMA,
    WRITABLE_STORAGE_SCHEMA,
)
from snuba.datasets.configuration.utils import parse_columns
from snuba.datasets.storages.generic_metrics import (
    aggregate_common_columns,
    aggregate_distributions_columns,
    bucket_columns,
    common_columns,
)

CONF_PATH = "./snuba/datasets/configuration/generic_metrics/storages"

DISTRIBUTIONS_STORAGE_COLUMN_SET = [
    *common_columns,
    *aggregate_common_columns,
    *aggregate_distributions_columns,
]


def test_distributions_storage() -> None:
    file = open(f"{CONF_PATH}/distributions.yaml")
    config = safe_load(file)
    validate(config, READABLE_STORAGE_SCHEMA)
    assert (
        parse_columns(config["schema"]["columns"]) == DISTRIBUTIONS_STORAGE_COLUMN_SET
    )


def test_distributions_bucket_storage() -> None:
    file = open(f"{CONF_PATH}/distributions_bucket.yaml")
    config = safe_load(file)
    validate(config, WRITABLE_STORAGE_SCHEMA)
    assert parse_columns(config["schema"]["columns"]) == [
        *common_columns,
        *bucket_columns,
    ]


def test_invalid_storage() -> None:
    config = {
        "storage": {"key": 1, "set_key": "x"},
        "schema": {"columns": []},
        "query_processors": [],
    }
    with pytest.raises(ValidationError) as e:
        validate(config, READABLE_STORAGE_SCHEMA)
    assert e.value.message == "1 is not of type 'string'"
