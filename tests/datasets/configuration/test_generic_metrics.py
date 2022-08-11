import pytest
from jsonschema import validate
from jsonschema.exceptions import ValidationError

from snuba.datasets.configuration.json_schema import READABLE_STORAGE_SCHEMA
from snuba.datasets.configuration.utils import load_storage_config, parse_columns
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.generic_metrics import (
    aggregate_common_columns,
    aggregate_distributions_columns,
    bucket_columns,
    common_columns,
)

DISTRIBUTIONS_STORAGE_COLUMN_SET = [
    *common_columns,
    *aggregate_common_columns,
    *aggregate_distributions_columns,
]


def test_distributions_storage() -> None:
    config = load_storage_config(StorageKey.GENERIC_METRICS_DISTRIBUTIONS)
    assert (
        parse_columns(config["schema"]["columns"]) == DISTRIBUTIONS_STORAGE_COLUMN_SET
    )


def test_distributions_bucket_storage() -> None:
    config = load_storage_config(StorageKey.GENERIC_METRICS_DISTRIBUTIONS_RAW)
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
