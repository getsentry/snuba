import pytest
from jsonschema import validate
from jsonschema.exceptions import ValidationError
from yaml import safe_load

from snuba.clickhouse.columns import AggregateFunction, Column, Float
from snuba.datasets.configuration.json_schema import (
    readable_storage_schema,
    writable_storage_schema,
)
from snuba.datasets.configuration.utils import parse_columns
from snuba.datasets.storages.generic_metrics import (
    aggregate_common_columns,
    bucket_columns,
    common_columns,
)

CONF_PATH = "./snuba/datasets/configuration/generic_metrics/storages"

DISTRIBUTIONS_STORAGE_COLUMN_SET = [
    *common_columns,
    *aggregate_common_columns,
    Column(
        "percentiles",
        AggregateFunction("quantiles(0.5, 0.75, 0.9, 0.95, 0.99)", [Float(64)]),
    ),
    Column("min", AggregateFunction("min", [Float(64)])),
    Column("max", AggregateFunction("max", [Float(64)])),
    Column("avg", AggregateFunction("avg", [Float(64)])),
    Column("sum", AggregateFunction("sum", [Float(64)])),
    Column("count", AggregateFunction("count", [Float(64)])),
    Column(
        "histogram_buckets",
        AggregateFunction("histogram(250)", [Float(64)]),
    ),
]


def test_distributions_storage() -> None:
    file = open(f"{CONF_PATH}/distributions_storage.yaml")
    config = safe_load(file)
    validate(config, readable_storage_schema)
    assert (
        parse_columns(config["schema"]["columns"]) == DISTRIBUTIONS_STORAGE_COLUMN_SET
    )


def test_distributions_bucket_storage() -> None:
    file = open(f"{CONF_PATH}/distributions_bucket_storage.yaml")
    config = safe_load(file)
    validate(config, writable_storage_schema)
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
        validate(config, readable_storage_schema)
    assert e.value.message == "1 is not of type 'string'"
