from __future__ import annotations

import pytest
from jsonschema import validate
from jsonschema.exceptions import ValidationError

from snuba.datasets.configuration.json_schema import V1_READABLE_STORAGE_SCHEMA
from snuba.datasets.configuration.storage_builder import get_storage
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import (
    ReadableTableStorage,
    WritableStorage,
    WritableTableStorage,
)
from snuba.datasets.storages.generic_metrics import (
    distributions_bucket_storage as distributions_bucket_storage_old,
)
from snuba.datasets.storages.generic_metrics import (
    distributions_storage as distributions_storage_old,
)
from snuba.datasets.storages.generic_metrics import (
    sets_bucket_storage as sets_bucket_storage_old,
)
from snuba.datasets.storages.generic_metrics import sets_storage as sets_storage_old


def test_distributions_storage() -> None:
    _deep_compare_storages(
        distributions_storage_old,
        get_storage(distributions_storage_old.get_storage_key()),
    )


def test_distributions_bucket_storage() -> None:
    _deep_compare_storages(
        distributions_bucket_storage_old,
        get_storage(distributions_bucket_storage_old.get_storage_key()),
    )


def test_sets_bucket_storage() -> None:
    _deep_compare_storages(
        sets_bucket_storage_old, get_storage(sets_bucket_storage_old.get_storage_key())
    )


def test_sets_storage() -> None:
    _deep_compare_storages(
        sets_storage_old, get_storage(sets_storage_old.get_storage_key())
    )


def test_invalid_storage() -> None:
    config = {
        "storage": {"key": 1, "set_key": "x"},
        "schema": {"columns": []},
        "query_processors": [],
    }
    with pytest.raises(ValidationError) as e:
        validate(config, V1_READABLE_STORAGE_SCHEMA)
    assert e.value.message == "1 is not of type 'string'"


def test_invalid_query_processor() -> None:
    config = {
        "storage": {"key": "x", "set_key": "x"},
        "schema": {"columns": []},
        "query_processors": [5],
    }
    with pytest.raises(ValidationError) as e:
        validate(config, V1_READABLE_STORAGE_SCHEMA)
    assert e.value.message == "5 is not of type 'string'"


def _deep_compare_storages(
    old: ReadableTableStorage | WritableTableStorage,
    new: ReadableTableStorage | WritableTableStorage,
) -> None:
    """
    temp function to compare storages
    """
    assert (
        old.get_cluster().get_clickhouse_cluster_name()
        == new.get_cluster().get_clickhouse_cluster_name()
    )
    assert (
        old.get_cluster().get_storage_set_keys()
        == new.get_cluster().get_storage_set_keys()
    )
    assert (
        old.get_mandatory_condition_checkers() == new.get_mandatory_condition_checkers()
    )
    assert len(old.get_query_processors()) == len(new.get_query_processors())
    assert old.get_query_splitters() == new.get_query_splitters()
    schema_old, schema_new = old.get_schema(), new.get_schema()
    assert schema_old.get_columns().columns == schema_new.get_columns().columns

    if isinstance(schema_old, TableSchema) and isinstance(schema_new, TableSchema):
        assert schema_old.get_table_name() == schema_new.get_table_name()

    if isinstance(old, WritableStorage) and isinstance(new, WritableStorage):
        assert (
            old.get_table_writer().get_schema().get_columns()
            == new.get_table_writer().get_schema().get_columns()
        )
