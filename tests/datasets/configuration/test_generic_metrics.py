import pytest
from jsonschema import validate
from jsonschema.exceptions import ValidationError

from snuba.datasets.configuration.json_schema import READABLE_STORAGE_SCHEMA
from snuba.datasets.storage import ReadableStorage, WritableStorage
from snuba.datasets.storages.generic_metrics import (
    distributions_bucket_storage as distributions_bucket_storage_old,
)
from snuba.datasets.storages.generic_metrics import (
    distributions_storage as distributions_storage_old,
)
from snuba.datasets.storages.generic_metrics import (
    sets_bucket_storage as sets_bucket_storage_old,
)
from snuba.datasets.storages.generic_metrics_from_config import (
    distributions_bucket_storage,
    distributions_storage,
    sets_bucket_storage,
)


def test_distributions_storage() -> None:
    _deep_compare_storages(distributions_storage_old, distributions_storage)


def test_distributions_bucket_storage() -> None:
    _deep_compare_storages(
        distributions_bucket_storage_old, distributions_bucket_storage
    )


def test_sets_bucket_storage() -> None:
    _deep_compare_storages(sets_bucket_storage_old, sets_bucket_storage)


def test_invalid_storage() -> None:
    config = {
        "storage": {"key": 1, "set_key": "x"},
        "schema": {"columns": []},
        "query_processors": [],
    }
    with pytest.raises(ValidationError) as e:
        validate(config, READABLE_STORAGE_SCHEMA)
    assert e.value.message == "1 is not of type 'string'"


def _deep_compare_storages(old: ReadableStorage, new: ReadableStorage) -> None:
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
    assert (
        old.get_schema().get_columns().columns == new.get_schema().get_columns().columns
    )

    if isinstance(old, WritableStorage) and isinstance(new, WritableStorage):
        assert (
            old.get_table_writer().get_schema().get_columns()
            == new.get_table_writer().get_schema().get_columns()
        )
