from __future__ import annotations

from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import ReadableTableStorage, Storage, WritableTableStorage
from snuba.datasets.storages.factory import get_config_built_storages
from snuba.datasets.storages.generic_metrics import (
    distributions_bucket_storage,
    distributions_storage,
    sets_bucket_storage,
    sets_storage,
)
from snuba.datasets.table_storage import KafkaStreamLoader

CONFIG_BUILT_STORAGES = get_config_built_storages()


def test_config_file_discovery() -> None:
    assert all(
        storage.get_storage_key() in CONFIG_BUILT_STORAGES
        for storage in [
            distributions_bucket_storage,
            distributions_storage,
            sets_bucket_storage,
            sets_storage,
        ]
    )
    assert len(CONFIG_BUILT_STORAGES) == 4


def test_distributions_storage() -> None:
    _deep_compare_storages(
        distributions_storage,
        CONFIG_BUILT_STORAGES[distributions_storage.get_storage_key()],
    )


def test_distributions_bucket_storage() -> None:
    _deep_compare_storages(
        distributions_bucket_storage,
        CONFIG_BUILT_STORAGES[distributions_bucket_storage.get_storage_key()],
    )


def test_sets_storage() -> None:
    _deep_compare_storages(
        sets_storage, CONFIG_BUILT_STORAGES[sets_storage.get_storage_key()]
    )


def test_sets_bucket_storage() -> None:
    _deep_compare_storages(
        sets_bucket_storage,
        CONFIG_BUILT_STORAGES[sets_bucket_storage.get_storage_key()],
    )


def _deep_compare_storages(old: Storage, new: Storage) -> None:
    """
    temp function to compare storages
    """
    assert isinstance(old, ReadableTableStorage)
    assert isinstance(new, ReadableTableStorage)
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

    if isinstance(old, WritableTableStorage) and isinstance(new, WritableTableStorage):
        assert (
            old.get_table_writer().get_schema().get_columns()
            == new.get_table_writer().get_schema().get_columns()
        )
        _compare_stream_loaders(
            old.get_table_writer().get_stream_loader(),
            new.get_table_writer().get_stream_loader(),
        )


def _compare_stream_loaders(old: KafkaStreamLoader, new: KafkaStreamLoader) -> None:
    assert old.get_commit_log_topic_spec() == new.get_commit_log_topic_spec()
    assert old.get_default_topic_spec() == new.get_default_topic_spec()
    assert isinstance(old.get_pre_filter(), type(new.get_pre_filter()))
    assert isinstance(old.get_processor(), type(new.get_processor()))
    assert old.get_replacement_topic_spec() == new.get_replacement_topic_spec()
    assert (
        old.get_subscription_result_topic_spec()
        == new.get_subscription_result_topic_spec()
    )
    assert (
        old.get_subscription_scheduled_topic_spec()
        == new.get_subscription_scheduled_topic_spec()
    )
    assert (
        old.get_subscription_scheduler_mode() == new.get_subscription_scheduler_mode()
    )
