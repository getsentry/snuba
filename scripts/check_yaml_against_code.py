from __future__ import annotations

import sys
from typing import Sequence

from snuba.datasets.configuration.storage_builder import build_storage_from_config
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import (
    ReadableStorage,
    ReadableTableStorage,
    Storage,
    WritableTableStorage,
)
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.datasets.table_storage import KafkaStreamLoader
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.utils.schemas import Column, SchemaModifiers


def _check_query_processors(
    converted: Sequence[ClickhouseQueryProcessor],
    original: Sequence[ClickhouseQueryProcessor],
) -> None:
    orig_by_name = {og_qp.config_key(): og_qp for og_qp in original}
    converted_by_name = {og_qp.config_key(): og_qp for og_qp in converted}

    assert set(orig_by_name.keys()) == set(converted_by_name.keys())

    for name in orig_by_name.keys():
        orig = orig_by_name[name]
        converted_thing = converted_by_name[name]
        assert orig.init_kwargs == converted_thing.init_kwargs  # type: ignore


def _check_columns(
    converted: Sequence[Column[SchemaModifiers]],
    original: Sequence[Column[SchemaModifiers]],
) -> None:
    assert len(converted) == len(original)
    for i, orig_col in enumerate(original):
        converted_col = converted[i]
        assert orig_col == converted_col


def check_against_real_storage(
    converted_storage: ReadableStorage, original_storage: ReadableStorage
) -> None:
    _check_columns(
        converted_storage.get_schema().get_columns().columns,
        original_storage.get_schema().get_columns().columns,
    )
    _check_query_processors(
        converted_storage.get_query_processors(),
        original_storage.get_query_processors(),
    )


def check_yaml_against_code(storage_key: str, yaml_file_path: str) -> None:
    original_storage = get_storage(StorageKey(storage_key))
    converted_storage = build_storage_from_config(yaml_file_path)
    check_against_real_storage(converted_storage, original_storage)

    _deep_compare_storages(original_storage, converted_storage)


def _deep_compare_storages(old: Storage, new: Storage) -> None:
    """
    Ported from tests/configuration/test_storage_loader.py
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

    assert len(old.get_mandatory_condition_checkers()) == len(
        new.get_mandatory_condition_checkers()
    ) and set(
        [checker.get_id() for checker in old.get_mandatory_condition_checkers()]
    ) == set(
        [checker.get_id() for checker in new.get_mandatory_condition_checkers()]
    )
    assert len(old.get_query_processors()) == len(new.get_query_processors()) and set(
        [processor.config_key() for processor in old.get_query_processors()]
    ) == set([processor.config_key() for processor in new.get_query_processors()])
    assert len(old.get_query_splitters()) == len(new.get_query_splitters()) and set(
        [splitter.config_key() for splitter in old.get_query_splitters()]
    ) == set([splitter.config_key() for splitter in new.get_query_splitters()])
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
    """
    Ported from tests/configuration/test_storage_loader.py
    """
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


if __name__ == "__main__":
    check_yaml_against_code(sys.argv[1], sys.argv[2])
    print("They match!")
