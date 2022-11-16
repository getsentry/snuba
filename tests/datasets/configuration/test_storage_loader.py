from __future__ import annotations

import os
import tempfile
from typing import Any

from snuba.clickhouse.columns import (
    Array,
    Column,
    DateTime,
    Float,
    Nested,
    SchemaModifiers,
    String,
    UInt,
)
from snuba.datasets.configuration.storage_builder import build_storage_from_config
from snuba.datasets.configuration.utils import parse_columns
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import ReadableTableStorage, Storage, WritableTableStorage
from snuba.datasets.storages.factory import get_config_built_storages
from snuba.utils.schemas import AggregateFunction

# this has to be done before the storage import because there's a cyclical dependency error
CONFIG_BUILT_STORAGES = get_config_built_storages()


from snuba.datasets.storages.generic_metrics import (
    distributions_bucket_storage,
    distributions_storage,
    sets_bucket_storage,
    sets_storage,
)
from snuba.datasets.storages.profiles import writable_storage as profiles
from snuba.datasets.storages.querylog import storage as querylog
from snuba.datasets.storages.replays import storage as replays
from snuba.datasets.storages.sessions import materialized_storage as sessions_hourly
from snuba.datasets.storages.sessions import org_materialized_storage as sessions_org
from snuba.datasets.storages.sessions import raw_storage as sessions_raw
from snuba.datasets.storages.transactions import storage as transactions
from snuba.datasets.table_storage import KafkaStreamLoader
from tests.datasets.configuration.utils import ConfigurationTest


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


class TestStorageConfiguration(ConfigurationTest):
    python_storages: list[ReadableTableStorage] = [
        distributions_bucket_storage,
        distributions_storage,
        sets_bucket_storage,
        sets_storage,
        sessions_raw,
        sessions_org,
        sessions_hourly,
        transactions,
        profiles,
        replays,
        querylog,
    ]

    def test_config_file_discovery(self) -> None:
        assert all(
            storage.get_storage_key() in CONFIG_BUILT_STORAGES
            for storage in self.python_storages
        )
        assert len(CONFIG_BUILT_STORAGES) == len(self.python_storages)

    def test_compare_storages(self) -> None:
        for storage in self.python_storages:
            _deep_compare_storages(
                storage, CONFIG_BUILT_STORAGES[storage.get_storage_key()]
            )

    def test_processor_with_constructor(self) -> None:
        yml_text = """
version: v1
kind: readable_storage
name: test-processor-with-constructor

storage:
  key: test-storage
  set_key: test-storage-set

schema:
  columns:
    [
      { name: org_id, type: UInt, args: { size: 64 } },
    ]
  local_table_name: "test"
  dist_table_name: "test"

query_processors:
  -
    processor: MappingOptimizer
    args:
      column_name: a
      hash_map_name: hashmap
      killswitch: kill

"""
        with tempfile.TemporaryDirectory() as tmpdirname:
            filename = os.path.join(tmpdirname, "file.yaml")
            with open(filename, "w") as f:
                f.write(yml_text)
            storage = build_storage_from_config(filename)
            assert len(storage.get_query_processors()) == 1
            qp = storage.get_query_processors()[0]
            assert getattr(qp, "_MappingOptimizer__column_name") == "a"
            assert getattr(qp, "_MappingOptimizer__hash_map_name") == "hashmap"
            assert getattr(qp, "_MappingOptimizer__killswitch") == "kill"

    def test_column_parser(self) -> None:
        serialized_columns: list[dict[str, Any]] = [
            {"name": "int_col", "type": "UInt", "args": {"size": 64}},
            {"name": "float_col", "type": "Float", "args": {"size": 32}},
            {"name": "string_col", "type": "String"},
            {"name": "time_col", "type": "DateTime"},
            {
                "name": "nested_col",
                "type": "Nested",
                "args": {
                    "subcolumns": [
                        {"name": "sub_col", "type": "UInt", "args": {"size": 64}},
                    ],
                },
            },
            {
                "name": "func_col",
                "type": "AggregateFunction",
                "args": {
                    "func": "uniqCombined64",
                    "arg_types": [{"type": "UInt", "args": {"size": 64}}],
                },
            },
            {
                "name": "array_col",
                "type": "Array",
                "args": {
                    "inner_type": {"type": "UInt", "args": {"size": 64}},
                    "schema_modifiers": ["readonly"],
                },
            },
            {
                "name": "double_array_col",
                "type": "Array",
                "args": {
                    "inner_type": {
                        "type": "Array",
                        "args": {"inner_type": {"type": "UInt", "args": {"size": 64}}},
                    },
                    "schema_modifiers": ["readonly"],
                },
            },
        ]

        expected_python_columns = [
            Column("int_col", UInt(64)),
            Column("float_col", Float(32)),
            Column("string_col", String()),
            Column("time_col", DateTime()),
            Column("nested_col", Nested([Column("sub_col", UInt(64))])),
            Column("func_col", AggregateFunction("uniqCombined64", [UInt(64)])),
            Column(
                "array_col",
                Array(UInt(64), SchemaModifiers(nullable=False, readonly=True)),
            ),
            Column(
                "double_array_col",
                Array(Array(UInt(64)), SchemaModifiers(nullable=False, readonly=True)),
            ),
        ]

        assert parse_columns(serialized_columns) == expected_python_columns
