from __future__ import annotations

import os
import tempfile
from typing import Any

from snuba.clickhouse.columns import (
    Array,
    Column,
    DateTime,
    Enum,
    Float,
    Nested,
    SchemaModifiers,
    String,
    UInt,
)
from snuba.datasets.configuration.storage_builder import build_storage_from_config
from snuba.datasets.configuration.utils import parse_columns
from snuba.utils.schemas import AggregateFunction
from tests.datasets.configuration.utils import ConfigurationTest


class TestStorageConfiguration(ConfigurationTest):
    def test_processor_with_constructor(self) -> None:
        yml_text = """
version: v1
kind: readable_storage
name: test-processor-with-constructor

storage:
  key: test-storage
  set_key: test-storage-set

readiness_state: limited

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
  -
    processor: ClickhouseSettingsOverride
    args:
      settings:
        max_rows_to_group_by: 1000000
        group_by_overflow_mode: any

allocation_policy:
  name: PassthroughPolicy
  args:
    required_tenant_types: ["some_tenant"]

"""
        with tempfile.TemporaryDirectory() as tmpdirname:
            filename = os.path.join(tmpdirname, "file.yaml")
            with open(filename, "w") as f:
                f.write(yml_text)
            storage = build_storage_from_config(filename)
            assert len(storage.get_query_processors()) == 2
            (
                mapping_optimizer_qp,
                clickhouse_settings_override_qp,
            ) = storage.get_query_processors()
            assert (
                getattr(mapping_optimizer_qp, "_MappingOptimizer__column_name") == "a"
            )
            assert (
                getattr(mapping_optimizer_qp, "_MappingOptimizer__hash_map_name")
                == "hashmap"
            )
            assert (
                getattr(mapping_optimizer_qp, "_MappingOptimizer__killswitch") == "kill"
            )
            assert (
                getattr(
                    clickhouse_settings_override_qp,
                    "_ClickhouseSettingsOverride__settings",
                )["max_rows_to_group_by"]
                == 1000000
            )
            assert (
                getattr(
                    clickhouse_settings_override_qp,
                    "_ClickhouseSettingsOverride__settings",
                )["group_by_overflow_mode"]
                == "any"
            )
            assert storage.get_allocation_policy()._required_tenant_types == {
                "some_tenant"
            }
            assert (
                storage.get_allocation_policy().runtime_config_prefix
                == "test-storage.PassthroughPolicy"
            )

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
            {
                "name": "enum_col",
                "type": "Enum",
                "args": {
                    "values": [("success", 0), ("error", 1), ("pending", 2)],
                    "schema_modifiers": ["nullable"],
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
            Column(
                "enum_col",
                Enum(
                    [("success", 0), ("error", 1), ("pending", 2)],
                    SchemaModifiers(nullable=True, readonly=False),
                ),
            ),
        ]

        assert parse_columns(serialized_columns) == expected_python_columns
