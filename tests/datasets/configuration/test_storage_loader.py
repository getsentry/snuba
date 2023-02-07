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
