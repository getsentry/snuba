from __future__ import annotations

from typing import Any

from jsonschema import validate
from yaml import safe_load

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
from snuba.datasets.configuration.json_schema import (
    V1_READABLE_STORAGE_SCHEMA,
    V1_WRITABLE_STORAGE_SCHEMA,
)
from snuba.datasets.storages import StorageKey
from snuba.utils.schemas import UUID, AggregateFunction


def __parse_simple(
    col: dict[str, Any], modifiers: SchemaModifiers | None
) -> Column[SchemaModifiers]:
    if col["type"] == "UInt":
        return Column(col["name"], UInt(col["args"]["size"], modifiers))
    elif col["type"] == "Float":
        return Column(col["name"], Float(col["args"]["size"], modifiers))
    elif col["type"] == "String":
        return Column(col["name"], String(modifiers))
    elif col["type"] == "DateTime":
        return Column(col["name"], DateTime(modifiers))
    elif col["type"] == "UUID":
        return Column(col["name"], UUID(modifiers))
    raise


def parse_columns(columns: list[dict[str, Any]]) -> list[Column[SchemaModifiers]]:
    cols: list[Column[SchemaModifiers]] = []

    SIMPLE_COLUMN_TYPES = {
        "UInt": UInt,
        "Float": Float,
        "String": String,
        "DateTime": DateTime,
        "UUID": UUID,
    }

    for col in columns:
        modifiers = None
        if "args" in col and "schema_modifiers" in col["args"]:
            modifiers = SchemaModifiers(
                "nullable" in col["args"]["schema_modifiers"],
                "readonly" in col["args"]["schema_modifiers"],
            )

        column: Column[SchemaModifiers] | None = None
        if col["type"] in SIMPLE_COLUMN_TYPES:
            column = __parse_simple(col, modifiers)
        elif col["type"] == "Nested":
            column = Column(
                col["name"], Nested(parse_columns(col["args"]["subcolumns"]), modifiers)
            )
        elif col["type"] == "Array":
            column = Column(
                col["name"],
                Array(
                    SIMPLE_COLUMN_TYPES[col["args"]["type"]](col["args"]["arg"]),
                    modifiers,
                ),
            )
        elif col["type"] == "AggregateFunction":
            column = Column(
                col["name"],
                AggregateFunction(
                    col["args"]["func"],
                    [
                        SIMPLE_COLUMN_TYPES[c["type"]](c["arg"])
                        if "arg" in c
                        else SIMPLE_COLUMN_TYPES[c["type"]]()
                        for c in col["args"]["arg_types"]
                    ],
                ),
            )
        assert column is not None
        cols.append(column)
    return cols


CONFIG_FILES_PATH = "./snuba/datasets/configuration/generic_metrics/storages"
CONFIG_FILES = {
    StorageKey.GENERIC_METRICS_DISTRIBUTIONS: f"{CONFIG_FILES_PATH}/distributions.yaml",
    StorageKey.GENERIC_METRICS_DISTRIBUTIONS_RAW: f"{CONFIG_FILES_PATH}/distributions_bucket.yaml",
}
STORAGE_VALIDATION_SCHEMAS = {
    "readonly_storage": V1_READABLE_STORAGE_SCHEMA,
    "writable_storage": V1_WRITABLE_STORAGE_SCHEMA,
}


def load_storage_config(storage_key: StorageKey) -> dict[str, Any]:
    file = open(CONFIG_FILES[storage_key])
    config = safe_load(file)
    assert isinstance(config, dict)
    validate(config, STORAGE_VALIDATION_SCHEMAS[config["kind"]])
    return config
