from __future__ import annotations

from typing import Any

TYPE_STRING = {"type": "string"}

######
# Column specific json schemas
def make_column_schema(
    column_type: dict[str, Any], args: dict[str, Any]
) -> dict[str, Any]:
    args["properties"]["schema_modifiers"] = {"type": "array", "items": TYPE_STRING}
    return {
        "type": "object",
        "properties": {
            "name": TYPE_STRING,
            "type": column_type,
            "args": args,
        },
    }


NUMBER_SCHEMA = make_column_schema(
    column_type={"enum": ["UInt", "Float"]},
    args={"type": "object", "properties": {"size": {"type": "number"}}},
)


NO_ARG_SCHEMA = make_column_schema(
    column_type={"enum": ["String", "DateTime"]},
    args={"type": "object", "properties": {}},
)


ARRAY_SCHEMA = make_column_schema(
    column_type={"const": "Array"},
    args={
        "type": "object",
        "properties": {"type": TYPE_STRING, "arg": {"type": "number"}},
    },
)

AGGREGATE_FUNCTION_SCHEMA = make_column_schema(
    column_type={"const": "AggregateFunction"},
    args={
        "type": "object",
        "properties": {
            "func": TYPE_STRING,
            "arg_types": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "type": {"enum": ["Float", "UUID"]},
                        "arg": {"type": ["number", "null"]},
                    },
                },
            },
        },
    },
)

COLUMN_TYPES = [
    NUMBER_SCHEMA,
    NO_ARG_SCHEMA,
    ARRAY_SCHEMA,
    AGGREGATE_FUNCTION_SCHEMA,
]

NESTED_SCHEMA = make_column_schema(
    column_type={"const": "Nested"},
    args={
        "type": "object",
        "properties": {
            "subcolumns": {"type": "array", "items": {"anyOf": COLUMN_TYPES}}
        },
    },
)

SCHEMA_SCHEMA = {
    "type": "object",
    "properties": {
        "columns": {
            "type": "array",
            "items": {"anyOf": [*COLUMN_TYPES, NESTED_SCHEMA]},
        },
        "local_table_name": TYPE_STRING,
        "dist_table_name": TYPE_STRING,
    },
}
######

STORAGE_SCHEMA = {
    "type": "object",
    "properties": {"key": TYPE_STRING, "set_key": TYPE_STRING},
}

QUERY_PROCESSORS_SCHEMA = {"type": "array", "items": TYPE_STRING}

KIND_SCHEMA = {"enum": ["writable_storage", "readonly_storage"]}


# Full schemas:

V1_WRITABLE_STORAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "version": {"const": "v1"},
        "kind": KIND_SCHEMA,
        "name": TYPE_STRING,
        "storage": STORAGE_SCHEMA,
        "schema": SCHEMA_SCHEMA,
        "query_processors": QUERY_PROCESSORS_SCHEMA,
        # TODO: "stream_loader": STREAM_LOADER_SCHEMA,
    },
}

V1_READABLE_STORAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "version": {"const": "v1"},
        "kind": KIND_SCHEMA,
        "name": TYPE_STRING,
        "storage": STORAGE_SCHEMA,
        "schema": SCHEMA_SCHEMA,
        "query_processors": QUERY_PROCESSORS_SCHEMA,
    },
}
