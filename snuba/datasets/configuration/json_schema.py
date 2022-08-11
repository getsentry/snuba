from __future__ import annotations

from typing import Any

TYPE_STRING = {"type": "string"}

STREAM_LOADER_SCHEMA = {
    "type": "object",
    "properties": {
        "processor": TYPE_STRING,
        "default_topic": TYPE_STRING,
        "commit_log_topic": TYPE_STRING,
        "subscription_scheduled_topic": TYPE_STRING,
        "subscription_scheduler_mode": TYPE_STRING,
        "subscription_result_topic": TYPE_STRING,
        "replacement_topic": {"type": ["string", "null"]},
        "prefilter": {
            "type": "object",
            "properties": {
                "type": TYPE_STRING,
                "args": {"type": "array", "items": TYPE_STRING},
            },
        },
        "dlq_policy": {
            "type": "object",
            "properties": {
                "type": TYPE_STRING,
                "args": {"type": "array", "items": TYPE_STRING},
            },
        },
    },
}

######
# Column specific json schemas
def make_column_schema(
    column_type: dict[str, Any], args: dict[str, Any] | None
) -> dict[str, Any]:
    props = {
        "name": TYPE_STRING,
        "type": column_type,
        "schema_modifiers": {"type": "array", "items": TYPE_STRING},
    }
    if args is not None:
        props["args"] = args
    return {
        "type": "object",
        "properties": props,
    }


NUMBER_SCHEMA = make_column_schema(
    column_type={"enum": ["UInt", "Float"]},
    args={"type": "object", "properties": {"size": {"type": "number"}}},
)


NO_ARG_SCHEMA = make_column_schema(
    column_type={"enum": ["String", "DateTime"]}, args=None
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
    args={"type": "array", "items": {"anyOf": COLUMN_TYPES}},
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

WRITABLE_STORAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "version": TYPE_STRING,
        "kind": KIND_SCHEMA,
        "name": TYPE_STRING,
        "storage": STORAGE_SCHEMA,
        "schema": SCHEMA_SCHEMA,
        "query_processors": QUERY_PROCESSORS_SCHEMA,
        "stream_loader": STREAM_LOADER_SCHEMA,
    },
}

READABLE_STORAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "version": TYPE_STRING,
        "kind": KIND_SCHEMA,
        "name": TYPE_STRING,
        "storage": STORAGE_SCHEMA,
        "schema": SCHEMA_SCHEMA,
        "query_processors": QUERY_PROCESSORS_SCHEMA,
    },
}
