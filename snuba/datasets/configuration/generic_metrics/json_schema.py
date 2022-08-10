from __future__ import annotations

from typing import Any

TYPE_STRING = {"type": "string"}

stream_loader_schema: Any = {
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


def make_column_schema(column_type: Any, args: Any) -> Any:
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


Number_schema: Any = make_column_schema(
    column_type={"enum": ["UInt", "Float"]},
    args={"type": "object", "properties": {"size": {"type": "number"}}},
)


No_arg_schema = make_column_schema(
    column_type={"enum": ["String", "DateTime"]}, args=None
)


Array_schema = make_column_schema(
    column_type={"const": "Array"},
    args={
        "type": "object",
        "properties": {"type": TYPE_STRING, "arg": {"type": "number"}},
    },
)

AggregateFunction_schema: Any = make_column_schema(
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

column_types = [
    Number_schema,
    No_arg_schema,
    Array_schema,
    AggregateFunction_schema,
]

Nested_schema = make_column_schema(
    column_type={"const": "Nested"},
    args={"type": "array", "items": {"anyOf": column_types}},
)

schema_schema: Any = {
    "type": "object",
    "properties": {
        "columns": {
            "type": "array",
            "items": {"anyOf": column_types + [Nested_schema]},
        },
        "local_table_name": TYPE_STRING,
        "dist_table_name": TYPE_STRING,
    },
}
storage_schema: Any = {
    "type": "object",
    "properties": {"key": TYPE_STRING, "set_key": TYPE_STRING},
}

query_processors_schema: Any = {"type": "array", "items": TYPE_STRING}


writable_storage_schema: Any = {
    "type": "object",
    "properties": {
        "storage": storage_schema,
        "schema": schema_schema,
        "stream_loader": stream_loader_schema,
        "query_processors": query_processors_schema,
    },
}

readable_storage_schema: Any = {
    "type": "object",
    "properties": {
        "storage": storage_schema,
        "schema": schema_schema,
        "query_processors": query_processors_schema,
    },
}
