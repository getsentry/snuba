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

UInt_schema: Any = {
    "type": "object",
    "properties": {
        "name": TYPE_STRING,
        "type": {"const": "UInt"},
        "args": {"type": "object", "properties": {"size": {"type": "number"}}},
        "schema_modifiers": {"type": "array", "items": TYPE_STRING},
    },
}

No_arg_schema: Any = {
    "type": "object",
    "properties": {
        "name": TYPE_STRING,
        "type": {"enum": ["String", "DateTime"]},
        "schema_modifiers": {"type": "array", "items": TYPE_STRING},
    },
}

Nested_schema: Any = {
    "type": "object",
    "properties": {
        "name": TYPE_STRING,
        "type": {"const": "Nested"},
        "args": {"type": "array"},
        "schema_modifiers": {"type": "array", "items": TYPE_STRING},
    },
}

Array_schema: Any = {
    "type": "object",
    "properties": {
        "name": TYPE_STRING,
        "type": {"const": "Array"},
        "args": {
            "type": "object",
            "properties": {"type": TYPE_STRING, "arg": {"type": "number"}},
        },
        "schema_modifiers": {"type": "array", "items": TYPE_STRING},
    },
}

AggregateFunction_schema: Any = {
    "type": "object",
    "properties": {
        "name": TYPE_STRING,
        "type": {"const": "AggregateFunction"},
        "args": {
            "type": "object",
            "properties": {"func": TYPE_STRING, "arg_types": {"type": "array"}},
        },
        "schema_modifiers": {"type": "array", "items": TYPE_STRING},
    },
}

schema_schema: Any = {
    "type": "object",
    "properties": {
        "columns": {
            "type": "array",
            "items": {
                "anyOf": [
                    UInt_schema,
                    No_arg_schema,
                    Nested_schema,
                    Array_schema,
                    AggregateFunction_schema,
                ]
            },
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
