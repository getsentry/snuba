from __future__ import annotations

from typing import Any

# Snubadocs are automatically generated from this file. When adding new schemas or individual keys,
# please ensure you add a description key in the same level and succinctly describe the property.

TYPE_STRING = {"type": "string"}
TYPE_STRING_ARRAY = {"type": "array", "items": TYPE_STRING}
TYPE_NULLABLE_INTEGER = {"type": ["integer", "null"]}
TYPE_NULLABLE_STRING = {"type": ["string", "null"]}

FUNCTION_CALL_SCHEMA = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "description": "Name of class key"},
        "args": {"type": "array", "items": {"type": "string"}, "description": ""},
    },
    "additionalProperties": False,
}

STREAM_LOADER_SCHEMA = {
    "type": "object",
    "properties": {
        "processor": {
            "type": "string",
            "description": "Class name for Processor. Responsible for converting an incoming message body from the event stream into a row or statement to be inserted or executed against clickhouse",
        },
        "default_topic": {
            "type": "string",
            "description": "Name of the Kafka topic to consume from",
        },
        "commit_log_topic": {
            "type": ["string", "null"],
            "description": "Name of the commit log Kafka topic",
        },
        "subscription_scheduled_topic": {
            "type": ["string", "null"],
            "description": "Name of the subscroption scheduled Kafka topic",
        },
        "subscription_scheduler_mode": {
            "type": ["string", "null"],
            "description": "The subscription scheduler mode used (e.g. partition or global). This must be specified if subscriptions are supported for this storage",
        },
        "subscription_result_topic": {
            "type": ["string", "null"],
            "description": "Name of the subscription result Kafka topic",
        },
        "replacement_topic": {
            "type": ["string", "null"],
            "description": "Name of the replacements Kafka topic",
        },
        "pre_filter": {
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "description": "Name of StreamMessageFilter class key",
                },
                "args": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Key/value mappings required to instantiate StreamMessageFilter class.",
                },
            },
            "additionalProperties": False,
            "description": "Name of class which filter messages incoming from stream",
        },
        "dlq_policy": {
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "description": "DLQ policy type",
                },
                "args": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Key/value mappings required to instantiate DLQ class (e.g. topic name).",
                },
            },
            "additionalProperties": False,
            "description": "Name of class which filter messages incoming from stream",
        },
    },
    "additionalProperties": False,
    "description": "The stream loader for a writing to ClickHouse. This provides what is needed to start a Kafka consumer and fill in the ClickHouse table.",
}

NULLABLE_DISALLOWED_AGGREGATIONS_SCHEMA = {
    "type": ["array", "null"],
    "items": TYPE_STRING,
}

######
# Column specific json schemas
def make_column_schema(
    column_type: dict[str, Any], args: dict[str, Any]
) -> dict[str, Any]:
    args["properties"]["schema_modifiers"] = TYPE_STRING_ARRAY
    return {
        "type": "object",
        "properties": {
            "name": TYPE_STRING,
            "type": column_type,
            "args": args,
        },
        "additionalProperties": False,
    }


NUMBER_SCHEMA = make_column_schema(
    column_type={"enum": ["UInt", "Float"]},
    args={
        "type": "object",
        "properties": {
            "size": {"type": "number"},
        },
        "additionalProperties": False,
    },
)


NO_ARG_SCHEMA = make_column_schema(
    column_type={"enum": ["String", "DateTime"]},
    args={
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
)


ARRAY_SCHEMA = make_column_schema(
    column_type={"const": "Array"},
    args={
        "type": "object",
        "properties": {
            "type": TYPE_STRING,
            "arg": {"type": "number"},
        },
        "additionalProperties": False,
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
                        "type": {"enum": ["Float", "UUID", "UInt"]},
                        "arg": {"type": ["number", "null"]},
                    },
                    "additionalProperties": False,
                },
            },
        },
        "additionalProperties": False,
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
        "additionalProperties": False,
    },
)

SCHEMA_COLUMNS = {
    "type": "array",
    "items": {"anyOf": [*COLUMN_TYPES, NESTED_SCHEMA]},
    "description": "Objects (or nested objects) representing columns containg a name, type and args",
}

SCHEMA_SCHEMA = {
    "type": "object",
    "properties": {
        "columns": SCHEMA_COLUMNS,
        "local_table_name": {
            "type": "string",
            "description": "The local table name in a single-node ClickHouse",
        },
        "dist_table_name": {
            "type": "string",
            "description": "The distributed table name in distributed ClickHouse",
        },
    },
    "additionalProperties": False,
}
######

STORAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "key": {
            "type": "string",
            "description": "A unique key identifier for the storage",
        },
        "set_key": {
            "type": "string",
            "description": "A unique key identifier for a collection of storages located in the same cluster.",
        },
    },
    "additionalProperties": False,
}

STORAGE_QUERY_PROCESSORS_SCHEMA = TYPE_STRING_ARRAY

ENTITY_QUERY_PROCESSOR = {
    "type": "object",
    "properties": {
        "processor": {
            "type": "string",
            "description": "Name of QueryProcessor class responsible for the transformation applied to a query.",
        },
        "args": {
            "type": "object",
            "description": "Key/value mappings required to instantiate QueryProcessor class.",
        },  # args are a flexible dict
    },
    "required": ["processor"],
    "additionalProperties": False,
}

ENTITY_VALIDATOR = {
    "type": "object",
    "properties": {
        "validator": {
            "type": "string",
            "description": "Name of Validator class config key",
        },
        "args": {
            "type": "object",
            "description": "Key/value mappings required to instantiate Validator class",
        },  # args are a flexible dict
    },
    "required": ["validator"],
    "additionalProperties": False,
}

ENTITY_TRANSLATION_MAPPER_SUB_LIST = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "mapper": {
                "type": "string",
                "description": "Name of Mapper class config key",
            },
            "args": {
                "type": "object",
                "description": "Key/value mappings required to instantiate Mapper class",
            },
        },
        "required": ["mapper"],
        "additionalProperties": False,
    },
}

ENTITY_TRANSLATION_MAPPERS = {
    "type": "object",
    "description": "Represents the set of rules used to translates different expression types",
    "properties": {
        "functions": ENTITY_TRANSLATION_MAPPER_SUB_LIST,
        "curried_functions": ENTITY_TRANSLATION_MAPPER_SUB_LIST,
        "subscriptables": ENTITY_TRANSLATION_MAPPER_SUB_LIST,
    },
    "additionalProperties": False,
}

# Full schemas:

V1_WRITABLE_STORAGE_SCHEMA = {
    "title": "Writable Storage Schema",
    "type": "object",
    "properties": {
        "version": {"const": "v1", "description": "Version of schema"},
        "kind": {"const": "writable_storage", "description": "Component kind"},
        "name": {"type": "string", "description": "Name of the writable storage"},
        "storage": STORAGE_SCHEMA,
        "schema": SCHEMA_SCHEMA,
        "query_processors": {
            "type": "array",
            "items": {
                "type": "string",
            },
            "description": "Names of QueryProcessor class which represents a transformation applied to the ClickHouse query",
        },
        "stream_loader": STREAM_LOADER_SCHEMA,
    },
    "required": [
        "version",
        "kind",
        "name",
        "storage",
        "schema",
        "stream_loader",
    ],
    "additionalProperties": False,
}


V1_READABLE_STORAGE_SCHEMA = {
    "title": "Readable Storage Schema",
    "type": "object",
    "properties": {
        "version": {"const": "v1", "description": "Version of schema"},
        "kind": {"const": "readable_storage", "description": "Component kind"},
        "name": {"type": "string", "description": "Name of the readable storage"},
        "storage": STORAGE_SCHEMA,
        "schema": SCHEMA_SCHEMA,
        "query_processors": {
            "type": "array",
            "items": {
                "type": "string",
            },
            "description": "Names of QueryProcess class which represents a transformation applied to the ClickHouse query",
        },
    },
    "required": [
        "version",
        "kind",
        "name",
        "storage",
        "schema",
    ],
    "additionalProperties": False,
}

V1_ENTITY_SCHEMA = {
    "title": "Entity Schema",
    "type": "object",
    "properties": {
        "version": {"const": "v1", "description": "Version of schema"},
        "kind": {"const": "entity", "description": "Component kind"},
        "schema": SCHEMA_COLUMNS,
        "name": {**TYPE_STRING, **{"description": "Name of the entity"}},
        "readable_storage": {
            **TYPE_STRING,
            **{
                "description": "Name of a ReadableStorage class which provides an abstraction to read from a table or a view in ClickHouse"
            },
        },
        "writable_storage": {
            "type": ["string", "null"],
            "description": "Name of a WritableStorage class which provides an abstraction to write to a table in ClickHouse",
        },
        "query_processors": {
            "type": "array",
            "items": ENTITY_QUERY_PROCESSOR,
            "description": "Represents a transformation applied to the ClickHouse query",
        },
        "translation_mappers": ENTITY_TRANSLATION_MAPPERS,
        "validators": {
            "type": "array",
            "items": ENTITY_VALIDATOR,
            "description": "The validation logic used on the ClickHouse query",
        },
        "required_time_column": {
            **TYPE_STRING,
            **{
                "description": "The name of the required time column specifed in schema"
            },
        },
    },
    "required": [
        "version",
        "kind",
        "schema",
        "name",
        "readable_storage",
        "query_processors",
        "validators",
        "required_time_column",
    ],
    "additionalProperties": False,
}

V1_DATASET_SCHEMA = {
    "title": "Dataset Schema",
    "type": "object",
    "properties": {
        "version": {"const": "v1", "description": "Version of schema"},
        "kind": {"const": "dataset", "description": "Component kind"},
        "name": {"type": "string", "description": "Name of the dataset"},
        "is_experimental": {
            "type": "boolean",
            "description": "Marks the dataset as experimental. Healthchecks failing on this dataset will not block deploys and affect Snuba server's SLOs",
        },
        "entities": {
            "type": "object",
            "properties": {
                "all": {
                    "type": "array",
                    "items": TYPE_STRING,
                    "description": "Names of entities associated with this dataset",
                },
            },
            "additionalProperties": False,
            "required": ["all"],
        },
    },
    "required": [
        "version",
        "kind",
        "name",
        "entities",
    ],
    "additionalProperties": False,
}

V1_ENTITY_SUBSCIPTION_SCHEMA = {
    "title": "Entity Subscription Schema",
    "type": "object",
    "properties": {
        "version": {"const": "v1", "description": "Version of schema"},
        "kind": {"const": "entity_subscription", "description": "Component kind"},
        "name": {"type": "string", "description": "Name of the entity subscription"},
        "max_allowed_aggregations": {
            "type": ["integer", "null"],
            "description": "Maximum number of allowed aggregations",
        },
        "disallowed_aggregations": {
            "type": ["array", "null"],
            "items": {
                "type": "string",
            },
            "description": "Name of aggregation clauses that are not allowed",
        },
    },
    "required": [
        "version",
        "kind",
        "name",
    ],
    "additionalProperties": False,
}


V1_MIGRATION_GROUP_SCHEMA = {
    "title": "Migration Group Schema",
    "type": "object",
    "properties": {
        "version": {"const": "v1", "description": "Version of schema"},
        "kind": {"const": "migration_group", "description": "Component kind"},
        "name": {"type": "string", "description": "Name of the migration group"},
        "optional": {
            "type": "boolean",
            "description": "Flag to determine if migration group is optional",
        },
        "migrations": {
            "type": "array",
            "items": {"type": "string", "description": "Names of migrations"},
            "description": "Names of migrations to be applied in group",
        },
    },
    "required": ["name", "migrations"],
    "additionalProperties": False,
}

V1_ALL_SCHEMAS = {
    "dataset": V1_DATASET_SCHEMA,
    "entity": V1_ENTITY_SCHEMA,
    "entity_subscription": V1_ENTITY_SUBSCIPTION_SCHEMA,
    "readable_storage": V1_READABLE_STORAGE_SCHEMA,
    "writable_storage": V1_WRITABLE_STORAGE_SCHEMA,
    "migration_group": V1_MIGRATION_GROUP_SCHEMA,
}
