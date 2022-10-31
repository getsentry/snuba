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
        "type": {
            "type": "string",
            "description": "Name of FunctionCall class config key",
        },
        "args": {"type": "array", "items": {"type": "string"}, "description": ""},
    },
    "additionalProperties": False,
}

STREAM_LOADER_SCHEMA = {
    "type": "object",
    "properties": {
        "processor": {
            "type": "object",
            "description": "Name of Processor class config key and it's arguments. Responsible for converting an incoming message body from the event stream into a row or statement to be inserted or executed against clickhouse",
            "properties": {
                "name": TYPE_STRING,
                "args": {
                    "type": "object",
                    "description": "Key/value mappings required to instantiate the processor class.",
                },
            },
            "additionalProperties": False,
            "required": ["name"],
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
            "description": "Name of the subscription scheduled Kafka topic",
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
                    "description": "Name of StreamMessageFilter class config key",
                },
                "args": {
                    "type": "object",
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
    column_type={"enum": ["String", "DateTime", "UUID", "IPv4", "IPv6"]},
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
        "partition_format": {
            "type": "array",
            "items": {"type": "string"},
            "description": "The format of the partitions in Clickhouse. Used in the cleanup job.",
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


def registered_class_schema(
    property_name: str, class_name: str, description: str
) -> dict[str, Any]:
    """
    There are a number of registered classes that are represented in the
    YAML in a very similar structure, just with different key names.
    This function reduces the duplicate schema code.

    :param property_name: The key in the configuration for the class name
    :param class_name: The name of the class being represented.
    :param description: The description added to the documentation.
    """
    single_class = {
        "type": "object",
        "properties": {
            property_name: {
                "type": "string",
                "description": description,
            },
            "args": {
                "type": "object",
                "description": f"Key/value mappings required to instantiate {class_name} class.",
            },  # args are a flexible dict
        },
        "required": [property_name],
        "additionalProperties": False,
    }
    return {"type": "array", "items": single_class}


STORAGE_QUERY_PROCESSORS_SCHEMA = registered_class_schema(
    "processor",
    "QueryProcessor",
    "Name of ClickhouseQueryProcessor class config key. Responsible for the transformation applied to a query.",
)
STORAGE_QUERY_SPLITTERS_SCHEMA = registered_class_schema(
    "splitter",
    "QuerySplitStrategy",
    "Name of QuerySplitStrategy class config key. Responsible for splitting a query into two at runtime and combining the results.",
)
STORAGE_MANDATORY_CONDITION_CHECKERS_SCHEMA = registered_class_schema(
    "condition",
    "ConditionChecker",
    "Name of ConditionChecker class config key. Responsible for running final checks on a query to ensure that transformations haven't impacted/removed conditions required for security reasons.",
)


ENTITY_QUERY_PROCESSOR = {
    "type": "object",
    "properties": {
        "processor": {
            "type": "string",
            "description": "Name of LogicalQueryProcessor class config key. Responsible for the transformation applied to a query.",
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
            "description": "Validator class name",
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
                "description": "Mapper class name",
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
        "columns": ENTITY_TRANSLATION_MAPPER_SUB_LIST,
        "functions": ENTITY_TRANSLATION_MAPPER_SUB_LIST,
        "curried_functions": ENTITY_TRANSLATION_MAPPER_SUB_LIST,
        "subscriptables": ENTITY_TRANSLATION_MAPPER_SUB_LIST,
        "columns": ENTITY_TRANSLATION_MAPPER_SUB_LIST,
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
        "stream_loader": STREAM_LOADER_SCHEMA,
        "query_processors": STORAGE_QUERY_PROCESSORS_SCHEMA,
        "query_splitters": STORAGE_QUERY_SPLITTERS_SCHEMA,
        "mandatory_condition_checkers": STORAGE_MANDATORY_CONDITION_CHECKERS_SCHEMA,
        "writer_options": {
            "type": "object",
            "description": "Extra Clickhouse fields that are used for consumer writes",
        },
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
        "query_processors": STORAGE_QUERY_PROCESSORS_SCHEMA,
        "query_splitters": STORAGE_QUERY_PROCESSORS_SCHEMA,
        "mandatory_condition_checkers": STORAGE_QUERY_PROCESSORS_SCHEMA,
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
        "partition_key_column_name": {
            "type": ["string", "null"],
            "description": "The column name, if this entity is partitioned, to select slice",
        },
        "entity_subscriptions": {
            "type": "object",
            "description": "Specifies whether entity has subscriptions enabled",
            "properties": {
                "max_allowed_aggregations": {
                    "type": "integer",
                    "description": "The max number of allowed of aggregations in subscription",
                },
                "disallowed_aggregations": {
                    "type": "array",
                    "items": TYPE_STRING,
                    "description": "The disallowed aggregation clauses in subscription query (e.g. [having, orderby])",
                },
            },
            "required": [
                "max_allowed_aggregations",
                "disallowed_aggregations",
            ],
            "additionalProperties": False,
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
            "type": "array",
            "items": TYPE_STRING,
            "description": "Names of entities associated with this dataset",
        },
    },
    "required": [
        "version",
        "kind",
        "name",
        "entities",
        "is_experimental",
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
    "readable_storage": V1_READABLE_STORAGE_SCHEMA,
    "writable_storage": V1_WRITABLE_STORAGE_SCHEMA,
    "migration_group": V1_MIGRATION_GROUP_SCHEMA,
}
