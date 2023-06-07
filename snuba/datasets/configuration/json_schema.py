from __future__ import annotations

from copy import deepcopy
from typing import Any

import fastjsonschema
import sentry_sdk

# Snubadocs are automatically generated from this file. When adding new schemas or individual keys,
# please ensure you add a description key in the same level and succinctly describe the property.

TYPE_STRING = {"type": "string"}
TYPE_STRING_ARRAY = {"type": "array", "items": TYPE_STRING}


def string_with_description(description: str) -> dict[str, str]:
    return {**TYPE_STRING, "description": description}


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
                "topic": {
                    "type": "string",
                    "description": "DLQ topic name",
                },
            },
            "additionalProperties": False,
            "description": "Name of class which filter messages incoming from stream",
        },
    },
    "additionalProperties": False,
    "description": "The stream loader for a writing to ClickHouse. This provides what is needed to start a Kafka consumer and fill in the ClickHouse table.",
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


def del_name_field(column_schema: dict[str, Any]) -> dict[str, Any]:
    """
    Useful for simply removing the `name` field from a Column Schema.
    Column types within Arrays do not have names but do maintain the
    same structure as the column type itself.
    """
    new_column_schema = deepcopy(column_schema)
    if "properties" in new_column_schema and "name" in new_column_schema["properties"]:
        del new_column_schema["properties"]["name"]
    return new_column_schema


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

FIXED_STRING_SCHEMA = make_column_schema(
    column_type={"enum": ["FixedString"]},
    args={
        "type": "object",
        "properties": {
            "length": {"type": "number"},
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

# Get just the type
_SIMPLE_COLUMN_TYPES = [
    del_name_field(col_type) for col_type in [NUMBER_SCHEMA, NO_ARG_SCHEMA]
]

AGGREGATE_FUNCTION_SCHEMA = make_column_schema(
    column_type={"const": "AggregateFunction"},
    args={
        "type": "object",
        "properties": {
            "func": TYPE_STRING,
            "arg_types": {
                "type": "array",
                "items": {"anyOf": _SIMPLE_COLUMN_TYPES},
            },
        },
        "additionalProperties": False,
    },
)

ENUM_SCHEMA = make_column_schema(
    column_type={"const": "Enum"},
    args={
        "type": "object",
        "properties": {
            "values": {
                "type": "array",
                "items": {
                    "type": "array",
                    "items": [
                        {"type": "string"},
                        {"type": "integer"},
                    ],
                },
            },
        },
        "additionalProperties": False,
    },
)

SIMPLE_COLUMN_SCHEMAS = [
    NUMBER_SCHEMA,
    FIXED_STRING_SCHEMA,
    NO_ARG_SCHEMA,
    AGGREGATE_FUNCTION_SCHEMA,
    ENUM_SCHEMA,
]

# Array inner types are the same as normal column types except they don't have a name
_SIMPLE_ARRAY_INNER_TYPES = [
    del_name_field(col_type) for col_type in SIMPLE_COLUMN_SCHEMAS
]

# Up to one subarray is supported. Eg Array(Array(String())).
_SUB_ARRAY_SCHEMA = make_column_schema(
    column_type={"const": "Array"},
    args={
        "type": "object",
        "properties": {"inner_type": {"anyOf": _SIMPLE_ARRAY_INNER_TYPES}},
        "additionalProperties": False,
    },
)

ARRAY_SCHEMA = make_column_schema(
    column_type={"const": "Array"},
    args={
        "type": "object",
        "properties": {
            "inner_type": {"anyOf": [*_SIMPLE_ARRAY_INNER_TYPES, _SUB_ARRAY_SCHEMA]}
        },
        "additionalProperties": False,
    },
)

COLUMN_SCHEMAS = [
    *SIMPLE_COLUMN_SCHEMAS,
    ARRAY_SCHEMA,
]


NESTED_SCHEMA = make_column_schema(
    column_type={"const": "Nested"},
    args={
        "type": "object",
        "properties": {
            "subcolumns": {"type": "array", "items": {"anyOf": COLUMN_SCHEMAS}}
        },
        "additionalProperties": False,
    },
)

SCHEMA_COLUMNS = {
    "type": "array",
    "items": {"anyOf": [*COLUMN_SCHEMAS, NESTED_SCHEMA]},
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
        "not_deleted_mandatory_condition": string_with_description(
            "The name of the column flagging a deletion, eg `deleted` column in Errors. "
            "Defining this column here will ensure any query served by this storage "
            "explicitly filters out any 'deleted' rows. Should only be used for storages "
            "supporting deletion replacement."
        ),
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
    return {
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


def registered_class_array_schema(
    property_name: str, class_name: str, description: str
) -> dict[str, Any]:
    return {
        "type": "array",
        "items": registered_class_schema(property_name, class_name, description),
    }


STORAGE_QUERY_PROCESSORS_SCHEMA = registered_class_array_schema(
    "processor",
    "QueryProcessor",
    "Name of ClickhouseQueryProcessor class config key. Responsible for the transformation applied to a query.",
)
STORAGE_QUERY_SPLITTERS_SCHEMA = registered_class_array_schema(
    "splitter",
    "QuerySplitStrategy",
    "Name of QuerySplitStrategy class config key. Responsible for splitting a query into two at runtime and combining the results.",
)
STORAGE_MANDATORY_CONDITION_CHECKERS_SCHEMA = registered_class_array_schema(
    "condition",
    "ConditionChecker",
    "Name of ConditionChecker class config key. Responsible for running final checks on a query to ensure that transformations haven't impacted/removed conditions required for security reasons.",
)
STORAGE_ALLOCATION_POLICIES_SCHEMA = registered_class_array_schema(
    "name",
    "AllocationPolicy",
    "Name of the AllocationPolicy used for allocating read resources per query on this storage.",
)
STORAGE_REPLACER_PROCESSOR_SCHEMA = registered_class_schema(
    "processor",
    "ReplacerProcessor",
    "Name of ReplacerProcessor class config key. Responsible for optimizing queries on a storage which can have replacements, eg deletions/updates.",
)
CDC_STORAGE_ROW_PROCESSOR_SCHEMA = registered_class_schema(
    "processor",
    "CdcRowProcessor",
    "Name of CDC Row Processor. Should only be used by CDC Storages.",
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

ENTITY_SUBSCRIPTION_PROCESSORS = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "processor": {
                "type": "string",
                "description": "Entity Subscription Processor class name",
            },
            "args": {
                "type": "object",
                "description": "Key/value mappings required to instantiate Entity Subscription Processor class",
            },
        },
        "required": ["processor"],
        "additionalProperties": False,
    },
}

ENTITY_SUBSCRIPTION_VALIDATORS = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "validator": {
                "type": "string",
                "description": "Entity Subscription Validator class name",
            },
            "args": {
                "type": "object",
                "description": "Key/value mappings required to instantiate Entity Subscription Validator class",
            },
        },
        "required": ["validator"],
        "additionalProperties": False,
    },
}

READINESS_STATE_SCHEMA = {
    "type": "string",
    "enum": ["limited", "deprecate", "partial", "complete"],
    "description": "The readiness state defines the availability of the storage in various environments. Internally, this label is used to determine which environments this storage is released in. There for four different readiness states: limited, deprecrate, partial, and complete. Different environments support a set of these readiness_states . If this is a new storage, start with `limited` which only exposes the storage to CI and local development.",
}

STORAGE_AND_MAPPER = {
    "type": "object",
    "properties": {
        "storage": {
            **TYPE_STRING,
            **{
                "description": "Name of a readable or writable storage class which provides an abstraction to read from a table or a view in ClickHouse"
            },
        },
        "is_writable": {
            "type": "boolean",
            "description": "Marks the storage is a writable one.",
        },
        "translation_mappers": ENTITY_TRANSLATION_MAPPERS,
    },
    "required": ["storage"],
    "additionalProperties": False,
}

ENTITY_JOIN_RELATIONSHIPS = {
    "type": "object",
    "patternProperties": {
        "^.*$": {
            "type": "object",
            "description": "The join relationship. The key for this relationship is how the relationship is specified in queries (`MATCH x -[key]-> y`)",
            "properties": {
                "rhs_entity": {
                    "type": "string",
                    "description": "The entity key of the rhs entity to join with",
                },
                "columns": {
                    "type": "array",
                    "items": {
                        "type": "array",
                        "prefixItems": [
                            {"type": "string"},
                            {"type": "string"},
                        ],
                    },
                    "description": "A sequence of tuples of columns to join on, in the form (left, right)",
                },
                "join_type": {
                    "type": "string",
                    "description": "The type of join that can be performed (either 'left' or 'inner'",
                },
                "equivalences": {
                    "type": "array",
                    "items": {
                        "type": "array",
                        "prefixItems": [
                            {"type": "string"},
                            {"type": "string"},
                        ],
                    },
                    "description": "Tracking columns in the two entities that are not part of the join key but are still equivalent",
                },
            },
            "required": ["rhs_entity", "columns", "join_type"],
            "additionalProperties": False,
        },
    },
}

# Full schemas:

V1_READABLE_STORAGE_SCHEMA = {
    "title": "Readable Storage Schema",
    "type": "object",
    "properties": {
        "version": {"const": "v1", "description": "Version of schema"},
        "kind": {"const": "readable_storage", "description": "Component kind"},
        "name": {"type": "string", "description": "Name of the readable storage"},
        "storage": STORAGE_SCHEMA,
        "readiness_state": READINESS_STATE_SCHEMA,
        "schema": SCHEMA_SCHEMA,
        "query_processors": STORAGE_QUERY_PROCESSORS_SCHEMA,
        "query_splitters": STORAGE_QUERY_SPLITTERS_SCHEMA,
        "mandatory_condition_checkers": STORAGE_MANDATORY_CONDITION_CHECKERS_SCHEMA,
        "allocation_policies": STORAGE_ALLOCATION_POLICIES_SCHEMA,
    },
    "required": [
        "version",
        "kind",
        "name",
        "storage",
        "readiness_state",
        "schema",
    ],
    "additionalProperties": False,
}

V1_WRITABLE_STORAGE_SCHEMA = {
    "title": "Writable Storage Schema",
    "type": "object",
    "properties": {
        "version": {"const": "v1", "description": "Version of schema"},
        "kind": {"const": "writable_storage", "description": "Component kind"},
        "name": {"type": "string", "description": "Name of the writable storage"},
        "storage": STORAGE_SCHEMA,
        "readiness_state": READINESS_STATE_SCHEMA,
        "schema": SCHEMA_SCHEMA,
        "stream_loader": STREAM_LOADER_SCHEMA,
        "query_processors": STORAGE_QUERY_PROCESSORS_SCHEMA,
        "query_splitters": STORAGE_QUERY_SPLITTERS_SCHEMA,
        "mandatory_condition_checkers": STORAGE_MANDATORY_CONDITION_CHECKERS_SCHEMA,
        "allocation_policies": STORAGE_ALLOCATION_POLICIES_SCHEMA,
        "replacer_processor": STORAGE_REPLACER_PROCESSOR_SCHEMA,
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
        "readiness_state",
        "schema",
        "stream_loader",
    ],
    "additionalProperties": False,
}


# This is basically writable + 3 args
V1_CDC_STORAGE_SCHEMA = {
    "title": "Writable Storage Schema",
    "type": "object",
    "properties": {
        "version": {"const": "v1", "description": "Version of schema"},
        "kind": {"const": "cdc_storage", "description": "Component kind"},
        "name": {"type": "string", "description": "Name of the writable storage"},
        "storage": STORAGE_SCHEMA,
        "readiness_state": READINESS_STATE_SCHEMA,
        "schema": SCHEMA_SCHEMA,
        "stream_loader": STREAM_LOADER_SCHEMA,
        "default_control_topic": TYPE_STRING,
        "postgres_table": TYPE_STRING,
        "row_processor": CDC_STORAGE_ROW_PROCESSOR_SCHEMA,
        "query_processors": STORAGE_QUERY_PROCESSORS_SCHEMA,
        "query_splitters": STORAGE_QUERY_SPLITTERS_SCHEMA,
        "mandatory_condition_checkers": STORAGE_MANDATORY_CONDITION_CHECKERS_SCHEMA,
        "allocation_policies": STORAGE_ALLOCATION_POLICIES_SCHEMA,
        "replacer_processor": STORAGE_REPLACER_PROCESSOR_SCHEMA,
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
        "default_control_topic",
        "postgres_table",
        "row_processor",
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
        "storages": {
            "type": "array",
            "items": STORAGE_AND_MAPPER,
            "description": "An array of storages and their associated translation mappers",
        },
        "join_relationships": ENTITY_JOIN_RELATIONSHIPS,
        "storage_selector": {
            "type": "object",
            "properties": {
                "selector": {
                    "type": "string",
                    "description": "QueryStorageSelector class name",
                },
                "args": {
                    "type": "object",
                    "description": "Key/value mappings required to instantiate QueryStorageSelector class",
                },
            },
            "required": ["selector"],
            "additionalProperties": False,
        },
        "query_processors": {
            "type": "array",
            "items": ENTITY_QUERY_PROCESSOR,
            "description": "Represents a transformation applied to the ClickHouse query",
        },
        "validators": {
            "type": "array",
            "items": ENTITY_VALIDATOR,
            "description": "The validation logic used on the ClickHouse query",
        },
        "validate_data_model": {
            "type": ["string", "null"],
            "description": "The level at which mismatched functions and columns when querying the entity should be logged",
        },
        "required_time_column": {
            "type": ["string", "null"],
            "description": "The name of the required time column specifed in schema",
        },
        "partition_key_column_name": {
            "type": ["string", "null"],
            "description": "The column name, if this entity is partitioned, to select slice",
        },
        "subscription_processors": ENTITY_SUBSCRIPTION_PROCESSORS,
        "subscription_validators": ENTITY_SUBSCRIPTION_VALIDATORS,
    },
    "required": [
        "version",
        "kind",
        "schema",
        "name",
        "storages",
        "storage_selector",
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

with sentry_sdk.start_span(op="compile", description="Storage Validators"):
    STORAGE_VALIDATORS = {
        "readable_storage": fastjsonschema.compile(V1_READABLE_STORAGE_SCHEMA),
        "writable_storage": fastjsonschema.compile(V1_WRITABLE_STORAGE_SCHEMA),
        "cdc_storage": fastjsonschema.compile(V1_CDC_STORAGE_SCHEMA),
    }

with sentry_sdk.start_span(op="compile", description="Entity Validators"):
    ENTITY_VALIDATORS = {"entity": fastjsonschema.compile(V1_ENTITY_SCHEMA)}


with sentry_sdk.start_span(op="compile", description="Dataset Validators"):
    DATASET_VALIDATORS = {"dataset": fastjsonschema.compile(V1_DATASET_SCHEMA)}


ALL_VALIDATORS = {
    **STORAGE_VALIDATORS,
    **ENTITY_VALIDATORS,
    **DATASET_VALIDATORS,
    # TODO: MIGRATION_GROUP_VALIDATORS if migration groups will be config'd
}


V1_ALL_SCHEMAS = {
    "dataset": V1_DATASET_SCHEMA,
    "entity": V1_ENTITY_SCHEMA,
    "readable_storage": V1_READABLE_STORAGE_SCHEMA,
    "writable_storage": V1_WRITABLE_STORAGE_SCHEMA,
    "migration_group": V1_MIGRATION_GROUP_SCHEMA,
}
