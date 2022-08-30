from __future__ import annotations

TYPE_STRING = {"type": "string"}
TYPE_NULLABLE_STRING = {"type": ["string", "null"]}
TYPE_NULLABLE_INTEGER = {"type": ["integer", "null"]}
NULLABLE_DISALLOWED_AGGREGATIONS_SCHEMA = {
    "type": ["array", "null"],
    "items": TYPE_STRING,
}

V1_ENTITY_SUBSCIPTION_SCHEMA = {
    "type": "object",
    "properties": {
        "version": {"const": "v1"},
        "kind": {"const": "entity_subscription"},
        "name": TYPE_STRING,
        "parent_subscription_entity_class": TYPE_STRING,
        "max_allowed_aggregations": TYPE_NULLABLE_INTEGER,
        "disallowed_aggregations": NULLABLE_DISALLOWED_AGGREGATIONS_SCHEMA,
    },
    "required": [
        "version",
        "kind",
        "name",
        "parent_subscription_entity_class",
    ],
}
