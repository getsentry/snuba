from __future__ import annotations

from snuba.datasets.configuration.json_schema import TYPE_STRING

REQUIRED_TYPE_STRING = {"type": "string"}

MIGRATION_GROUP_SCHEMA = {
    "type": "object",
    "properties": {
        "version": REQUIRED_TYPE_STRING,
        "type": REQUIRED_TYPE_STRING,
        "name": REQUIRED_TYPE_STRING,
        "optional": {"type": "boolean"},
        "migrations": {
            "type": "array",
            "items": TYPE_STRING,
        },
    },
}
