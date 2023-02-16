from typing import Any, Mapping, MutableMapping, Optional

from arroyo.processing.strategies.decoder import JsonCodec

from snuba.utils.streams.topics import Topic

_HARDCODED_SCHEMAS: Mapping[Topic, Mapping[str, Any]] = {
    Topic.GENERIC_METRICS: {
        "$schema": "http://json-schema.org/draft-2020-12/schema#",
        "$ref": "#/definitions/Main",
        "definitions": {
            "Main": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "use_case_id": {"type": "string"},
                    "org_id": {"type": "integer"},
                    "project_id": {"type": "integer"},
                    "metric_id": {"type": "integer"},
                    "type": {"type": "string"},
                    "timestamp": {"type": "integer"},
                    "tags": {"$ref": "#/definitions/IntToInt"},
                    "value": {"type": "array", "items": {"type": "number"}},
                    "retention_days": {"type": "integer"},
                    "mapping_meta": {"$ref": "#/definitions/MappingMeta"},
                },
                "required": [
                    "mapping_meta",
                    "metric_id",
                    "org_id",
                    "project_id",
                    "retention_days",
                    "tags",
                    "timestamp",
                    "type",
                    "use_case_id",
                    "value",
                ],
                "title": "Main",
            },
            "MappingMeta": {
                "type": "object",
                "additionalProperties": False,
                "patternProperties": {
                    "^[chdfr]$": {"$ref": "#/definitions/IntToString"}
                },
                "title": "MappingMeta",
            },
            "IntToInt": {
                "type": "object",
                "patternProperties": {"^[0-9]$": {"type": "integer"}},
                "title": "IntToInt",
            },
            "IntToString": {
                "type": "object",
                "patternProperties": {"^[0-9]$": {"type": "string"}},
                "title": "IntToString",
            },
        },
    }
}


def get_schema(topic: Topic) -> Optional[Mapping[str, Any]]:
    """
    This is a placeholder. Eventually the schema will be fetched from the
    sentry-kafka-topics library when it gets published.

    This function returns either the schema if it is defined, or None if not.

    """
    return _HARDCODED_SCHEMAS.get(topic)


_cache: MutableMapping[Topic, JsonCodec] = {}


def get_json_codec(topic: Topic) -> JsonCodec:
    if topic not in _cache:
        _cache[topic] = JsonCodec(get_schema(topic))

    return _cache[topic]
