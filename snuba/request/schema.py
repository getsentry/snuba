from __future__ import annotations

import itertools
from typing import Any, Mapping, MutableMapping, NamedTuple, Type

import jsonschema

from snuba import environment
from snuba.query.schema import SNQL_QUERY_SCHEMA
from snuba.request.exceptions import JsonSchemaValidationException
from snuba.request.request_settings import (
    HTTPRequestSettings,
    RequestSettings,
    SubscriptionRequestSettings,
)
from snuba.schemas import Schema, validate_jsonschema
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "parser")


class RequestParts(NamedTuple):
    query: Mapping[str, Any]
    settings: Mapping[str, Any]


class RequestSchema:
    def __init__(
        self,
        query_schema: Schema,
        settings_schema: Schema,
        settings_class: Type[RequestSettings] = HTTPRequestSettings,
    ):
        self.__query_schema = query_schema
        self.__settings_schema = settings_schema

        self.__composite_schema: MutableMapping[str, Any] = {
            "type": "object",
            "properties": {},
            "required": [],
            "definitions": {},
            "additionalProperties": False,
        }
        self.__setting_class = settings_class

        for schema in itertools.chain([self.__query_schema, self.__settings_schema]):
            assert schema["type"] == "object", "subschema must be object"
            assert (
                schema["additionalProperties"] is False
            ), "subschema must not allow additional properties"
            self.__composite_schema["required"].extend(schema.get("required", []))

            for property_name, property_schema in schema["properties"].items():
                assert (
                    property_name not in self.__composite_schema["properties"]
                ), "subschema cannot redefine property"
                self.__composite_schema["properties"][property_name] = property_schema

            for definition_name, definition_schema in schema.get(
                "definitions", {}
            ).items():
                assert (
                    definition_name not in self.__composite_schema["definitions"]
                ), "subschema cannot redefine definition"
                self.__composite_schema["definitions"][
                    definition_name
                ] = definition_schema

        self.__composite_schema["required"] = set(self.__composite_schema["required"])

    @classmethod
    def build(cls, settings_class: Type[RequestSettings]) -> RequestSchema:
        generic_schema = SNQL_QUERY_SCHEMA
        settings_schema = SETTINGS_SCHEMAS[settings_class]
        return cls(generic_schema, settings_schema, settings_class)

    def validate(self, value: MutableMapping[str, Any]) -> RequestParts:
        try:
            value = validate_jsonschema(value, self.__composite_schema)
        except jsonschema.ValidationError as error:
            raise JsonSchemaValidationException(str(error)) from error

        query_body = {
            key: value.pop(key)
            for key in self.__query_schema["properties"].keys()
            if key in value
        }
        settings = {
            key: value.pop(key)
            for key in self.__settings_schema["properties"].keys()
            if key in value
        }

        return RequestParts(query=query_body, settings=settings)

    def __generate_template_impl(self, schema: Mapping[str, Any]) -> Any:
        """
        Generate a (not necessarily valid) object that can be used as a template
        from the provided schema
        """
        typ = schema.get("type")
        if "default" in schema:
            default = schema["default"]
            return default() if callable(default) else default
        elif typ == "object":
            return {
                prop: self.__generate_template_impl(subschema)
                for prop, subschema in schema.get("properties", {}).items()
            }
        elif typ == "array":
            return []
        elif typ == "string":
            return ""
        return None

    def generate_template(self) -> Any:
        return self.__generate_template_impl(self.__composite_schema)


SETTINGS_SCHEMAS: Mapping[Type[RequestSettings], Schema] = {
    HTTPRequestSettings: {
        "type": "object",
        "properties": {
            # Never add FINAL to queries, enable sampling
            "turbo": {"type": "boolean", "default": False},
            # Force queries to hit the first shard replica, ensuring the query
            # sees data that was written before the query. This burdens the
            # first replica, so should only be used when absolutely necessary.
            "consistent": {"type": "boolean", "default": False},
            "debug": {"type": "boolean", "default": False},
            # Don't actually run the query Clickhouse, just generate the SQL
            # and return it.
            "dry_run": {"type": "boolean", "default": False},
            # Flags if this a legacy query that was automatically generated by the SnQL SDK
            "legacy": {"type": "boolean", "default": False},
            # Team and feature are used for resource attribution
            "team": {"type": "string", "default": "<unknown>"},
            "feature": {"type": "string", "default": "<unknown>"},
        },
        "additionalProperties": False,
    },
    # Subscriptions have no customizable settings.
    SubscriptionRequestSettings: {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
}
