from __future__ import annotations

import itertools
import uuid
from collections import ChainMap
from typing import Any, Mapping, MutableMapping, Type, Union

import jsonschema
import sentry_sdk
from snuba import environment
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.factory import get_entity
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.parser import parse_query
from snuba.query.schema import GENERIC_QUERY_SCHEMA, SNQL_QUERY_SCHEMA
from snuba.query.snql.parser import parse_snql_query
from snuba.request import Language, Request
from snuba.request.exceptions import JsonSchemaValidationException
from snuba.request.request_settings import (
    HTTPRequestSettings,
    RequestSettings,
    SubscriptionRequestSettings,
)
from snuba.schemas import Schema, validate_jsonschema
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "parser")


class RequestSchema:
    def __init__(
        self,
        query_schema: Schema,
        settings_schema: Schema,
        extensions_schemas: Mapping[str, Schema],
        settings_class: Type[RequestSettings] = HTTPRequestSettings,
        language: Language = Language.LEGACY,
    ):
        self.__query_schema = query_schema
        self.__settings_schema = settings_schema
        self.__extension_schemas = extensions_schemas
        self.__language = language

        self.__composite_schema: MutableMapping[str, Any] = {
            "type": "object",
            "properties": {},
            "required": [],
            "definitions": {},
            "additionalProperties": False,
        }
        self.__setting_class = settings_class

        for schema in itertools.chain(
            [self.__query_schema, self.__settings_schema],
            self.__extension_schemas.values(),
        ):
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
    def build_with_extensions(
        cls,
        extensions: Mapping[str, QueryExtension],
        settings_class: Type[RequestSettings],
        language: Language,
    ) -> RequestSchema:
        if language == Language.SNQL:
            generic_schema = SNQL_QUERY_SCHEMA
            extensions_schemas = {}
        else:
            generic_schema = GENERIC_QUERY_SCHEMA
            extensions_schemas = {
                extension_key: extension.get_schema()
                for extension_key, extension in extensions.items()
            }

        settings_schema = SETTINGS_SCHEMAS[settings_class]
        return cls(
            generic_schema,
            settings_schema,
            extensions_schemas,
            settings_class,
            language,
        )

    def validate(
        self, value: MutableMapping[str, Any], dataset: Dataset, referrer: str
    ) -> Request:
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

        class_name = self.__setting_class
        if isinstance(class_name, type(HTTPRequestSettings)):
            settings_obj: Union[
                HTTPRequestSettings, SubscriptionRequestSettings
            ] = class_name(**settings)
        elif isinstance(class_name, type(SubscriptionRequestSettings)):
            settings_obj = class_name()

        extensions = {}
        for extension_name, extension_schema in self.__extension_schemas.items():
            extensions[extension_name] = {
                key: value.pop(key)
                for key in extension_schema["properties"].keys()
                if key in value
            }

        if self.__language == Language.SNQL:
            query = parse_snql_query(query_body["query"], dataset)
        else:
            query = parse_query(query_body, dataset)
            apply_query_extensions(query, extensions, settings_obj)

        request_id = uuid.uuid4().hex
        return Request(
            request_id,
            # TODO: Replace this with the actual query raw body.
            # this can have an impact on subscriptions so we need
            # to be careful with the change.
            ChainMap(query_body, *extensions.values()),
            query,
            settings_obj,
            referrer,
        )

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


def apply_query_extensions(
    query: Query, extensions: Mapping[str, Mapping[str, Any]], settings: RequestSettings
) -> None:
    """
    Applies query extensions in place on an already parsed query.
    """

    query_entity = query.get_from_clause()
    entity = get_entity(query_entity.key)

    extensions_processors = entity.get_extensions()
    for name, extension in extensions_processors.items():
        with sentry_sdk.start_span(
            description=type(extension.get_processor()).__name__, op="extension"
        ):
            extension.get_processor().process_query(query, extensions[name], settings)
