from __future__ import annotations

import itertools

from collections import ChainMap
from dataclasses import dataclass
from deprecation import deprecated
from typing import Any, Mapping

from snuba.query.extensions import QueryExtension
from snuba.query.query import Query
from snuba.query.schema import GENERIC_QUERY_SCHEMA, SETTINGS_SCHEMA
from snuba.request.request_settings import RequestSettings
from snuba.schemas import Schema, validate_jsonschema


@dataclass(frozen=True)
class Request:
    query: Query
    settings: RequestSettings  # settings provided by the request
    extensions: Mapping[str, Mapping[str, Any]]

    @property
    @deprecated(
        details="Do not access the internal query representation "
        "use the specific accessor methods on the query object instead.")
    def body(self):
        return ChainMap(self.query.get_body(), *self.extensions.values())


class RequestSchema:
    def __init__(self, query_schema: Schema, settings_schema: Schema, extensions_schemas: Mapping[str, Schema]):
        self.__query_schema = query_schema
        self.__settings_schema = settings_schema
        self.__extension_schemas = extensions_schemas

        self.__composite_schema = {
            'type': 'object',
            'properties': {},
            'required': [],
            'definitions': {},
            'additionalProperties': False,
        }

        for schema in itertools.chain([self.__query_schema, self.__settings_schema], self.__extension_schemas.values()):
            assert schema['type'] == 'object', 'subschema must be object'
            assert schema['additionalProperties'] is False, 'subschema must not allow additional properties'
            self.__composite_schema['required'].extend(schema.get('required', []))

            for property_name, property_schema in schema['properties'].items():
                assert property_name not in self.__composite_schema['properties'], 'subschema cannot redefine property'
                self.__composite_schema['properties'][property_name] = property_schema

            for definition_name, definition_schema in schema.get('definitions', {}).items():
                assert definition_name not in self.__composite_schema['definitions'], 'subschema cannot redefine definition'
                self.__composite_schema['definitions'][definition_name] = definition_schema

        self.__composite_schema['required'] = set(self.__composite_schema['required'])

    @classmethod
    def build_with_extensions(cls, extensions: Mapping[str, QueryExtension]) -> RequestSchema:
        generic_schema = GENERIC_QUERY_SCHEMA
        settings_schema = SETTINGS_SCHEMA
        extensions_schemas = {
            extension_key: extension.get_schema()
            for extension_key, extension
            in extensions.items()
        }
        return cls(generic_schema, settings_schema, extensions_schemas)

    def validate(self, value) -> Request:
        value = validate_jsonschema(value, self.__composite_schema)

        query_body = {key: value.pop(key) for key in self.__query_schema['properties'].keys() if key in value}
        settings = {key: value.pop(key) for key in self.__settings_schema['properties'].keys() if key in value}

        extensions = {}
        for extension_name, extension_schema in self.__extension_schemas.items():
            extensions[extension_name] = {key: value.pop(key) for key in extension_schema['properties'].keys() if key in value}

        return Request(
            Query(query_body),
            RequestSettings(settings['turbo'], settings['consistent'], settings['debug']),
            extensions
        )

    def __generate_template_impl(self, schema) -> Any:
        """
        Generate a (not necessarily valid) object that can be used as a template
        from the provided schema
        """
        typ = schema.get('type')
        if 'default' in schema:
            default = schema['default']
            return default() if callable(default) else default
        elif typ == 'object':
            return {prop: self.__generate_template_impl(subschema) for prop, subschema in schema.get('properties', {}).items()}
        elif typ == 'array':
            return []
        elif typ == 'string':
            return ""
        return None

    def generate_template(self) -> Any:
        return self.__generate_template_impl(self.__composite_schema)
