from __future__ import annotations

import copy
import itertools
import jsonschema

from collections import ChainMap
from dataclasses import dataclass
from typing import Any, Mapping

from snuba.query.extensions import QueryExtension
from snuba.query.query import Query
from snuba.schemas import Schema, GENERIC_QUERY_SCHEMA


@dataclass(frozen=True)
class Request:
    query: Query
    extensions: Mapping[str, Mapping[str, Any]]

    @property
    def body(self):
        return ChainMap(self.query.get_body(), *self.extensions.values())


class RequestSchema:
    def __init__(self, query_schema: Schema, extensions_schemas: Mapping[str, Schema]):
        self.__query_schema = query_schema
        self.__extension_schemas = extensions_schemas

        self.__composite_schema = {
            'type': 'object',
            'properties': {},
            'required': [],
            'definitions': {},
            'additionalProperties': False,
        }

        for schema in itertools.chain([self.__query_schema], self.__extension_schemas.values()):
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
        extensions_schemas = {
            extension_key: extension.get_schema()
            for extension_key, extension
            in extensions.items()
        }
        return RequestSchema(
            generic_schema,
            extensions_schemas,
        )

    def validate(self, value) -> Request:
        value = validate_jsonschema(value, self.__composite_schema)

        query_body = {key: value.pop(key) for key in self.__query_schema['properties'].keys() if key in value}

        extensions = {}
        for extension_name, extension_schema in self.__extension_schemas.items():
            extensions[extension_name] = {key: value.pop(key) for key in extension_schema['properties'].keys() if key in value}

        return Request(Query(query_body), extensions)

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


def validate_jsonschema(value, schema, set_defaults=True):
    """
    Validates a value against the provided schema, returning the validated
    value if the value conforms to the schema, otherwise raising a
    ``jsonschema.ValidationError``.
    """
    orig = jsonschema.Draft6Validator.VALIDATORS['properties']

    def validate_and_default(validator, properties, instance, schema):
        for property, subschema in properties.items():
            if 'default' in subschema:
                if callable(subschema['default']):
                    instance.setdefault(property, subschema['default']())
                else:
                    instance.setdefault(property, copy.deepcopy(subschema['default']))

        for error in orig(validator, properties, instance, schema):
            yield error

    # Using schema defaults during validation will cause the input value to be
    # mutated, so to be on the safe side we create a deep copy of that value to
    # avoid unwanted side effects for the calling function.
    if set_defaults:
        value = copy.deepcopy(value)

    validator_cls = jsonschema.validators.extend(
        jsonschema.Draft4Validator,
        {'properties': validate_and_default}
    ) if set_defaults else jsonschema.Draft6Validator

    validator_cls(
        schema,
        types={'array': (list, tuple)},
        format_checker=jsonschema.FormatChecker()
    ).validate(value, schema)

    return value
