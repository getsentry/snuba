import copy
import itertools
from collections import ChainMap
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Mapping

import jsonschema


CONDITION_OPERATORS = ['>', '<', '>=', '<=', '=', '!=', 'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL', 'LIKE', 'NOT LIKE']
POSITIVE_OPERATORS = ['>', '<', '>=', '<=', '=', 'IN', 'IS NULL', 'LIKE']

GENERIC_QUERY_SCHEMA = {
    'type': 'object',
    'properties': {
        'selected_columns': {
            'anyOf': [
                {'$ref': '#/definitions/column_name'},
                {'$ref': '#/definitions/column_list'},
                {'type': 'array', 'minItems': 0, 'maxItems': 0},
            ],
            'default': [],
        },
        'aggregations': {
            'type': 'array',
            'items': {
                'type': 'array',
                'items': [
                    {
                        # Aggregation function
                        # TODO this should eventually become more restrictive again.
                        'type': 'string',
                    }, {
                        # Aggregate column
                        'anyOf': [
                            {'$ref': '#/definitions/column_name'},
                            {'enum': ['']},
                            {'type': 'null'},
                        ],
                    }, {
                        # Alias
                        'type': ['string', 'null'],
                    },
                ],
                'minItems': 3,
                'maxItems': 3,
            },
            'default': [],
        },
        'arrayjoin': {
            '$ref': '#/definitions/column_name',
        },
        'sample': {
            'type': 'number',
            'min': 0,
        },
        'conditions': {
            'type': 'array',
            'items': {
                'anyOf': [
                    {'$ref': '#/definitions/condition'},
                    {
                        'type': 'array',
                        'items': {'$ref': '#/definitions/condition'},
                    },
                ],
            },
            'default': [],
        },
        'groupby': {
            'anyOf': [
                {'$ref': '#/definitions/column_name'},
                {'$ref': '#/definitions/column_list'},
                {'type': 'array', 'maxItems': 0},
            ],
            'default': [],
        },
        'totals': {
            'type': 'boolean',
            'default': False
        },
        'having': {
            'type': 'array',
            # HAVING looks just like a condition
            'items': {'$ref': '#/definitions/condition'},
            'default': [],
        },
        'orderby': {
            'anyOf': [
                {'$ref': '#/definitions/column_name'},
                {'$ref': '#/definitions/nested_expr'},
                {
                    'type': 'array',
                    'items': {
                        'anyOf': [
                            {'$ref': '#/definitions/column_name'},
                            {'$ref': '#/definitions/nested_expr'},
                        ],
                    }
                }
            ]
        },
        'limit': {
            'type': 'number',
            'default': 1000,
            'maximum': 10000,
        },
        'offset': {
            'type': 'number',
        },
        'limitby': {
            'type': 'array',
            'items': [
                {'type': 'number'},
                {'$ref': '#/definitions/column_name'},
            ]
        },
    },
    'dependencies': {
        'offset': ['limit'],
        'totals': ['groupby']
    },
    'additionalProperties': False,
    'definitions': {
        'column_name': {
            'type': 'string',
            'anyOf': [
                # This supports ClickHouse identifiers (with the addition of
                # the dot operator for referencing Nested columns, either in
                # aggregate or after an ARRAY JOIN), as well as an optional
                # subscript component at the end of the identifier (surrounded
                # by square brackets, e.g. `tags[key]`) that can be used for
                # treating Nested columns as mapping types in some contexts.
                {'pattern': r'^-?[a-zA-Z0-9_.]+(\[[a-zA-Z0-9_.:-]+\])?$'},
            ],
        },
        'column_list': {
            'type': 'array',
            'items': {
                'anyOf': [
                    {'$ref': '#/definitions/column_name'},
                    {'$ref': '#/definitions/nested_expr'},
                ]
            },
            'minItems': 1,
        },
        # TODO: can the complex nested expr actually be encoded here?
        'nested_expr': {'type': 'array'},
        # A condition is a 3-tuple of (column, operator, literal)
        # `conditions` is an array of conditions, or an array of arrays of conditions.
        # Conditions at the the top level are ANDed together.
        # Conditions at the second level are ORed together.
        # eg: [(a, =, 1), (b, =, 2)] => "a = 1 AND b = 2"
        # eg: [(a, =, 1), [(b, =, 2), (c, =, 3)]] => "a = 1 AND (b = 2 OR c = 3)"
        'condition': {
            'type': 'array',
            'items': [
                {
                    'anyOf': [
                        {'$ref': '#/definitions/column_name'},
                        {'$ref': '#/definitions/nested_expr'},
                    ],
                }, {
                    # Operator
                    'type': 'string',
                    # TODO  enforce literal = NULL for unary operators
                    'enum': CONDITION_OPERATORS,
                }, {
                    # Literal
                    'anyOf': [
                        {'type': ['string', 'number', 'null']},
                        {
                            'type': 'array',
                            'items': {'type': ['string', 'number']}
                        },
                    ],
                },
            ],
            'minItems': 3,
            'maxItems': 3,
        },
    }
}

PERFORMANCE_EXTENSION_SCHEMA = {
    'type': 'object',
    'properties': {
        # Never add FINAL to queries, enable sampling
        'turbo': {
            'type': 'boolean',
            'default': False,
        },
        # Force queries to hit the first shard replica, ensuring the query
        # sees data that was written before the query. This burdens the
        # first replica, so should only be used when absolutely necessary.
        'consistent': {
            'type': 'boolean',
            'default': False,
        },
        'debug': {
            'type': 'boolean',
            'default': False,
        },
    },
    'additionalProperties': False,
}

PROJECT_EXTENSION_SCHEMA = {
    'type': 'object',
    'properties': {
        'project': {
            'anyOf': [
                {'type': 'number'},
                {
                    'type': 'array',
                    'items': {'type': 'number'},
                    'minItems': 1,
                },
            ]
        },
    },
    # Need to select down to the project level for customer isolation and performance
    'required': ['project'],
    'additionalProperties': False,
}


def get_time_series_extension_properties(default_granularity: int, default_window: timedelta):
    return {
        'type': 'object',
        'properties': {
            'from_date': {
                'type': 'string',
                'format': 'date-time',
                'default': lambda: (datetime.utcnow().replace(microsecond=0) - default_window).isoformat()
            },
            'to_date': {
                'type': 'string',
                'format': 'date-time',
                'default': lambda: datetime.utcnow().replace(microsecond=0).isoformat()
            },
            'granularity': {
                'type': 'number',
                'default': default_granularity,
            },
        },
        'additionalProperties': False,
    }


@dataclass
class Request:
    query: Mapping[str, Any]
    extensions: Mapping[str, Mapping[str, Any]]

    def __post_init__(self):
        self.body = ChainMap(self.query, *self.extensions.values())


class RequestSchema:
    def __init__(self, query_schema, extensions_schemas: Mapping[str, Any]):
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

    def validate(self, value) -> Request:
        # XXX: Mutates input value!
        validate(value, self.__composite_schema)

        query = {key: value.pop(key) for key in self.__query_schema['properties'].keys() if key in value}

        extensions = {}
        for extension_name, extension_schema in self.__extension_schemas.items():
            extensions[extension_name] = {key: value.pop(key) for key in extension_schema['properties'].keys() if key in value}

        return Request(query, extensions)


EVENTS_QUERY_SCHEMA = RequestSchema(GENERIC_QUERY_SCHEMA, {
    'performance': PERFORMANCE_EXTENSION_SCHEMA,
    'project': PROJECT_EXTENSION_SCHEMA,
    'timeseries': get_time_series_extension_properties(
        default_granularity=3600,
        default_window=timedelta(days=5),
    ),
})

SDK_STATS_SCHEMA = RequestSchema({
    'type': 'object',
    'properties': {
        'groupby': {
            'type': 'array',
            'items': {
                # at the moment the only additional thing you can group by is project_id
                'enum': ['project_id']
            },
            'default': [],
        },
    },
    'additionalProperties': False,
}, {
    'timeseries': get_time_series_extension_properties(
        default_granularity=86400,  # SDK stats query defaults to 1-day bucketing
        default_window=timedelta(days=1),
    ),
})


def validate(value, schema, set_defaults=True):
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

    validator_cls = jsonschema.validators.extend(
        jsonschema.Draft4Validator,
        {'properties': validate_and_default}
    ) if set_defaults else jsonschema.Draft6Validator

    validator_cls(
        schema,
        types={'array': (list, tuple)},
        format_checker=jsonschema.FormatChecker()
    ).validate(value, schema)


def generate(schema):
    """
    Generate a (not necessarily valid) object that can be used as a template
    from the provided schema
    """
    typ = schema.get('type')
    if 'default' in schema:
        default = schema['default']
        return default() if callable(default) else default
    elif typ == 'object':
        return {prop: generate(subschema) for prop, subschema in schema.get('properties', {}).items()}
    elif typ == 'array':
        return []
    elif typ == 'string':
        return ""
    return None
