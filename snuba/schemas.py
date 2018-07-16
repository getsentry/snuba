from datetime import datetime, timedelta
import jsonschema
import copy
import six

QUERY_SCHEMA = {
    'type': 'object',
    'properties': {
        # A condition is a 3-tuple of (column, operator, literal)
        # `conditions` is an array of conditions, or an array of arrays of conditions.
        # Conditions at the the top level are ANDed together.
        # Conditions at the second level are ORed together.
        # eg: [(a, =, 1), (b, =, 2)] => "a = 1 AND b = 2"
        # eg: [(a, =, 1), [(b, =, 2), (c, =, 3)]] => "a = 1 AND (b = 2 OR c = 3)"
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
        'having': {
            'type': 'array',
            # HAVING looks just like a condition
            'items': {'$ref': '#/definitions/condition'},
            'default': [],
        },
        'from_date': {
            'type': 'string',
            'format': 'date-time',
            'default': lambda: (datetime.utcnow().replace(microsecond=0) - timedelta(days=5)).isoformat()
        },
        'to_date': {
            'type': 'string',
            'format': 'date-time',
            'default': lambda: datetime.utcnow().replace(microsecond=0).isoformat()
        },
        'granularity': {
            'type': 'number',
            'default': 3600,
        },
        'issues': {
            'type': 'array',
            'items': {
                'type': 'array',
                'minItems': 3,
                'maxItems': 3,
                'items': [
                    {'type': 'number'},
                    {'type': 'number'},
                    {
                        'type': 'array',
                        'items': {
                            'anyOf': [
                                {'$ref': '#/definitions/fingerprint_hash'},
                                {'$ref': '#/definitions/fingerprint_hash_with_tombstone'},
                            ],
                        },
                        'minItems': 1,
                    },
                ],
            },
            'default': [],
        },
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
        'groupby': {
            'anyOf': [
                {'$ref': '#/definitions/column_name'},
                {'$ref': '#/definitions/column_list'},
                {'type': 'array', 'maxItems': 0},
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
                'minLength': 3,
                'maxLength': 3,
            },
            'minLength': 1,
            'default': [],
        },
        'arrayjoin': {
            '$ref': '#/definitions/column_name',
        },
        'orderby': {
            '$ref': '#/definitions/column_name',
        },
        'limit': {
            'type': 'number'
        },
        'offset': {
            'type': 'number',
        },
        'selected_columns': {
            'anyOf': [
                {'$ref': '#/definitions/column_name'},
                {'$ref': '#/definitions/column_list'},
                {'type': 'array', 'minItems': 0, 'maxItems': 0},
            ],
            'default': [],
        },
    },
    # Need to select down to the project level for customer isolation and performance
    'required': ['project'],
    'dependencies': {
        'offset': ['limit'],
        'limit': ['orderby'],
    },

    'definitions': {
        'fingerprint_hash': {
            'type': 'string',
            'minLength': 32,
            'maxLength': 32,
            'pattern': '^[0-9a-f]{32}$',
        },
        'fingerprint_hash_with_tombstone': {
            'type': 'array',
            'items': [
                {'$ref': '#/definitions/fingerprint_hash'},
                {
                    'anyOf': [
                        {'type': 'null'},
                        {'pattern': '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'},
                    ]
                },
            ],
        },
        'column_name': {
            'type': 'string',
            'anyOf': [
                # Special computed column created from `issues` definition
                {'enum': ['issue', '-issue']},
                {'pattern': '^-?[a-zA-Z0-9_.]+$', },
                {'pattern': '^-?tags\[[a-zA-Z0-9_.:-]+\]$', },
            ],
        },
        'column_list': {
            'type': 'array',
            'items': {'$ref': '#/definitions/column_name'},
            'minItems': 1,
        },
        'condition': {
            'type': 'array',
            'items': [
                {
                    'anyOf': [
                        {'$ref': '#/definitions/column_name'},
                        # anything allowed by util.ESCAPE_RE
                        {'pattern': '^[a-zA-Z][a-zA-Z0-9_\.(), ]+$'},
                    ],
                }, {
                    # Operator
                    'type': 'string',
                    # TODO  enforce literal = NULL for unary operators
                    'enum': ['>', '<', '>=', '<=', '=', '!=', 'IN', 'IS NULL', 'IS NOT NULL', 'LIKE', 'NOT LIKE'],
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
            'minLength': 3,
            'maxLength': 3,
        }
    }
}


def validate(value, schema, set_defaults=True):
    orig = jsonschema.Draft4Validator.VALIDATORS['properties']

    def validate_and_default(validator, properties, instance, schema):
        for property, subschema in six.iteritems(properties):
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
    ) if set_defaults else jsonschema.Draft4Validator

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
        return {prop: generate(subschema) for prop, subschema in six.iteritems(schema.get('properties', {}))}
    elif typ == 'array':
        return []
    elif typ == 'string':
        return ""
    return None
