from datetime import datetime, timedelta
import jsonschema
import copy

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
                        'minItems': 2,
                    },
                ],
            },
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
                'minItems': 2,
                'maxItems': 2,
                'items': [
                    {'type': 'number'},
                    {
                        'anyOf': [
                            {'$ref': '#/definitions/fingerprint_hash'},
                            {
                                'type': 'array',
                                'items': {'$ref': '#/definitions/fingerprint_hash'},
                                'minItems': 1,
                            },
                        ],
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
            'default': 'time',
        },
        'aggregations': {
            'type': 'array',
            'items': {
                'type': 'array',
                'items': [
                    {
                        # Aggregation function
                        'type': 'string',
                        'anyOf': [
                            {'enum': ['count', 'uniq', 'min', 'max']},
                            {'pattern': 'topK\(\d+\)'},
                        ],
                    }, {
                        # Aggregate column
                        'anyOf': [
                            {'$ref': '#/definitions/column_name'},
                            {'enum': ['']},
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
            'default': [['count', '', 'aggregate']],
        },
        'arrayjoin': {
            '$ref': '#/definitions/column_name',
        },
        'orderby': {
            '$ref': '#/definitions/column_name',
            'default': 'time',
        },
        'limit': {
            'type': 'number'
        },
        'offset': {
            'type': 'number',
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
        'column_name': {
            'type': 'string',
            'anyOf': [
                {'enum': ['issue', '-issue']},  # Special computed column created from `issues` definition
                {'pattern': '^-?[a-zA-Z0-9_.]+$',},
                {'pattern': '^-?tags\[[a-zA-Z0-9_.:-]+\]$',},
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
                    '$ref': '#/definitions/column_name'
                }, {
                    # Operator
                    'type': 'string',
                    # TODO  enforce literal = NULL for unary operators
                    'enum': ['>', '<', '>=', '<=', '=', '!=', 'IN', 'IS NULL', 'IS NOT NULL'],
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
        for property, subschema in properties.iteritems():
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
