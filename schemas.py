QUERY_SCHEMA = {
    'type': 'object',
    'properties': {
        'conditions': {
            'type': 'array', # TODO conditions probably need more validation
        },
        'from_date': {'type': 'string'},
        'to_date': {'type': 'string'},
        'unit': {'type': 'string', 'enum': ['hour', 'day', 'minute']},
        'groups': {
            'type': 'array',
            'items': {
                'type': 'array',
                'minItems': 2,
                'maxItems': 2,
                'items': [
                    {'type': 'number'},
                    {
                        'anyOf': [
                            {"$ref": "#/definitions/fingerprint_hash"},
                            {
                                'type': 'array',
                                'items': {"$ref": "#/definitions/fingerprint_hash"}
                            },
                        ]
                    }
                ],
            }
        },
    },

    'definitions': {
        'fingerprint_hash': {
            'type': 'string',
            'minLength': 16,
            'maxLength': 16,
            'pattern': '^[0-9a-f]{16}$',
        }
    }
}
