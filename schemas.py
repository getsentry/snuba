from datetime import datetime, timedelta
import isodate
import jsonschema

QUERY_SCHEMA = {
    'type': 'object',
    'properties': {
        'conditions': {
            'type': 'array', # TODO conditions probably need more validation
            'items': {'type': 'string'},
            'default': list,
        },
        'from_date': {
            'type': 'string',
            'format': 'date-time',
            'default': lambda: isodate.datetime_isoformat(datetime.utcnow() - timedelta(days=1))
        },
        'to_date': {
            'type': 'string',
            'format': 'date-time',
            'default': lambda: isodate.datetime_isoformat(datetime.utcnow())
        },
        'unit': {
            'type': 'string',
            'enum': ['hour', 'day', 'minute'],
            'default': 'hour',
        },
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
    'required': [],

    'definitions': {
        'fingerprint_hash': {
            'type': 'string',
            'minLength': 16,
            'maxLength': 16,
            'pattern': '^[0-9a-f]{16}$',
        }
    }
}

def validate(value, schema, set_defaults=True):
    orig = jsonschema.Draft4Validator.VALIDATORS["properties"]

    def validate_and_default(validator, properties, instance, schema):
        for property, subschema in properties.iteritems():
            if "default" in subschema:
                if callable(subschema["default"]):
                    instance.setdefault(property, subschema["default"]())
                else:
                    instance.setdefault(property, subschema["default"])

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
