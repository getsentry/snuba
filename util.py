from flask import request, render_template
from datetime import date, datetime
import json
import jsonschema
import requests
import six

import schemas
import settings

def escape_literal(value):
    # TODO this escaping is garbage
    if isinstance(value, six.string_types):
        value = value.replace("'", "\\'")
        return "'{}'".format(value)
    elif isinstance(value, (datetime, date)):
        return "toDateTime('{}')".format(value.strftime("%Y-%m-%dT%H:%M:%S"))
    else:
        return str(value)

def granularity_group(unit):
    return {
        'hour': 'toHour(timestamp)',
        'minute': 'toMinute(timestamp)',
    }.get(unit, 'toHour(timestamp)')


def raw_query(sql):
    sql = sql + ' FORMAT JSON'
    result = requests.get(
        settings.CLICKHOUSE_SERVER,
        params={'query': sql},
    ).text
    return result

def issue_expr(issues, col='primary_hash'):
    """
    Takes a list of (issue_id, fingerprint(s)) tuples of the form:

        [(1, (hash1, hash2)), (2, hash3)]

    and constructs a nested SQL if() expression to return the issue_id of the
    matching fingerprint expression when evaluated on the given column_name.

        if(col in (hash1, hash2), 1, if(col = hash3, 2), NULL)

    """
    if len(issues) == 0:
        return 0
    else:
        issue_id, hashes = issues[0]

        if hasattr(hashes, '__iter__'):
            predicate = "{} IN ('{}')".format(col, "', '".join(hashes))
        else:
            predicate = "{} = '{}'".format(col, hashes)

        return 'if({}, {}, {})'.format(predicate, issue_id, issue_expr(issues[1:], col=col))

def validate_request(schema):
    """
    Decorator to validate that a request body matches the given schema.
    """
    def validator(func):
        def wrapper(*args, **kwargs):
            try:
                body = json.loads(request.data)
                schemas.validate(body, schema)
                setattr(request, 'validated_body', body)
            except (ValueError, jsonschema.ValidationError) as e:
                return (render_template('error.html',
                    error=str(e),
                    schema=json.dumps(schema, indent=4, sort_keys=True, default=default_encode)
                ), 400)
            return func(*args, **kwargs)
        return wrapper
    return validator

def default_encode(value):
    if callable(value):
        return value()
    else:
        raise TypeError()
