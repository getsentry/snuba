from flask import request, render_template
from datetime import date, datetime
from dateutil.parser import parse as parse_datetime
from dateutil.tz import tz
import json
import jsonschema
import numbers
import requests
import six

import schemas
import settings

def escape_literal(value):
    """
    Escape a literal value for use in a SQL clause
    """
    if isinstance(value, six.string_types):
        value = value.replace("'", "\\'") # TODO this escaping is garbage
        return "'{}'".format(value)
    elif isinstance(value, datetime):
        value = value.replace(tzinfo=None, microsecond=0)
        return "toDateTime('{}')".format(value.isoformat())
    elif isinstance(value, date):
        return "toDate('{}')".format(value.isoformat())
    elif isinstance(value, list):
        return "({})".format(', '.join(escape_literal(v) for v in value))
    elif isinstance(value, numbers.Number):
        return str(value)
    else:
        raise ValueError('Do not know how to escape {} for SQL'.format(type(value)))

def raw_query(sql):
    """
    Submit a raw SQL query to clickhouse and do some post-processing on it to
    fix some of the formatting issues in the result JSON
    """
    sql = sql + ' FORMAT JSON'
    response = requests.get(
        settings.CLICKHOUSE_SERVER,
        params={'query': sql},
    )
    # TODO handle query failures / retries

    try:
        result = json.loads(response.text)
    except ValueError as e:
        print response
        raise e
    assert 'meta' in result
    assert 'data' in result

    # Fix up various anomalies in clickhouse JSON
    for col in result['meta']:
        # Clickhouse sends UInt64's as JSON strings
        if col['type'] == 'UInt64':
            for d in result['data']:
                try:
                    d[col['name']] = int(d[col['name']])
                except:
                    pass
        # Convert naive datetime strings back to TZ aware ones
        elif col['type'] == "DateTime('Etc/Zulu')":
            for d in result['data']:
                d[col['name']] = parse_datetime(
                    d[col['name']]
                ).replace(tzinfo=tz.tzutc()).isoformat()

    # TODO reformat rows into tuples instead of dicts, or column arrays
    # TODO record statistics somewhere
    return { k: result[k] for k in ['data', 'meta']}

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

            def default_encode(value):
                if callable(value):
                    return value()
                else:
                    raise TypeError()

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

