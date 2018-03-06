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

def group_expr(groups, column_name='primary_hash'):
    """
    Takes a list of (group_id, fingerprint(s)) tuples of the form:

        [(1, (hash1, hash2)), (2, hash3)]

    and constructs a nested SQL if() expression to return the group_id of the
    matching fingerprint expression when evaluated on the given column_name.

        if(col in (hash1, hash2), 1, if(col = hash3, 2), NULL)

    """
    if len(groups) == 0:
        return 'NULL'
    else:
        group_id, hashes = groups[0]
        predicate = '{} = {}' if hasattr(hashes, '__iter__') else '{} IN {}'
        predicate = predicate.format(column_name, hashes)
        return 'if({}, {}, {})'.format(predicate, group_id, group_expr(groups[1:], column_name=column_name))

def validate_query(query):
    jsonschema.validate(query, schemas.QUERY_SCHEMA)
