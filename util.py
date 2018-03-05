from datetime import date, datetime
import json
import requests
import six

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
