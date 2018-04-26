from flask import request, render_template

from datetime import date, datetime
from dateutil.tz import tz
import simplejson as json
from itertools import chain
import jsonschema
import numbers
import re
import requests
import os
import six

import schemas

from snuba import settings


def to_list(value):
    return value if isinstance(value, list) else [value]


def column_expr(column_name, body, alias=None, aggregate=None):
    """
    Certain special column names expand into more complex expressions. Return
    a 2-tuple of:
        (expanded column expression, sanitized alias)

    Needs the body of the request for some extra data used to expand column expressions.
    """
    assert column_name or aggregate
    column_name = column_name or ''

    if alias is None:
        alias = column_name if column_name else aggregate

    if column_name == settings.TIME_GROUP_COLUMN:
        expr = settings.TIME_GROUPS.get(body['granularity'], settings.DEFAULT_TIME_GROUP)
    elif column_name == 'issue':
        # If there are conditions on what 'issue' can be, then only expand the
        # expression for the issues that will actually be selected.
        cond = flat_conditions(body.get('conditions', []))
        ids = [set([lit]) for (col, op, lit) in cond if col == 'issue' and op == '='] +\
              [set(lit) for (col, op, lit) in cond if col ==
               'issue' and op == 'IN' and isinstance(lit, list)]
        ids = set.union(*ids) if ids else None
        expr = issue_expr(body['issues'], ids=ids) if body['issues'] is not None else None
    elif settings.NESTED_COL_EXPR.match(column_name):
        # For tags/contexts, we expand the expression depending on whether the tag is
        # "promoted" to a top level column, or whether we have to look in the tags map.
        match = settings.NESTED_COL_EXPR.match(column_name)
        col, sub = match.group(1), match.group(2)
        sub_field = sub.replace('.', '_')
        if col in settings.PROMOTED_COLS and sub_field in settings.PROMOTED_COLS[col]:
            expr = sub_field  # TODO recurse?
        else:
            expr = u'{col}.value[indexOf({col}.key, {sub})]'.format(**{
                'col': col,
                'sub': escape_literal(sub)
            })
    elif column_name in ['tags_key', 'tags_value']:
        # For special columns `tags_key` and `tags_value` we construct a an interesting
        # arrayjoin that enumerates all promoted and non-promoted keys. This is for
        # cases where we do not have a tag key to filter on (eg top tags).
        col, typ = column_name.split('_', 1)
        promoted = settings.PROMOTED_COLS[col]

        key_list = u'arrayConcat([{}], {}.key)'.format(
            u', '.join(u'\'{}\''.format(p) for p in promoted),
            col
        )
        val_list = u'arrayConcat([{}], {}.value)'.format(
            # TODO os_rooted is actually the only one that needs a toString()
            ', '.join('toString({})'.format(p) for p in promoted),
            col
        )
        expr = (u'arrayJoin(arrayMap((x,y) -> [x,y], {}, {}))').format(
            key_list,
            val_list
        )
        # alias sub-expression for later reuse
        expr = alias_expr(expr, 'all_tags', body)
        expr = u'({})'.format(expr) + ('[1]' if typ == 'key' else '[2]')
    else:
        expr = column_name

    if aggregate is not None:
        if not expr:
            expr = aggregate
        else:
            expr = u'{}({})'.format(aggregate, expr)

    return alias_expr(expr, alias, body)


def alias_expr(expr, alias, body):
    """
    Return the correct expression to use in the final SQL. Keeps a cache of
    the previously created expressions and aliases, so it knows when it can
    subsequently replace a redundant expression with an alias.

    1. If the expression and alias are equal, just return that.
    2. Otherwise, if the expression is new, add it to the cache and its alias so
       it can be reused later and return `expr AS alias`
    3. If the expression has been aliased before, return the alias
    """
    alias_cache = body.setdefault('alias_cache', {})

    if expr == alias:
        return expr
    elif expr in alias_cache:
        return u'`{}`'.format(alias_cache[expr])
    else:
        alias_cache[expr] = alias
        return u'({} AS `{}`)'.format(expr, alias)


def is_condition(cond_or_list):
    return len(cond_or_list) == 3 and isinstance(cond_or_list[0], six.string_types)


def flat_conditions(conditions):
    return list(chain(*[[c] if is_condition(c) else c for c in conditions]))


def condition_expr(conditions, body, depth=0):
    """
    Return a boolean expression suitable for putting in the WHERE clause of the
    query.  The expression is constructed by ANDing groups of OR expressions.
    Expansion of columns is handled, as is replacement of columns with aliases,
    if the column has already been expanded and aliased elsewhere.
    """
    if not conditions:
        return ''

    if depth == 0:
        sub = (condition_expr(cond, body, depth + 1) for cond in conditions)
        return u' AND '.join(s for s in sub if s)
    elif is_condition(conditions):
        col, op, lit = conditions
        col = column_expr(col, body)
        lit = escape_literal(tuple(lit) if isinstance(lit, list) else lit)
        return u'{} {} {}'.format(col, op, lit)
    elif depth == 1:
        sub = (condition_expr(cond, body, depth + 1) for cond in conditions)
        sub = [s for s in sub if s]
        res = u' OR '.join(sub)
        return u'({})'.format(res) if len(sub) > 1 else res


def escape_literal(value):
    """
    Escape a literal value for use in a SQL clause
    """
    if isinstance(value, six.string_types):
        value = value.replace("'", "\\'")  # TODO this escaping is garbage
        return u"'{}'".format(value)
    elif isinstance(value, datetime):
        value = value.replace(tzinfo=None, microsecond=0)
        return "toDateTime('{}')".format(value.isoformat())
    elif isinstance(value, date):
        return "toDate('{}')".format(value.isoformat())
    elif isinstance(value, (list, tuple)):
        return u"({})".format(', '.join(escape_literal(v) for v in value))
    elif isinstance(value, numbers.Number):
        return str(value)
    elif value is None:
        return ''
    else:
        raise ValueError(u'Do not know how to escape {} for SQL'.format(type(value)))


def raw_query(sql, client):
    """
    Submit a raw SQL query to clickhouse and do some post-processing on it to
    fix some of the formatting issues in the result JSON
    """
    print sql
    try:
        error = None
        data, meta = client.execute(sql, with_column_types=True)
    except BaseException as ex:
        data, meta, error = [], [], six.text_type(ex)

    # for now, convert back to a dict-y format to emulate the json
    data = [{c[0]: d[i] for i, c in enumerate(meta)} for d in data]
    meta = [{'name': m[0], 'type': m[1]} for m in meta]

    for col in meta:
        # Convert naive datetime strings back to TZ aware ones, and stringify
        # TODO maybe this should be in the json serializer
        if col['type'] == "DateTime":
            for d in data:
                d[col['name']] = d[col['name']].replace(tzinfo=tz.tzutc()).isoformat()
        if col['type'] == "Date":
            for d in data:
                dt = datetime(*(d[col['name']].timetuple()[:6])).replace(tzinfo=tz.tzutc())
                d[col['name']] = dt.isoformat()

    # TODO record statistics somewhere
    return {'data': data, 'meta': meta, 'error': error}


def issue_expr(issues, col='primary_hash', ids=None):
    """
    Takes a list of (issue_id, fingerprint(s)) tuples of the form:

        [(1, (hash1, hash2)), (2, hash3)]

    and constructs a SQL expression that will return the corresponding
    issue_id for any row whose `col` matches any of that issue_id's hashes
    """
    issue_ids = []
    hashes = []
    for issue_id, issue_hashes in issues:
        if ids is None or issue_id in ids:
            if not hasattr(issue_hashes, '__iter__'):
                issue_hashes = [issue_hashes]
            issue_ids.extend([six.text_type(issue_id)] * len(issue_hashes))
            hashes.extend('\'{}\''.format(h) for h in issue_hashes)
    assert len(issue_ids) == len(hashes)
    if len(hashes) == 0:
        return 0
    return '[{}][indexOf(CAST([{}], \'Array(FixedString(32))\'), {})]'.format(
        ','.join(issue_ids),
        ','.join(hashes),
        col
    )


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
                                        schema=json.dumps(
                                            schema, indent=4, sort_keys=True, default=default_encode)
                                        ), 400)
            return func(*args, **kwargs)
        return wrapper
    return validator


def get_table_definition(name, engine, columns=settings.SCHEMA_COLUMNS):
    return """
    CREATE TABLE IF NOT EXISTS %(name)s (%(columns)s) ENGINE = %(engine)s""" % {
        'columns': columns,
        'engine': engine,
        'name': name,
    }


def get_replicated_engine(
        name,
        order_by=settings.DEFAULT_ORDER_BY,
        partition_by=settings.DEFAULT_PARTITION_BY):
    return """
        ReplicatedMergeTree('/clickhouse/tables/{shard}/%(name)s', '{replica}')
        PARTITION BY %(partition_by)s
        ORDER BY %(order_by)s;""" % {
        'name': name,
        'order_by': order_by,
        'partition_by': partition_by,
    }


def get_distributed_engine(cluster, database, local_table,
                           sharding_key=settings.DEFAULT_SHARDING_KEY):
    return """Distributed(%(cluster)s, %(database)s, %(local_table)s, %(sharding_key)s);""" % {
        'cluster': cluster,
        'database': database,
        'local_table': local_table,
        'sharding_key': sharding_key,
    }


def force_bytes(s):
    if isinstance(s, bytes):
        return s
    return s.encode('utf-8', 'replace')


def create_metrics(host, port, prefix, tags=None):
    """Create a DogStatsd object with the specified prefix and tags. Prefixes
    must start with `snuba.<category>`, for example: `snuba.processor`."""

    from datadog import DogStatsd

    bits = prefix.split('.', 2)
    assert len(bits) >= 2 and bits[0] == 'snuba', "prefix must be like `snuba.<category>`"

    return DogStatsd(host=host, port=port, namespace=prefix, constant_tags=tags)
