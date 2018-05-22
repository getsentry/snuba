from flask import request, render_template

from datetime import date, datetime
from dateutil.tz import tz
import simplejson as json
from itertools import chain
import logging
import jsonschema
import numbers
import os
import re
import requests
import six
import time

from snuba import schemas, settings, state


logger = logging.getLogger('snuba.util')


ESCAPE_RE = re.compile(r'^[a-zA-Z]*$')


def to_list(value):
    return value if isinstance(value, list) else [value]


def escape_col(col):
    if ESCAPE_RE.match(col):
        return col
    else:
        return '`{}`'.format(col)


def string_col(col):
    if 'String' in settings.SCHEMA_MAP[col]:
        return escape_col(col)
    else:
        return 'toString({})'.format(escape_col(col))


def column_expr(column_name, body, alias=None, aggregate=None):
    """
    Certain special column names expand into more complex expressions. Return
    a 2-tuple of:
        (expanded column expression, sanitized alias)

    Needs the body of the request for some extra data used to expand column expressions.
    """
    assert column_name or aggregate
    column_name = column_name or ''

    if column_name == settings.TIME_GROUP_COLUMN:
        expr = settings.TIME_GROUPS.get(body['granularity'], settings.DEFAULT_TIME_GROUP)
    elif settings.NESTED_COL_EXPR.match(column_name):
        expr = tag_expr(column_name)
    elif column_name in ['tags_key', 'tags_value']:
        expr = tags_expr(column_name, body)
    else:
        expr = escape_col(column_name)

    if aggregate:
        if expr:
            expr = u'{}({})'.format(aggregate, expr)
        else:
            # This is the "count()" case where the brackets are already in the aggregate
            expr = aggregate

    alias = escape_col(alias or column_name or aggregate)

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
        return alias_cache[expr]
    else:
        alias_cache[expr] = alias
        return u'({} AS {})'.format(expr, alias)


def tag_expr(column_name):
    """
    Return an expression for the value of a single named tag.

    For tags/contexts, we expand the expression depending on whether the tag is
    "promoted" to a top level column, or whether we have to look in the tags map.
    """
    match = settings.NESTED_COL_EXPR.match(column_name)
    col, sub = match.group(1), match.group(2)
    sub_field = sub.replace('.', '_')
    if col in settings.PROMOTED_COLS and sub_field in settings.PROMOTED_COLS[col]:
        expr = escape_col(sub_field)  # TODO recurse?
    else:
        expr = u'{col}.value[indexOf({col}.key, {sub})]'.format(**{
            'col': col,
            'sub': escape_literal(sub)
        })
    return expr


def tags_expr(column_name, body):
    """
    Return an expression enumerating all tags as rows

    For special columns `tags_key` and `tags_value` we construct a an interesting
    arrayjoin that enumerates all promoted and non-promoted keys. This is for
    cases where we do not have a tag key to filter on (eg top tags).
    """
    assert column_name in ['tags_key', 'tags_value']
    col, typ = column_name.split('_', 1)
    promoted = settings.PROMOTED_COLS[col]

    key_list = u'arrayConcat([{}], {}.key)'.format(
        u', '.join(u'\'{}\''.format(p) for p in promoted),
        col
    )
    val_list = u'arrayConcat([{}], {}.value)'.format(
        ', '.join(string_col(p) for p in promoted),
        col
    )
    expr = (u'arrayJoin(arrayMap((x,y) -> [x,y], {}, {}))').format(
        key_list,
        val_list
    )
    # put the tag expression in the alias cache so we can use the alias
    # to refer to it next time instead of expanding it again.
    expr = alias_expr(expr, 'all_tags', body)
    return u'({})'.format(expr) + ('[1]' if typ == 'key' else '[2]')


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
    logger.debug(sql)
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
        elif col['type'] == "Date":
            for d in data:
                dt = datetime(*(d[col['name']].timetuple()[:6])).replace(tzinfo=tz.tzutc())
                d[col['name']] = dt.isoformat()

    return {'data': data, 'meta': meta, 'error': error}


def issue_expr(body, hash_column='primary_hash'):
    """
    Takes a list of (issue_id, fingerprint(s)) tuples of the form:

        [(1, (hash1, hash2)), (2, hash3)]

    from body['issues'] and constructs a SQL JOIN expression that will add the
    issue id to the query. If specific issue IDs are selected for in the query
    conditions, this query will only expand the expression for those referenced
    issues.
    """
    cond = flat_conditions(body.get('conditions', []))
    used_ids = [set([lit]) for (col, op, lit) in cond if col == 'issue' and op == '='] +\
        [set(lit) for (col, op, lit) in cond if col ==
     'issue' and op == 'IN' and isinstance(lit, list)]
    used_ids = set.union(*used_ids) if used_ids else None

    issue_ids = []
    hashes = []

    # NB the number of issues in the request is already limited by the schema.
    # This is for further limiting at runtime.
    max_issues = state.get_config('max_issues')
    max_hashes_per_issue = state.get_config('max_hashes_per_issue')
    issues = body['issues']
    if max_issues is not None:
        issues = issues[:max_issues]

    for issue_id, issue_hashes in issues:
        if used_ids is None or issue_id in used_ids:
            issue_hashes = to_list(issue_hashes)
            if max_hashes_per_issue is not None:
                issue_hashes = issue_hashes[:max_hashes_per_issue]
            issue_ids.extend([six.text_type(issue_id)] * len(issue_hashes))
            hashes.extend('\'{}\''.format(h) for h in issue_hashes)
    assert len(issue_ids) == len(hashes)
    if len(hashes) == 0:
        return ''
    return ('ANY INNER JOIN '
            '(SELECT arrayJoin('
            'arrayMap((x, y) -> tuple(x, y), CAST([{hashes}], \'Array(FixedString(32))\'), [{issue_ids}])) as map,'
            'tupleElement(map, 1) as {col},'
            'tupleElement(map, 2) as issue'
            ') USING {col}').format(
        issue_ids=','.join(issue_ids),
        hashes=','.join(hashes),
        col=hash_column
    )


def validate_request(schema):
    """
    Decorator to validate that a request body matches the given schema.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):

            def default_encode(value):
                if callable(value):
                    return value()
                else:
                    raise TypeError()

            if request.method == 'POST':
                try:
                    body = json.loads(request.data)
                    schemas.validate(body, schema)
                    kwargs['validated_body'] = body
                except (ValueError, jsonschema.ValidationError) as e:
                    result = {'error': str(e), 'schema': schema}
                    return (
                        json.dumps(result, sort_keys=True, indent=4, default=default_encode),
                        400,
                        {'Content-Type': 'application/json'}
                    )
            return func(*args, **kwargs)
        return wrapper
    return decorator


class Timer(object):
    def __init__(self, name=''):
        self.marks = [(name, time.time())]
        self.final = None

    def mark(self, name):
        self.marks.append((name, time.time()))

    def finish(self):
        if not self.final:
            start = self.marks[0][1]
            end = time.time() if len(self.marks) == 1 else self.marks[-1][1]
            diff_ms = lambda start, end: int((end - start) * 1000)
            self.final = {
                'timestamp': int(start),
                'duration_ms': diff_ms(start, end),
                'marks_ms': {
                    name: diff_ms(self.marks[i][1], ts) for i, (name, ts) in enumerate(self.marks[1:])
                }
            }
        return self.final

    def for_json(self):
        return self.finish()

    def record(self, metrics):
        name = self.marks[0][0]
        final = self.finish()
        metrics.timing(name, final['duration_ms'])
        for mark, duration in six.iteritems(final['marks_ms']):
            metrics.timing('{}.{}'.format(name, mark), duration)


def time_request(name):
    def decorator(func):
        def wrapper(*args, **kwargs):
            kwargs['timer'] = Timer(name)
            return func(*args, **kwargs)
        return wrapper
    return decorator


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
