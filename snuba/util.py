from flask import request

from datetime import date, datetime
from dateutil.tz import tz
from hashlib import md5
from itertools import chain
import calendar
import jsonschema
import logging
import numbers
import re
import simplejson as json
import six
import time

from snuba import schemas, settings, state


logger = logging.getLogger('snuba.util')


ESCAPE_RE = re.compile(r'^[a-zA-Z][a-zA-Z0-9_\.(), ]*$')
# example partition name: "('2018-03-13 00:00:00', 90)"
PART_RE = re.compile(r"\('(\d{4}-\d{2}-\d{2}) 00:00:00', (\d+)\)")


class Literal(object):
    def __init__(self, literal):
        self.literal = literal


def to_list(value):
    return value if isinstance(value, list) else [value]


def escape_col(col):
    if not col:
        return col
    elif ESCAPE_RE.match(col):
        return col
    else:
        return '`{}`'.format(col)


def string_col(col):
    col_type = settings.SCHEMA_MAP.get(col, None)

    if col_type and 'String' in col_type and 'FixedString' not in col_type:
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
        expr = settings.TIME_GROUPS[body['granularity']]
    elif settings.NESTED_COL_EXPR.match(column_name):
        expr = tag_expr(column_name)
    elif column_name in ['tags_key', 'tags_value']:
        expr = tags_expr(column_name, body)
    else:
        expr = escape_col(column_name)

    if aggregate:
        if expr:
            expr = u'{}({})'.format(aggregate, expr)
            if aggregate == 'uniq':  # default uniq() result to 0, not null
                expr = 'ifNull({}, 0)'.format(expr)
        else:  # This is the "count()" case where the '()' is already provided
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
    alias_cache = body.setdefault('alias_cache', [])

    if expr == alias:
        return expr
    elif alias in alias_cache:
        return alias
    else:
        alias_cache.append(alias)
        return u'({} AS {})'.format(expr, alias)


def tag_expr(column_name):
    """
    Return an expression for the value of a single named tag.

    For tags/contexts, we expand the expression depending on whether the tag is
    "promoted" to a top level column, or whether we have to look in the tags map.
    """
    match = settings.NESTED_COL_EXPR.match(column_name)
    col, tag = match.group(1), match.group(2)

    # For promoted tags, return the column name.
    if col in settings.PROMOTED_COLS:
        actual_tag = settings.TAG_COLUMN_MAP[col].get(tag, tag)
        if actual_tag in settings.PROMOTED_COLS[col]:
            return string_col(actual_tag)

    # For the rest, return an expression that looks it up in the nested tags.
    return u'{col}.value[indexOf({col}.key, {tag})]'.format(**{
        'col': col,
        'tag': escape_literal(tag)
    })


def tags_expr(column_name, body):
    """
    Return an expression that array-joins on tags to produce an output with one
    row per tag.
    """
    assert column_name in ['tags_key', 'tags_value']
    col, k_or_v = column_name.split('_', 1)
    nested_tags_only = state.get_config('nested_tags_only', 1)
    if nested_tags_only:
        key_list = '{}.key'.format(col)
        val_list = '{}.value'.format(col)
    else:
        promoted = settings.PROMOTED_COLS[col]
        col_map = settings.COLUMN_TAG_MAP[col]
        key_list = u'arrayConcat([{}], {}.key)'.format(
            u', '.join(u'\'{}\''.format(col_map.get(p, p)) for p in promoted),
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
    return u'({})'.format(expr) + ('[1]' if k_or_v == 'key' else '[2]')


def is_condition(cond_or_list):
    return len(cond_or_list) == 3 and isinstance(cond_or_list[0], six.string_types)


def flat_conditions(conditions):
    return list(chain(*[[c] if is_condition(c) else c for c in conditions]))


def tuplify(nested):
    if isinstance(nested, (list, tuple)):
        return tuple(tuplify(child) for child in nested)
    return nested


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
        if op == 'LIKE':
            return u'like({}, {})'.format(string_col(col), lit)
        elif op == 'NOT LIKE':
            return u'notLike({}, {})'.format(string_col(col), lit)
        else:
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
    if isinstance(value, Literal):
        return value.literal
    elif isinstance(value, six.string_types):
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


def raw_query(body, sql, client, timer, stats=None):
    """
    Submit a raw SQL query to clickhouse and do some post-processing on it to
    fix some of the formatting issues in the result JSON
    """
    stats = stats or {}
    grl, gcl, prl, pcl, use_cache, query_settings = state.get_configs([
        ('global_per_second_limit', 1000),
        ('global_concurrent_limit', 1000),
        ('project_per_second_limit', 1000),
        ('project_concurrent_limit', 1000),
        ('use_cache', 0),
        ('query_settings_json', None),
    ])
    try:
        query_settings = json.loads(query_settings)
    except (TypeError, ValueError):
        query_settings = {}
    stats.update(query_settings)
    timer.mark('get_configs')

    with state.rate_limit('global', grl, gcl) as (g_allowed, g_rate, g_concurr):
        metrics.gauge('query.global_concurrent', g_concurr)
        stats.update({'global_rate': g_rate, 'global_concurrent': g_concurr})

        # TODO rate limit on every project in the list?
        project_ids = to_list(body['project'])
        with state.rate_limit(project_ids[0], prl, pcl) as (p_allowed, p_rate, p_concurr):
            stats.update({'project_rate': p_rate, 'project_concurrent': p_concurr})
            timer.mark('rate_limit')

            if g_allowed and p_allowed:
                query_id = md5(force_bytes(sql)).hexdigest()
                with state.deduper(query_id) as is_dupe:
                    timer.mark('dedupe_wait')

                    result = state.get_result(query_id) if use_cache else None
                    timer.mark('cache_get')

                    stats.update({
                        'is_duplicate': is_dupe,
                        'query_id': query_id,
                        'use_cache': bool(use_cache),
                        'cache_hit': bool(result)}
                    ),

                    if result:
                        status = 200
                    else:
                        try:
                            data, meta = client.execute(
                                sql,
                                with_column_types=True,
                                settings=query_settings,
                                # All queries should already be deduplicated at this point
                                # But the query_id will let us know if they aren't
                                query_id=query_id
                            )
                            data, meta = scrub_ch_data(data, meta)
                            result = {'data': data, 'meta': meta}
                            status = 200

                            logger.debug(sql)
                            timer.mark('execute')
                            stats.update({
                                'result_rows': len(data),
                                'result_cols': len(meta),
                            })

                            if use_cache:
                                state.set_result(query_id, result)
                                timer.mark('cache_set')

                        except BaseException as ex:
                            error = six.text_type(ex)
                            status = 500
                            logger.error("Error running query: %s\nClickhouse error: %s" % (sql, error))
                            result = {'error': error}
            else:
                status = 429
                result = {'error': 'rate limit exceeded'}

    state.record_query({
        'request': body,
        'sql': sql,
        'timing': timer,
        'stats': stats,
        'status': status,
    })

    if settings.RECORD_QUERIES:
        timer.record(metrics)
    result['timing'] = timer

    if settings.STATS_IN_RESPONSE:
        result['stats'] = stats

    return (result, status)


def scrub_ch_data(data, meta):
    # for now, convert back to a dict-y format to emulate the json
    data = [{c[0]: d[i] for i, c in enumerate(meta)} for d in data]
    meta = [{'name': m[0], 'type': m[1]} for m in meta]

    for col in meta:
        # Convert naive datetime strings back to TZ aware ones, and stringify
        # TODO maybe this should be in the json serializer
        if col['type'].startswith('DateTime'):
            for d in data:
                d[col['name']] = d[col['name']].replace(tzinfo=tz.tzutc()).isoformat()
        elif col['type'].startswith('Date'):
            for d in data:
                dt = datetime(*(d[col['name']].timetuple()[:6])).replace(tzinfo=tz.tzutc())
                d[col['name']] = dt.isoformat()

    return (data, meta)


def uses_issue(body):
    """
    Returns whether the query references `issue` in groupings, conditions, or
    aggregations. and which issue IDs it specifically selects for, if any.
    """
    cond = flat_conditions(body.get('conditions', []))
    used_ids = [set([lit]) for (col, op, lit) in cond if col == 'issue' and op == '='] +\
        [set(lit) for (col, op, lit) in cond if col == 'issue'
         and op == 'IN' and isinstance(lit, list)]
    # TODO handle NOT IN or not equal
    used_ids = set.union(*used_ids) if used_ids else None

    uses = (
        used_ids is not None or
        'issue' in to_list(body.get('groupby', [])) or
        any(col == 'issue' for (_, col, _) in body.get('aggregations', []))
    )
    return (uses, used_ids)


def issue_expr(body, hash_column='primary_hash'):
    """
    Takes a list of (issue_id, fingerprint(s)) tuples of the form:

        [(1, (hash1, hash2)), (2, hash3)]

    from body['issues'] and constructs a SQL JOIN expression that will add the
    issue id to the query. If specific issue IDs are selected for in the query
    conditions, this query will only expand the expression for those referenced
    issues.
    """
    uses, used_ids = uses_issue(body)
    if not uses:
        return ''

    issue_ids = []
    hashes = []
    project_ids = []
    tombstones = []

    max_issues, max_hashes_per_issue = state.get_configs([
        ('max_issues', None),
        ('max_hashes_per_issue', None),
    ])
    issues = body['issues']
    if max_issues is not None:
        issues = issues[:max_issues]

    for issue_id, project_id, issue_hashes in issues:
        if used_ids is None or issue_id in used_ids:
            if max_hashes_per_issue is not None:
                issue_hashes = issue_hashes[:max_hashes_per_issue]

            issue_ids.extend([six.text_type(issue_id)] * len(issue_hashes))
            project_ids.extend([six.text_type(project_id)] * len(issue_hashes))

            for hash_obj in issue_hashes:
                if isinstance(hash_obj, list):
                    issue_hash, tombstone = hash_obj
                else:
                    issue_hash = hash_obj
                    tombstone = None

                if tombstone:
                    tombstone = int(calendar.timegm(
                        datetime.strptime(tombstone, "%Y-%m-%d %H:%M:%S").timetuple()))
                tombstones.append(str(tombstone) if tombstone else 'Null')
                hashes.append('\'{}\''.format(issue_hash))

    # Special case, we have no issues to expand but there is still a
    # reference to a specific `issue = X`. Clickhouse will error trying to
    # compare (Nothing, UInt8), but will work on comparing (Null, Uint8) so
    # we need a Null value in the issue expression for the query to work.
    if not issue_ids and used_ids:
        issue_ids, hashes, tombstones = ['0'], ['Null'], ['Null']

    if project_ids and len(set(project_ids)) > 1:
        return ("""
            ANY INNER JOIN
            (SELECT arrayJoin(
                arrayMap(
                    (w, x, y, z) -> tuple(w, x, y, z),
                    CAST([{hashes}], 'Array(Nullable(FixedString(32)))'),
                    [{issue_ids}],
                    CAST([{project_ids}], 'Array(UInt64)'),
                    CAST([{tombstones}], 'Array(Nullable(DateTime))'))
                ) as map,
                tupleElement(map, 1) as {col},
                tupleElement(map, 2) as issue,
                tupleElement(map, 3) as project_id,
                tupleElement(map, 4) as hash_timestamp
            ) USING ({col}, project_id)""").format(
            issue_ids=','.join(issue_ids),
            project_ids=','.join(project_ids),
            hashes=','.join(hashes),
            tombstones=','.join(tombstones),
            col=hash_column,
        )
    else:
        return ("""
            ANY INNER JOIN
            (SELECT arrayJoin(
                arrayMap(
                    (x, y, z) -> tuple(x, y, z),
                    CAST([{hashes}], 'Array(Nullable(FixedString(32)))'),
                    [{issue_ids}],
                    CAST([{tombstones}], 'Array(Nullable(DateTime))'))
                ) as map,
                tupleElement(map, 1) as {col},
                tupleElement(map, 2) as issue,
                tupleElement(map, 3) as hash_timestamp
            ) USING {col}""").format(
            issue_ids=','.join(issue_ids),
            hashes=','.join(hashes),
            tombstones=','.join(tombstones),
            col=hash_column,
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
                    if kwargs.get('timer'):
                        kwargs['timer'].mark('validate_schema')
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


def decode_part_str(part_str):
    match = PART_RE.match(part_str)
    if not match:
        raise ValueError("Unknown part name/format: " + str(part_str))

    date_str, retention_days = match.groups()
    date = datetime.strptime(date_str, '%Y-%m-%d')

    return (date, int(retention_days))


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


metrics = create_metrics(settings.DOGSTATSD_HOST, settings.DOGSTATSD_PORT, 'snuba.api')
