from flask import request

from clickhouse_driver.errors import Error as ClickHouseError
from collections import namedtuple, OrderedDict
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from dateutil.parser import parse as dateutil_parse
from dateutil.tz import tz
from functools import wraps
from hashlib import md5
from itertools import chain, groupby
from typing import NamedTuple
import jsonschema
import logging
import numbers
import re
import simplejson as json
import _strptime  # NOQA fixes _strptime deferred import issue
import time

from snuba import clickhouse, schemas, settings, state
from snuba.clickhouse import escape_col


logger = logging.getLogger('snuba.util')


# example partition name: "('2018-03-13 00:00:00', 90)"
PART_RE = re.compile(r"\('(\d{4}-\d{2}-\d{2})',\s*(\d+)\)")
DATE_TYPE_RE = re.compile(r'(Nullable\()?Date\b')
DATETIME_TYPE_RE = re.compile(r'(Nullable\()?DateTime\b')
QUOTED_LITERAL_RE = re.compile(r"^'.*'$")
ESCAPE_STRING_RE = re.compile(r"(['\\])")
SAFE_FUNCTION_RE = re.compile(r'-?[a-zA-Z_][a-zA-Z0-9_]*$')
TOPK_FUNCTION_RE = re.compile(r'^top([1-9]\d*)$')


class InvalidConditionException(Exception):
    pass


def to_list(value):
    return value if isinstance(value, list) else [value]


def string_col(dataset, col):
    col_type = dataset.get_schema().get_columns().get(col, None)
    col_type = str(col_type) if col_type else None

    if col_type and 'String' in col_type and 'FixedString' not in col_type:
        return escape_col(col)
    else:
        return 'toString({})'.format(escape_col(col))


def parse_datetime(value, alignment=1):
    dt = dateutil_parse(value, ignoretz=True).replace(microsecond=0)
    return dt - timedelta(seconds=(dt - dt.min).seconds % alignment)

def time_expr(alias, granularity):
    column = settings.TIME_GROUP_COLUMNS[alias]
    template = {
        3600: 'toStartOfHour({column})',
        60: 'toStartOfMinute({column})',
        86400: 'toDate({column})',
    }.get(granularity, 'toDateTime(intDiv(toUInt32({column}), {granularity}) * {granularity})')

    return template.format(column=column, granularity=granularity)

def function_expr(fn, args_expr=''):
    """
    Generate an expression for a given function name and an already-evaluated
    args expression. This is a place to define convenience functions that evaluate
    to more complex expressions.

    """
    # For functions with no args, (or static args) we allow them to already
    # include them as part of the function name, eg, "count()" or "sleep(1)"
    if not args_expr and fn.endswith(')'):
        return fn

    # Convenience topK function eg "top10", "top3" etc.
    topk = TOPK_FUNCTION_RE.match(fn)
    if topk:
        return 'topK({})({})'.format(topk.group(1), args_expr)

    # turn uniq() into ifNull(uniq(), 0) so it doesn't return null where
    # a number was expected.
    if fn == 'uniq':
        return 'ifNull({}({}), 0)'.format(fn, args_expr)

    # emptyIfNull(col) is a simple pseudo function supported by Snuba that expands
    # to the actual clickhouse function ifNull(col, '') Until we figure out the best
    # way to disambiguate column names from string literals in complex functions.
    if fn == 'emptyIfNull' and args_expr:
        return 'ifNull({}, \'\')'.format(args_expr)

    # default: just return fn(args_expr)
    return  u'{}({})'.format(fn, args_expr)

def is_function(column_expr, depth=0):
    """
    Returns a 3-tuple of (name, args, alias) if column_expr is a function,
    otherwise None.

    A function expression is of the form:

        [func, [arg1, arg2]]  => func(arg1, arg2)

    If a string argument is followed by list arg, the pair of them is assumed
    to be a nested function call, with extra args to the outer function afterward.

        [func1, [func2, [arg1, arg2], arg3]]  => func1(func2(arg1, arg2), arg3)

    Although at the top level, there is no outer function call, and the optional
    3rd argument is interpreted as an alias for the entire expression.

        [func, [arg1] alias] => function(arg1) AS alias

    """
    if (isinstance(column_expr, (tuple, list))
            and len(column_expr) >= 2
            and isinstance(column_expr[0], str)
            and isinstance(column_expr[1], (tuple, list))
            and (depth > 0 or len(column_expr) <= 3)):
        assert SAFE_FUNCTION_RE.match(column_expr[0])
        if len(column_expr) == 2:
            return tuple(column_expr) + (None,)
        else:
            return tuple(column_expr)
    else:
        return None

def column_expr(dataset, column_name, body, alias=None, aggregate=None):
    """
    Certain special column names expand into more complex expressions. Return
    a 2-tuple of:
        (expanded column expression, sanitized alias)

    Needs the body of the request for some extra data used to expand column expressions.
    """
    assert column_name or aggregate
    assert not aggregate or (aggregate and (column_name or alias))
    column_name = column_name or ''

    if is_function(column_name, 0):
        return complex_column_expr(dataset, column_name, body)
    elif isinstance(column_name, str) and QUOTED_LITERAL_RE.match(column_name):
        return escape_literal(column_name[1:-1])
    elif column_name in settings.TIME_GROUP_COLUMNS:
        expr = time_expr(column_name, body['granularity'])
    else:
        expr = dataset.column_expr(column_name, body)

    if aggregate:
        expr = function_expr(aggregate, expr)

    alias = escape_col(alias or column_name)

    return alias_expr(expr, alias, body)


def complex_column_expr(dataset, expr, body, depth=0):
    function_tuple = is_function(expr, depth)
    if function_tuple is None:
        raise ValueError('complex_column_expr was given an expr %s that is not a function at depth %d.' % (expr, depth))

    name, args, alias = function_tuple
    out = []
    i = 0
    while i < len(args):
        next_2 = args[i:i+2]
        if is_function(next_2, depth+1):
            out.append(complex_column_expr(dataset, next_2, body, depth+1))
            i += 2
        else:
            nxt = args[i]
            if is_function(nxt, depth + 1):  # Embedded function
                out.append(complex_column_expr(dataset, nxt, body, depth + 1))
            elif isinstance(nxt, str):
                out.append(column_expr(dataset, nxt, body))
            else:
                out.append(escape_literal(nxt))
            i += 1

    ret = function_expr(name, ', '.join(out))
    if alias:
        ret = alias_expr(ret, alias, body)
    return ret


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


def is_condition(cond_or_list):
    return (
        # A condition is:
        # a 3-tuple
        len(cond_or_list) == 3 and
        # where the middle element is an operator
        cond_or_list[1] in schemas.CONDITION_OPERATORS and
        # and the first element looks like a column name or expression
        isinstance(cond_or_list[0], (str, tuple, list))
    )


def all_referenced_columns(body):
    """
    Return the set of all columns that are used by a query.
    """
    col_exprs = []

    # These fields can reference column names
    for field in ['arrayjoin', 'groupby', 'orderby', 'selected_columns']:
        if field in body:
            col_exprs.extend(to_list(body[field]))

    # Conditions need flattening as they can be nested as AND/OR
    if 'conditions' in body:
        flat_conditions = list(chain(*[[c] if is_condition(c) else c for c in body['conditions']]))
        col_exprs.extend([c[0] for c in flat_conditions])

    if 'aggregations' in body:
        col_exprs.extend([a[1] for a in body['aggregations']])

    # Return the set of all columns referenced in any expression
    return set(chain(*[columns_in_expr(ex) for ex in col_exprs]))


def columns_in_expr(expr):
    """
    Get the set of columns that are referenced by a single column expression.
    Either it is a simple string with the column name, or a nested function
    that could reference multiple columns
    """
    cols = []
    # TODO possibly exclude quoted args to functions as those are
    # string literals, not column names.
    if isinstance(expr, str):
        cols.append(expr.lstrip('-'))
    elif (isinstance(expr, (list, tuple)) and len(expr) >= 2
          and isinstance(expr[1], (list, tuple))):
        for func_arg in expr[1]:
            cols.extend(columns_in_expr(func_arg))
    return cols


def tuplify(nested):
    if isinstance(nested, (list, tuple)):
        return tuple(tuplify(child) for child in nested)
    return nested


def conditions_expr(dataset, conditions, body, depth=0):
    """
    Return a boolean expression suitable for putting in the WHERE clause of the
    query.  The expression is constructed by ANDing groups of OR expressions.
    Expansion of columns is handled, as is replacement of columns with aliases,
    if the column has already been expanded and aliased elsewhere.
    """
    if not conditions:
        return ''

    if depth == 0:
        # dedupe conditions at top level, but keep them in order
        sub = OrderedDict((conditions_expr(dataset, cond, body, depth + 1), None) for cond in conditions)
        return u' AND '.join(s for s in sub.keys() if s)
    elif is_condition(conditions):
        lhs, op, lit = conditions

        if (
            lhs in ('received', 'timestamp') and
            op in ('>', '<', '>=', '<=', '=', '!=') and
            isinstance(lit, str)
        ):
            lit = parse_datetime(lit)

        # facilitate deduping IN conditions by sorting them.
        if op in ('IN', 'NOT IN') and isinstance(lit, tuple):
            lit = tuple(sorted(lit))

        # If the LHS is a simple column name that refers to an array column
        # (and we are not arrayJoining on that column, which would make it
        # scalar again) and the RHS is a scalar value, we assume that the user
        # actually means to check if any (or all) items in the array match the
        # predicate, so we return an `any(x == value for x in array_column)`
        # type expression. We assume that operators looking for a specific value
        # (IN, =, LIKE) are looking for rows where any array value matches, and
        # exclusionary operators (NOT IN, NOT LIKE, !=) are looking for rows
        # where all elements match (eg. all NOT LIKE 'foo').
        columns = dataset.get_schema().get_columns()
        if (
            isinstance(lhs, str) and
            lhs in columns and
            type(columns[lhs].type) == clickhouse.Array and
            columns[lhs].base_name != body.get('arrayjoin') and
            not isinstance(lit, (list, tuple))
            ):
            any_or_all = 'arrayExists' if op in schemas.POSITIVE_OPERATORS else 'arrayAll'
            return u'{}(x -> assumeNotNull(x {} {}), {})'.format(
                any_or_all,
                op,
                escape_literal(lit),
                column_expr(dataset, lhs, body)
            )
        else:
            return u'{} {} {}'.format(
                column_expr(dataset, lhs, body),
                op,
                escape_literal(lit)
            )

    elif depth == 1:
        sub = (conditions_expr(dataset, cond, body, depth + 1) for cond in conditions)
        sub = [s for s in sub if s]
        res = u' OR '.join(sub)
        return u'({})'.format(res) if len(sub) > 1 else res
    else:
        raise InvalidConditionException(str(conditions))


def escape_string(str):
    str = ESCAPE_STRING_RE.sub(r"\\\1", str)
    return u"'{}'".format(str)


def escape_literal(value):
    """
    Escape a literal value for use in a SQL clause.
    """
    if isinstance(value, str):
        return escape_string(value)
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
    project_ids = to_list(body['project'])
    project_id = project_ids[0] if project_ids else 0  # TODO rate limit on every project in the list?
    stats = stats or {}
    grl, gcl, prl, pcl, use_cache, uc_max = state.get_configs([
        ('global_per_second_limit', None),
        ('global_concurrent_limit', 1000),
        ('project_per_second_limit', 1000),
        ('project_concurrent_limit', 1000),
        ('use_cache', 0),
        ('uncompressed_cache_max_cols', 5),
    ])

    # Specific projects can have their rate limits overridden
    prl, pcl = state.get_configs([
        ('project_per_second_limit_{}'.format(project_id), prl),
        ('project_concurrent_limit_{}'.format(project_id), pcl),
    ])

    all_confs = state.get_all_configs()
    query_settings = {
        k.split('/', 1)[1]: v
        for k, v in all_confs.items()
        if k.startswith('query_settings/')
    }

    # Experiment, if we are going to grab more than X columns worth of data,
    # don't use uncompressed_cache in clickhouse, or result cache in snuba.
    if len(all_referenced_columns(body)) > uc_max:
        query_settings['use_uncompressed_cache'] = 0
        use_cache = 0

    timer.mark('get_configs')

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
            with state.rate_limit('global', grl, gcl) as (g_allowed, g_rate, g_concurr):
                metrics.gauge('query.global_concurrent', g_concurr)
                stats.update({'global_rate': g_rate, 'global_concurrent': g_concurr})

                with state.rate_limit(project_id, prl, pcl) as (p_allowed, p_rate, p_concurr):
                    stats.update({'project_rate': p_rate, 'project_concurrent': p_concurr})
                    timer.mark('rate_limit')

                    if g_allowed and p_allowed:

                        # Experiment, reduce max threads by 1 for each extra concurrent query
                        # that a project has running beyond the first one
                        if 'max_threads' in query_settings and p_concurr > 1:
                            maxt = query_settings['max_threads']
                            query_settings['max_threads'] = max(1, maxt - p_concurr + 1)

                        # Force query to use the first shard replica, which
                        # should have synchronously received any cluster writes
                        # before this query is run.
                        consistent = body.get('consistent', False)
                        stats['consistent'] = consistent
                        if consistent:
                            query_settings['load_balancing'] = 'in_order'
                            query_settings['max_threads'] = 1

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
                            status = 200
                            if body.get('totals', False):
                                assert len(data) > 0
                                data, totals = data[:-1], data[-1]
                                result = {'data': data, 'meta': meta, 'totals': totals}
                            else:
                                result = {'data': data, 'meta': meta}

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
                            error = str(ex)
                            status = 500
                            logger.exception("Error running query: %s\n%s", sql, error)
                            if isinstance(ex, ClickHouseError):
                                result = {'error': {
                                    'type': 'clickhouse',
                                    'code': ex.code,
                                    'message': error,
                                }}
                            else:
                                result = {'error': {
                                    'type': 'unknown',
                                    'message': error,
                                }}

                    else:
                        status = 429
                        Reason = namedtuple('reason', 'scope name val limit')
                        reasons = [
                            Reason('global', 'concurrent', g_concurr, gcl),
                            Reason('global', 'per-second', g_rate, grl),
                            Reason('project', 'concurrent', p_concurr, pcl),
                            Reason('project', 'per-second', p_rate, prl)
                        ]
                        reason = next((r for r in reasons if r.limit is not None and r.val > r.limit), None)
                        result = {'error': {
                            'type': 'ratelimit',
                            'message': 'rate limit exceeded',
                            'detail': reason and '{r.scope} {r.name} of {r.val:.0f} exceeds limit of {r.limit:.0f}'.format(r=reason)
                        }}

    stats.update(query_settings)

    if settings.RECORD_QUERIES:
        # send to redis
        state.record_query({
            'request': body,
            'sql': sql,
            'timing': timer,
            'stats': stats,
            'status': status,
        })

        # send to datadog
        tags = [
            'status:{}'.format(status),
            'referrer:{}'.format(stats.get('referrer', 'none')),
            'final:{}'.format(stats.get('final', False))
        ]
        mark_tags = [
            'final:{}'.format(stats.get('final', False))
        ]
        timer.send_metrics_to(metrics, tags=tags, mark_tags=mark_tags)

    result['timing'] = timer

    if settings.STATS_IN_RESPONSE or body.get('debug', False):
        result['stats'] = stats
        result['sql'] = sql

    return (result, status)


def scrub_ch_data(data, meta):
    # for now, convert back to a dict-y format to emulate the json
    data = [{c[0]: d[i] for i, c in enumerate(meta)} for d in data]
    meta = [{'name': m[0], 'type': m[1]} for m in meta]

    for col in meta:
        # Convert naive datetime strings back to TZ aware ones, and stringify
        # TODO maybe this should be in the json serializer
        if DATETIME_TYPE_RE.match(col['type']):
            for d in data:
                d[col['name']] = d[col['name']].replace(tzinfo=tz.tzutc()).isoformat()
        elif DATE_TYPE_RE.match(col['type']):
            for d in data:
                dt = datetime(*(d[col['name']].timetuple()[:6])).replace(tzinfo=tz.tzutc())
                d[col['name']] = dt.isoformat()

    return (data, meta)


def validate_request(schema):
    """
    Decorator to validate that a request body matches the given schema.
    """
    def decorator(func):
        @wraps(func)
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
                    result = {'error': {
                        'type': 'schema',
                        'message': str(e),
                    }, 'schema': schema}
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
        self.final = None
        self.marks.append((name, time.time()))

    def finish(self):
        if not self.final:
            start = self.marks[0][1]
            end = time.time() if len(self.marks) == 1 else self.marks[-1][1]
            diff_ms = lambda start, end: int((end - start) * 1000)
            durations = [(name, diff_ms(self.marks[i][1], ts)) for i, (name, ts) in enumerate(self.marks[1:])]
            self.final = {
                'timestamp': int(start),
                'duration_ms': diff_ms(start, end),
                'marks_ms': {
                    key: sum(d[1] for d in group) for key, group in groupby(sorted(durations), key=lambda x: x[0])
                }
            }
        return self.final

    def for_json(self):
        return self.finish()

    def send_metrics_to(self, metrics, tags=None, mark_tags=None):
        name = self.marks[0][0]
        final = self.finish()
        metrics.timing(name, final['duration_ms'], tags=tags)
        for mark, duration in final['marks_ms'].items():
            metrics.timing('{}.{}'.format(name, mark), duration, tags=mark_tags)


def time_request(name):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            kwargs['timer'] = Timer(name)
            return func(*args, **kwargs)
        return wrapper
    return decorator


class Part(NamedTuple):
    date: datetime
    retention_days: int


def decode_part_str(part_str: str) -> Part:
    match = PART_RE.match(part_str)
    if not match:
        raise ValueError("Unknown part name/format: " + str(part_str))

    date_str, retention_days = match.groups()
    date = datetime.strptime(date_str, '%Y-%m-%d')

    return Part(date, int(retention_days))


def force_bytes(s):
    if isinstance(s, bytes):
        return s
    return s.encode('utf-8', 'replace')


@contextmanager
def settings_override(overrides):
    previous = {}
    for k, v in overrides.items():
        previous[k] = getattr(settings, k, None)
        setattr(settings, k, v)

    try:
        yield
    finally:
        for k, v in previous.items():
            setattr(settings, k, v)


def create_metrics(host, port, prefix, tags=None):
    """Create a DogStatsd object with the specified prefix and tags. Prefixes
    must start with `snuba.<category>`, for example: `snuba.processor`."""

    from datadog import DogStatsd

    bits = prefix.split('.', 2)
    assert len(bits) >= 2 and bits[0] == 'snuba', "prefix must be like `snuba.<category>`"

    return DogStatsd(host=host, port=port, namespace=prefix, constant_tags=tags)


metrics = create_metrics(settings.DOGSTATSD_HOST, settings.DOGSTATSD_PORT, 'snuba.api')
