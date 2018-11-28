from flask import request

from clickhouse_driver.errors import Error as ClickHouseError
from datetime import date, datetime, timedelta
from dateutil.parser import parse as dateutil_parse
from dateutil.tz import tz
from functools import wraps
from hashlib import md5
from itertools import chain, groupby
import jsonschema
import logging
import numbers
import re
import simplejson as json
import six
import _strptime  # fixes _strptime deferred import issue
import time

from snuba import clickhouse, schemas, settings, state
from snuba.clickhouse import escape_col, ALL_COLUMNS, PROMOTED_COLS, TAG_COLUMN_MAP, COLUMN_TAG_MAP


logger = logging.getLogger('snuba.util')


# A column name like "tags[url]"
NESTED_COL_EXPR_RE = re.compile('^(tags|contexts)\[([a-zA-Z0-9_\.:-]+)\]$')

# example partition name: "('2018-03-13 00:00:00', 90)"
PART_RE = re.compile(r"\('(\d{4}-\d{2}-\d{2})', (\d+)\)")
DATE_TYPE_RE = re.compile(r'(Nullable\()?Date\b')
DATETIME_TYPE_RE = re.compile(r'(Nullable\()?DateTime\b')
QUOTED_LITERAL_RE = re.compile(r"^'.*'$")


class InvalidConditionException(Exception):
    pass


class Literal(object):
    def __init__(self, literal):
        self.literal = literal


def to_list(value):
    return value if isinstance(value, list) else [value]


def string_col(col):
    col_type = ALL_COLUMNS.get(col, None)
    col_type = str(col_type) if col_type else None

    if col_type and 'String' in col_type and 'FixedString' not in col_type:
        return escape_col(col)
    else:
        return 'toString({})'.format(escape_col(col))


def parse_datetime(value, alignment=1):
    dt = dateutil_parse(value, ignoretz=True).replace(microsecond=0)
    return dt - timedelta(seconds=(dt - dt.min).seconds % alignment)


def column_expr(column_name, body, alias=None, aggregate=None):
    """
    Certain special column names expand into more complex expressions. Return
    a 2-tuple of:
        (expanded column expression, sanitized alias)

    Needs the body of the request for some extra data used to expand column expressions.
    """
    assert column_name or aggregate
    assert not aggregate or (aggregate and (column_name or alias))
    column_name = column_name or ''

    if isinstance(column_name, (tuple, list)) and isinstance(column_name[1], (tuple, list)):
        return complex_column_expr(column_name, body)
    elif isinstance(column_name, six.string_types) and QUOTED_LITERAL_RE.match(column_name):
        return escape_literal(column_name[1:-1])
    elif column_name == settings.TIME_GROUP_COLUMN:
        expr = settings.TIME_GROUPS[body['granularity']]
    elif NESTED_COL_EXPR_RE.match(column_name):
        expr = tag_expr(column_name)
    elif column_name in ['tags_key', 'tags_value']:
        expr = tags_expr(column_name, body)
    elif column_name == 'issue':
        expr = 'group_id'
    else:
        expr = escape_col(column_name)

    if aggregate:
        if expr:
            expr = u'{}({})'.format(aggregate, expr)
            if aggregate == 'uniq':  # default uniq() result to 0, not null
                expr = 'ifNull({}, 0)'.format(expr)
        else:  # This is the "count()" case where the '()' is already provided
            expr = aggregate

    alias = escape_col(alias or column_name)

    return alias_expr(expr, alias, body)


def complex_column_expr(expr, body, depth=0):
    # TODO instead of the mutual recursion between column_expr and complex_column_expr
    # we should probably encapsulate all this logic in a single recursive column_expr
    if depth == 0:
        # we know the first item is a function
        ret = expr[0]
        expr = expr[1:]

        # if the last item of the toplevel is a string, it's an alias
        alias = None
        if len(expr) > 1 and isinstance(expr[-1], six.string_types):
            alias = expr[-1]
            expr = expr[:-1]
    else:
        # is this a nested function call?
        if len(expr) > 1 and isinstance(expr[1], tuple):
            ret = expr[0]
            expr = expr[1:]
        else:
            ret = ''

    # emptyIfNull(col) is a simple pseudo function supported by Snuba that expands
    # to the actual clickhouse function ifNull(col, '') Until we figure out the best
    # way to disambiguate column names from string literals in complex functions.
    if ret == 'emptyIfNull' and len(expr) >= 1 and isinstance(expr[0], tuple):
        ret = 'ifNull'
        expr = (expr[0] + (Literal('\'\''),),) + expr[1:]

    first = True
    for subexpr in expr:
        if isinstance(subexpr, tuple):
            ret += '(' + complex_column_expr(subexpr, body, depth + 1) + ')'
        else:
            if not first:
                ret += ', '
            if isinstance(subexpr, six.string_types):
                ret += column_expr(subexpr, body)
            else:
                ret += escape_literal(subexpr)
        first = False

    if depth == 0 and alias:
        return alias_expr(ret, alias, body)

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


def tag_expr(column_name):
    """
    Return an expression for the value of a single named tag.

    For tags/contexts, we expand the expression depending on whether the tag is
    "promoted" to a top level column, or whether we have to look in the tags map.
    """
    col, tag = NESTED_COL_EXPR_RE.match(column_name).group(1, 2)

    # For promoted tags, return the column name.
    if col in PROMOTED_COLS:
        actual_tag = TAG_COLUMN_MAP[col].get(tag, tag)
        if actual_tag in PROMOTED_COLS[col]:
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

    # Generate parallel lists of keys and values to arrayJoin on
    if nested_tags_only:
        key_list = '{}.key'.format(col)
        val_list = '{}.value'.format(col)
    else:
        promoted = PROMOTED_COLS[col]
        col_map = COLUMN_TAG_MAP[col]
        key_list = u'arrayConcat([{}], {}.key)'.format(
            u', '.join(u'\'{}\''.format(col_map.get(p, p)) for p in promoted),
            col
        )
        val_list = u'arrayConcat([{}], {}.value)'.format(
            ', '.join(string_col(p) for p in promoted),
            col
        )

    cols_used = all_referenced_columns(body) & set(['tags_key', 'tags_value'])
    if len(cols_used) == 2:
        # If we use both tags_key and tags_value in this query, arrayjoin
        # on (key, value) tag tuples.
        expr = (u'arrayJoin(arrayMap((x,y) -> [x,y], {}, {}))').format(
            key_list,
            val_list
        )

        # put the all_tags expression in the alias cache so we can use the alias
        # to refer to it next time (eg. 'all_tags[1] AS tags_key'). instead of
        # expanding the whole tags expression again.
        expr = alias_expr(expr, 'all_tags', body)
        return u'({})[{}]'.format(expr, 1 if k_or_v == 'key' else 2)
    else:
        # If we are only ever going to use one of tags_key or tags_value, don't
        # bother creating the k/v tuples to arrayJoin on, or the all_tags alias
        # to re-use as we won't need it.
        return 'arrayJoin({})'.format(key_list if k_or_v == 'key' else val_list)


def is_condition(cond_or_list):
    return (
        # A condition is:
        # a 3-tuple
        len(cond_or_list) == 3 and
        # where the middle element is an operator
        cond_or_list[1] in schemas.CONDITION_OPERATORS and
        # and the first element looks like a column name or expression
        isinstance(cond_or_list[0], (six.string_types, tuple, list))
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
    if isinstance(expr, six.string_types):
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


def conditions_expr(conditions, body, depth=0):
    """
    Return a boolean expression suitable for putting in the WHERE clause of the
    query.  The expression is constructed by ANDing groups of OR expressions.
    Expansion of columns is handled, as is replacement of columns with aliases,
    if the column has already been expanded and aliased elsewhere.
    """
    if not conditions:
        return ''

    if depth == 0:
        sub = (conditions_expr(cond, body, depth + 1) for cond in conditions)
        return u' AND '.join(s for s in sub if s)
    elif is_condition(conditions):
        lhs, op, lit = conditions

        if (
            lhs in ('received', 'timestamp') and
            op in ('>', '<', '>=', '<=', '=', '!=') and
            isinstance(lit, str)
        ):
            lit = parse_datetime(lit)

        # If the LHS is a simple column name that refers to an array column
        # (and we are not arrayJoining on that column, which would make it
        # scalar again) and the RHS is a scalar value, we assume that the user
        # actually means to check if any (or all) items in the array match the
        # predicate, so we return an `any(x == value for x in array_column)`
        # type expression. We assume that operators looking for a specific value
        # (IN, =, LIKE) are looking for rows where any array value matches, and
        # exclusionary operators (NOT IN, NOT LIKE, !=) are looking for rows
        # where all elements match (eg. all NOT LIKE 'foo').
        if (
            isinstance(lhs, six.string_types) and
            lhs in ALL_COLUMNS and
            type(ALL_COLUMNS[lhs].type) == clickhouse.Array and
            ALL_COLUMNS[lhs].base_name != body.get('arrayjoin') and
            not isinstance(lit, (list, tuple))
            ):
            any_or_all = 'arrayExists' if op in schemas.POSITIVE_OPERATORS else 'arrayAll'
            return u'{}(x -> assumeNotNull(x {} {}), {})'.format(
                any_or_all,
                op,
                escape_literal(lit),
                column_expr(lhs, body)
            )
        else:
            return u'{} {} {}'.format(
                column_expr(lhs, body),
                op,
                escape_literal(lit)
            )

    elif depth == 1:
        sub = (conditions_expr(cond, body, depth + 1) for cond in conditions)
        sub = [s for s in sub if s]
        res = u' OR '.join(sub)
        return u'({})'.format(res) if len(sub) > 1 else res
    else:
        raise InvalidConditionException(str(conditions))


def escape_literal(value):
    """
    Escape a literal value for use in a SQL clause
    """
    # TODO in both the Literal and the raw string cases, we need to
    # sanitize the string from potential SQL injection.
    if isinstance(value, Literal):
        return value.literal
    elif isinstance(value, six.string_types):
        value = value.replace("'", "\\'")
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
    project_ids = to_list(body['project'])
    project_id = project_ids[0] if project_ids else 0  # TODO rate limit on every project in the list?
    stats = stats or {}
    grl, gcl, prl, pcl, use_cache = state.get_configs([
        ('global_per_second_limit', 1000),
        ('global_concurrent_limit', 1000),
        ('project_per_second_limit', 1000),
        ('project_concurrent_limit', 1000),
        ('use_cache', 0),
    ])

    # Specific projects can have their rate limits overridden
    prl, pcl = state.get_configs([
        ('project_per_second_limit_{}'.format(project_id), prl),
        ('project_concurrent_limit_{}'.format(project_id), pcl),
    ])

    all_confs = six.iteritems(state.get_all_configs())
    query_settings = {k.split('/', 1)[1]: v for k, v in all_confs if k.startswith('query_settings/')}

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
                            error = six.text_type(ex)
                            status = 500
                            logger.error("Error running query: %s\n%s", sql, error)
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
                        result = {'error': {
                            'type': 'ratelimit',
                            'message': 'rate limit exceeded',
                        }}

    stats.update(query_settings)
    state.record_query({
        'request': body,
        'sql': sql,
        'timing': timer,
        'stats': stats,
        'status': status,
    })

    if settings.RECORD_QUERIES:
        timer.send_metrics_to(metrics)
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

    def send_metrics_to(self, metrics):
        name = self.marks[0][0]
        final = self.finish()
        metrics.timing(name, final['duration_ms'])
        for mark, duration in six.iteritems(final['marks_ms']):
            metrics.timing('{}.{}'.format(name, mark), duration)


def time_request(name):
    def decorator(func):
        @wraps(func)
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
