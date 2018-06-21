import logging
import os

from copy import deepcopy
from datetime import timedelta
from dateutil.parser import parse as parse_datetime
from flask import Flask, render_template, request
from hashlib import md5
from markdown import markdown
from raven.contrib.flask import Sentry
import simplejson as json

from snuba import settings, util, schemas, state
from snuba.clickhouse import ClickhousePool


logger = logging.getLogger('snuba.api')
logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL.upper()), format='%(asctime)s %(message)s')


clickhouse_rw = ClickhousePool()
clickhouse_ro = ClickhousePool(client_settings={
    'readonly': True,
})


try:
    import uwsgi
except ImportError:
    def check_down_file_exists():
        return False
else:
    def check_down_file_exists():
        try:
            return os.stat('/tmp/snuba.down').st_mtime > uwsgi.started_on
        except OSError:
            return False


def check_clickhouse():
    try:
        return any(settings.CLICKHOUSE_TABLE == r[0] for r in clickhouse_ro.execute('show tables'))
    except IndexError:
        return False


application = Flask(__name__, static_url_path='')
application.testing = settings.TESTING
application.debug = settings.DEBUG

sentry = Sentry(application, dsn=settings.SENTRY_DSN)
metrics = util.create_metrics(settings.DOGSTATSD_HOST, settings.DOGSTATSD_PORT, 'snuba.api')


@application.route('/')
def root():
    with open('README.md') as f:
        return render_template('index.html', body=markdown(f.read()))


@application.route('/css/<path:path>')
def send_css(path):
    return application.send_static_file(os.path.join('css', path))


@application.route('/img/<path:path>')
@application.route('/snuba/static/img/<path:path>')
def send_img(path):
    return application.send_static_file(os.path.join('img', path))


@application.route('/dashboard')
@application.route('/dashboard.<fmt>')
def dashboard(fmt='html'):
    if fmt == 'json':
        result = {
            'queries': state.get_queries(),
            'concurrent': {k: state.get_concurrent(k) for k in ['global']},
            'rates': {k: state.get_rates(k) for k in ['global']},
        }
        return (json.dumps(result), 200, {'Content-Type': 'application/json'})
    else:
        return application.send_static_file('dashboard.html')


@application.route('/config')
@application.route('/config.<fmt>', methods=['GET', 'POST'])
def config(fmt='html'):
    if fmt == 'json':
        if request.method == 'GET':
            return (json.dumps(state.get_all_configs()), 200, {'Content-Type': 'application/json'})
        elif request.method == 'POST':
            state.set_configs(json.loads(request.data))
            return (json.dumps(state.get_all_configs()), 200, {'Content-Type': 'application/json'})
    else:
        return application.send_static_file('config.html')


@application.route('/health')
def health():
    down_file_exists = check_down_file_exists()
    clickhouse_health = check_clickhouse()

    if not down_file_exists and clickhouse_health:
        body = {'status': 'ok'}
        status = 200
    else:
        body = {
            'down_file_exists': down_file_exists,
            'clickhouse_ok': clickhouse_health,
        }
        status = 502

    return (json.dumps(body), status, {'Content-Type': 'application/json'})


@application.route('/query', methods=['GET', 'POST'])
@util.time_request('query')
@util.validate_request(schemas.QUERY_SCHEMA)
def query(validated_body=None, timer=None):
    if request.method == 'GET':
        query_template = schemas.generate(schemas.QUERY_SCHEMA)
        template_str = json.dumps(query_template, sort_keys=True, indent=4)
        return render_template('query.html', query_template=template_str)

    body = deepcopy(validated_body)
    project_ids = util.to_list(body['project'])
    to_date = parse_datetime(body['to_date'])
    from_date = parse_datetime(body['from_date'])
    assert from_date <= to_date

    max_days = state.get_config('max_days', None)
    if max_days is not None and (to_date - from_date).days > max_days:
        from_date = to_date - timedelta(days=max_days)

    where_conditions = body['conditions']
    where_conditions.extend([
        ('timestamp', '>=', from_date),
        ('timestamp', '<', to_date),
        ('deleted', '=', 0),
        ('project_id', 'IN', project_ids),
    ])
    having_conditions = body['having']

    aggregate_exprs = [
        util.column_expr(col, body, alias, agg)
        for (agg, col, alias) in body['aggregations']
    ]
    groupby = util.to_list(body['groupby'])
    group_exprs = [util.column_expr(gb, body) for gb in groupby]

    selected_cols = [util.column_expr(colname, body) for colname in body['selected_columns']]

    select_exprs = group_exprs + aggregate_exprs + selected_cols

    select_clause = u'SELECT {}'.format(', '.join(select_exprs))
    from_clause = u'FROM {}'.format(settings.CLICKHOUSE_TABLE)

    joins = []
    issue_expr = util.issue_expr(body)
    if issue_expr:
        joins.append(issue_expr)
        where_conditions.append(
            ('timestamp', '>', util.Literal(
                'ifNull(hash_timestamp, CAST(\'1970-01-01 00:00:00\', \'DateTime\'))')
            )
        )
    if 'arrayjoin' in body:
        joins.append(u'ARRAY JOIN {}'.format(body['arrayjoin']))
    join_clause = ' '.join(joins)

    where_clause = ''
    if where_conditions:
        where_conditions = list(set(util.tuplify(where_conditions)))
        where_clause = u'WHERE {}'.format(util.condition_expr(where_conditions, body))

    prewhere_clause = ''
    prewhere_conditions = []
    # Experiment, if only a single issue with a single hash, add that as a condition in PREWHERE
    if 'issues' in body and len(body['issues']) == 1 and len(body['issues'][0][1]) == 1:
        hash_ = body['issues'][0][1][0]
        hash_ = hash_[0] if isinstance(hash_, (list, tuple)) else hash_ # strip out tombstone
        prewhere_conditions.append(['primary_hash', '=', hash_])

    if not prewhere_conditions and settings.PREWHERE_KEYS:
        prewhere_conditions.extend([c for c in where_conditions if c[0] in settings.PREWHERE_KEYS])

    if prewhere_conditions:
        prewhere_clause = u'PREWHERE {}'.format(util.condition_expr(prewhere_conditions, body))

    having_clause = ''
    if having_conditions:
        assert groupby, 'found HAVING clause with no GROUP BY'
        having_clause = u'HAVING {}'.format(util.condition_expr(having_conditions, body))

    group_clause = ', '.join(util.column_expr(gb, body) for gb in groupby)
    if group_clause:
        group_clause = 'GROUP BY ({})'.format(group_clause)

    order_clause = ''
    if body.get('orderby'):
        desc = body['orderby'].startswith('-')
        orderby = body['orderby'].lstrip('-')
        order_clause = u'ORDER BY {} {}'.format(
            util.column_expr(orderby, body), 'DESC' if desc else 'ASC'
        )

    limit_clause = ''
    if 'limit' in body:
        limit_clause = 'LIMIT {}, {}'.format(body.get('offset', 0), body['limit'])

    sql = ' '.join([c for c in [
        select_clause,
        from_clause,
        join_clause,
        prewhere_clause,
        where_clause,
        group_clause,
        having_clause,
        order_clause,
        limit_clause
    ] if c])

    timer.mark('prepare_query')

    grl, gcl, prl, pcl, use_query_id, use_cache = state.get_configs([
        ('global_per_second_limit', 1000),
        ('global_concurrent_limit', 1000),
        ('project_per_second_limit', 1000),
        ('project_concurrent_limit', 1000),
        ('use_query_id', 0),
        ('use_cache', 0),
    ])
    concurr, rate, g_concurr, g_rate = 0, 0, 0, 0
    timer.mark('get_configs')

    query_id = md5(util.force_bytes(sql)).hexdigest() if use_query_id else None
    cache_hit = use_cache
    is_dupe = False
    result = {}
    status = 200

    with state.rate_limit('global', grl, gcl) as (g_allowed, g_concurr, g_rate):
        with state.rate_limit(project_ids[0], prl, pcl) as (allowed, concurr, rate):
            timer.mark('rate_limit')
            if not g_allowed or not allowed:
                status = 429
            else:
                with state.deduper(query_id) as is_dupe:
                    timer.mark('dedupe_wait')
                    if use_cache:
                        result = json.loads(state.get_result(query_id) or "{}")
                        timer.mark('cache_get')

                    if not result:
                        cache_hit = False
                        result = util.raw_query(sql, clickhouse_ro, query_id)
                        timer.mark('execute')
                        if result.get('error'):
                            status = 500
                        elif use_cache:
                            state.set_result(query_id, json.dumps(result))
                            timer.mark('cache_set')

    stats = {
        'is_duplicate': is_dupe,
        'cache_hit': cache_hit,
        'num_days': (to_date - from_date).days,
        'num_issues': len(validated_body.get('issues', [])),
        'num_hashes': sum(len(h) for i, h in validated_body.get('issues', [])),
        'global_concurrent': g_concurr,
        'global_rate': g_rate,
        'project_concurrent': concurr,
        'project_rate': rate,
    }
    metrics.gauge('query.global_concurrent', g_concurr)
    timer.record(metrics)
    state.record_query({
        'request': validated_body,
        'referrer': request.referrer,
        'sql': sql,
        'timing': timer,
        'stats': stats,
    })

    result['timing'] = timer
    if settings.STATS_IN_RESPONSE:
        result['stats'] = stats

    return (json.dumps(result, for_json=True), status, {'Content-Type': 'application/json'})


if application.debug or application.testing:
    # These should only be used for testing/debugging. Note that the database name
    # is hardcoded to 'test' on purpose to avoid scary production mishaps.
    TEST_TABLE = 'test'

    @application.route('/tests/insert', methods=['POST'])
    def write():
        from snuba.clickhouse import get_table_definition, get_test_engine
        from snuba.processor import process_message
        from snuba.writer import row_from_processed_event, write_rows

        body = json.loads(request.data)

        rows = []
        for event in body:
            _, _, processed = process_message(event)
            row = row_from_processed_event(processed)
            rows.append(row)

        clickhouse_rw.execute(
            get_table_definition(TEST_TABLE, get_test_engine(), settings.SCHEMA_COLUMNS)
        )
        write_rows(clickhouse_rw, table=TEST_TABLE, columns=settings.WRITER_COLUMNS, rows=rows)

        return ('ok', 200, {'Content-Type': 'text/plain'})

    @application.route('/tests/drop', methods=['POST'])
    def drop():
        clickhouse_rw.execute("DROP TABLE IF EXISTS %s" % TEST_TABLE)


        return ('ok', 200, {'Content-Type': 'text/plain'})
