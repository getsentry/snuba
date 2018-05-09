import logging
import os

from copy import deepcopy
from datetime import timedelta
from dateutil.parser import parse as parse_datetime
from flask import Flask, render_template, request
from markdown import markdown
from raven.contrib.flask import Sentry
import simplejson as json

from snuba import settings, util, schemas, state
from snuba.clickhouse import Clickhouse


logger = logging.getLogger('snuba.api')
logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL.upper()), format='%(asctime)s %(message)s')


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
    with Clickhouse() as clickhouse:
        try:
            return settings.CLICKHOUSE_TABLE in clickhouse.execute('show tables')[0]
        except IndexError:
            return False


app = Flask(__name__, static_url_path='')
app.testing = settings.TESTING
app.debug = settings.DEBUG

sentry = Sentry(app, dsn=settings.SENTRY_DSN)
metrics = util.create_metrics(settings.DOGSTATSD_HOST, settings.DOGSTATSD_PORT, 'snuba.api')


@app.route('/')
def root():
    with open('README.md') as f:
        return render_template('index.html', body=markdown(f.read()))

@app.route('/dashboard')
@app.route('/dashboard.<fmt>')
def dashboard(fmt='html'):
    if fmt == 'json':
        result = {
            'queries': state.get_queries(),
            'concurrent': {k: state.get_concurrent(k) for k in ['global']},
            'rates': {k: state.get_rates(k) for k in ['global']},
        }
        return (json.dumps(result), 200, {'Content-Type': 'application/json'})
    else:
        return app.send_static_file('dashboard.html')

@app.route('/health')
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


@app.route('/query', methods=['GET', 'POST'])
@util.time_request('query')
@util.validate_request(schemas.QUERY_SCHEMA)
def query(validated_body, timer):
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
        ('project_id', 'IN', project_ids),
    ])
    having_conditions = body['having']

    aggregate_exprs = [
        util.column_expr(col, body, alias, agg)
        for (agg, col, alias) in body['aggregations']
    ]
    groupby = util.to_list(body['groupby'])
    group_exprs = [util.column_expr(gb, body) for gb in groupby]

    select_exprs = group_exprs + aggregate_exprs
    select_clause = u'SELECT {}'.format(', '.join(select_exprs))
    from_clause = u'FROM {}'.format(settings.CLICKHOUSE_TABLE)
    join_clause = u'ARRAY JOIN {}'.format(body['arrayjoin']) if 'arrayjoin' in body else ''

    where_clause = ''
    if where_conditions:
        where_clause = u'WHERE {}'.format(util.condition_expr(where_conditions, body))

    having_clause = ''
    if having_conditions:
        assert groupby, 'found HAVING clause with no GROUP BY'
        having_clause = u'HAVING {}'.format(util.condition_expr(having_conditions, body))

    group_clause = ', '.join(util.column_expr(gb, body) for gb in groupby)
    if group_clause:
        group_clause = 'GROUP BY ({})'.format(group_clause)

    order_clause = ''
    desc = body['orderby'].startswith('-')
    orderby = body['orderby'].lstrip('-')
    if orderby in body.get('alias_cache', {}).values():
        order_clause = u'ORDER BY `{}` {}'.format(orderby, 'DESC' if desc else 'ASC')

    limit_clause = ''
    if 'limit' in body:
        limit_clause = 'LIMIT {}, {}'.format(body.get('offset', 0), body['limit'])

    sql = ' '.join([c for c in [
        select_clause,
        from_clause,
        join_clause,
        where_clause,
        group_clause,
        having_clause,
        order_clause,
        limit_clause
    ] if c])

    timer.mark('prepare_query')

    result = {}
    gpsl = state.get_config('global_per_second_limit', 1000)
    gcl = state.get_config('global_concurrent_limit', 1000)
    ppsl = state.get_config('project_per_second_limit', 1000)
    pcl = state.get_config('project_concurrent_limit', 1000)
    with state.rate_limit('global', gpsl, gcl) as global_allowed:
        with state.rate_limit(project_ids[0], ppsl, pcl) as allowed:
            if not global_allowed or not allowed:
                status = 429
            else:
                with Clickhouse() as clickhouse:
                    result = util.raw_query(sql, clickhouse)
                    timer.mark('execute')

                if result.get('error'):
                    logger.error(result['error'])
                    status = 500
                else:
                    status = 200

    result['timing'] = timer
    timer.record(metrics)
    state.record_query({
        'request': validated_body,
        'sql': sql,
        'result': result,
    })

    return (json.dumps(result, for_json=True), status, {'Content-Type': 'application/json'})


if app.debug or app.testing:
    # These should only be used for testing/debugging. Note that the database name
    # is hardcoded to 'test' on purpose to avoid scary production mishaps.
    TEST_TABLE = 'test'

    @app.route('/tests/insert', methods=['POST'])
    def write():
        from snuba.processor import process_raw_event
        from snuba.writer import row_from_processed_event, write_rows

        body = json.loads(request.data)

        rows = []
        for event in body:
            processed = process_raw_event(event)
            row = row_from_processed_event(processed)
            rows.append(row)

        with Clickhouse() as clickhouse:
            clickhouse.execute(util.get_table_definition(TEST_TABLE, 'Memory', settings.SCHEMA_COLUMNS))
            write_rows(clickhouse, table=TEST_TABLE, columns=settings.WRITER_COLUMNS, rows=rows)

        return ('ok', 200, {'Content-Type': 'text/plain'})

    @app.route('/tests/drop', methods=['POST'])
    def drop():
        with Clickhouse() as clickhouse:
            clickhouse.execute("DROP TABLE IF EXISTS %s" % TEST_TABLE)

        return ('ok', 200, {'Content-Type': 'text/plain'})
