import logging
import os

from copy import deepcopy
from datetime import datetime, timedelta
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

#sentry = Sentry(application, dsn=settings.SENTRY_DSN)


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
            return (json.dumps(state.get_raw_configs()), 200, {'Content-Type': 'application/json'})
        elif request.method == 'POST':
            state.set_configs(json.loads(request.data))
            return (json.dumps(state.get_raw_configs()), 200, {'Content-Type': 'application/json'})
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

    max_days, table, date_align = state.get_configs([
        ('max_days', None),
        ('clickhouse_table', settings.CLICKHOUSE_TABLE),
        ('date_align_seconds', 1),
    ])
    body = deepcopy(validated_body)
    stats = {}
    project_ids = util.to_list(body['project'])
    to_date = util.parse_datetime(body['to_date'], date_align)
    from_date = util.parse_datetime(body['from_date'], date_align)
    assert from_date <= to_date

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
    from_clause = u'FROM {}'.format(table)

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
    if 'issues' in body \
        and (len(body['issues']) == 1) \
        and (len(body['issues'][0][2]) == 1) \
        and (len(project_ids) == 1):

        hash_ = body['issues'][0][2][0]
        hash_ = hash_[0] if isinstance(hash_, (list, tuple)) else hash_  # strip out tombstone
        prewhere_conditions.append(['primary_hash', '=', hash_])

    if not prewhere_conditions and settings.PREWHERE_KEYS:
        prewhere_conditions.extend([c for c in where_conditions if c and c[0] in settings.PREWHERE_KEYS])

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

    stats.update({
        'clickhouse_table': table,
        'referrer': request.referrer,
        'num_days': (to_date - from_date).days,
        'num_projects': len(project_ids),
        'num_issues': len(body.get('issues', [])),
        'num_hashes': sum(len(i[-1]) for i in body.get('issues', [])),
    })

    result, status = util.raw_query(validated_body, sql, clickhouse_ro, timer, stats)

    return (
        json.dumps(
            result,
            for_json=True,
            default=lambda obj: obj.isoformat() if isinstance(obj, datetime) else obj),
        status,
        {'Content-Type': 'application/json'}
    )


if application.debug or application.testing:
    # These should only be used for testing/debugging. Note that the database name
    # is checked to avoid scary production mishaps.
    assert settings.CLICKHOUSE_TABLE in ('dev', 'test')

    def ensure_table_exists():
        from snuba.clickhouse import get_table_definition, get_test_engine

        clickhouse_rw.execute(
            get_table_definition(
                name=settings.CLICKHOUSE_TABLE,
                engine=get_test_engine(),
                columns=settings.SCHEMA_COLUMNS
            )
        )

    ensure_table_exists()

    @application.route('/tests/insert', methods=['POST'])
    def write():
        from snuba.processor import process_message
        from snuba.writer import row_from_processed_event, write_rows

        body = json.loads(request.data)

        rows = []
        for event in body:
            _, _, processed = process_message(event)
            row = row_from_processed_event(processed)
            rows.append(row)

        ensure_table_exists()
        write_rows(clickhouse_rw, table=settings.CLICKHOUSE_TABLE, columns=settings.WRITER_COLUMNS, rows=rows)
        return ('ok', 200, {'Content-Type': 'text/plain'})

    @application.route('/tests/drop', methods=['POST'])
    def drop():
        clickhouse_rw.execute("DROP TABLE IF EXISTS %s" % settings.CLICKHOUSE_TABLE)
        ensure_table_exists()
        return ('ok', 200, {'Content-Type': 'text/plain'})
