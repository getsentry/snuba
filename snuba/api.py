import logging
import os
import six

from copy import deepcopy
from datetime import datetime, timedelta
from flask import Flask, render_template, request
from markdown import markdown
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
import simplejson as json

from snuba import generalizer, schemas, settings, state, util
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
    except Exception:
        return False


application = Flask(__name__, static_url_path='')
application.testing = settings.TESTING
application.debug = settings.DEBUG

sentry_sdk.init(
    dsn=settings.SENTRY_DSN,
    integrations=[FlaskIntegration()],
    release=os.getenv('SNUBA_RELEASE')
)


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
            state.set_configs(json.loads(request.data), user=request.headers.get('x-email'))
            return (json.dumps(state.get_raw_configs()), 200, {'Content-Type': 'application/json'})
    else:
        return application.send_static_file('config.html')


@application.route('/config/changes.json')
def config_changes():
    return (
        json.dumps(state.get_config_changes()),
        200,
        {'Content-Type': 'application/json'},
    )


@application.route('/health')
def health():
    down_file_exists = check_down_file_exists()
    thorough = request.args.get('thorough', False)
    clickhouse_health = check_clickhouse() if thorough else True

    if not down_file_exists and clickhouse_health:
        body = {'status': 'ok'}
        status = 200
    else:
        body = {
            'down_file_exists': down_file_exists,
        }
        if thorough:
            body['clickhouse_ok'] = clickhouse_health
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

    result, status = parse_and_run_query(validated_body, timer)
    return (
        json.dumps(
            result,
            for_json=True,
            default=lambda obj: obj.isoformat() if isinstance(obj, datetime) else obj),
        status,
        {'Content-Type': 'application/json'}
    )


@generalizer.generalize
def parse_and_run_query(validated_body, timer):
    max_days, table, date_align, config_sample, use_final = state.get_configs([
        ('max_days', None),
        ('clickhouse_table', settings.CLICKHOUSE_TABLE),
        ('date_align_seconds', 1),
        ('sample', 1),
        ('use_final', 0),
    ])
    body = deepcopy(validated_body)
    stats = {}
    to_date = util.parse_datetime(body['to_date'], date_align)
    from_date = util.parse_datetime(body['from_date'], date_align)
    assert from_date <= to_date

    if max_days is not None and (to_date - from_date).days > max_days:
        from_date = to_date - timedelta(days=max_days)

    where_conditions = body.get('conditions', [])
    where_conditions.extend([
        ('timestamp', '>=', from_date),
        ('timestamp', '<', to_date),
        ('deleted', '=', 0),
    ])
    # NOTE: we rely entirely on the schema to make sure that regular snuba
    # queries are required to send a project_id filter. Some other special
    # internal query types do not require a project_id filter.
    project_ids = util.to_list(body['project'])
    if project_ids:
        where_conditions.append(('project_id', 'IN', project_ids))

    having_conditions = body.get('having', [])

    aggregate_exprs = [
        util.column_expr(col, body, alias, agg)
        for (agg, col, alias) in body['aggregations']
    ]
    groupby = util.to_list(body['groupby'])
    group_exprs = [util.column_expr(gb, body) for gb in groupby]

    selected_cols = [util.column_expr(util.tuplify(colname), body)
                     for colname in body.get('selected_columns', [])]

    select_exprs = group_exprs + aggregate_exprs + selected_cols
    select_clause = u'SELECT {}'.format(', '.join(select_exprs))

    from_clause = u'FROM {}'.format(table)

    if use_final:
        from_clause = u'{} FINAL'.format(from_clause)

    sample = body.get('sample', config_sample)
    if sample != 1:
        from_clause = u'{} SAMPLE {}'.format(from_clause, sample)

    joins = []
    prewhere_clause = ''
    prewhere_conditions = []

    issues = body.get('issues', [])
    use_group_id_column = body.get('use_group_id_column')
    if not use_group_id_column:
        issue_expr = util.issue_expr(body)
        if issue_expr:
            joins.append(issue_expr)
            where_conditions.append(
                ('timestamp', '>', util.Literal(
                    'ifNull(hash_timestamp, CAST(\'1970-01-01 00:00:00\', \'DateTime\'))')
                )
            )

        # Experiment, if only a single issue with a single hash, add that as a condition in PREWHERE
        if issues \
                and (len(issues) == 1) \
                and (len(issues[0][2]) == 1) \
                and (len(project_ids) == 1):

            hash_ = issues[0][2][0]
            hash_ = hash_[0] if isinstance(hash_, (list, tuple)) else hash_  # strip out tombstone
            prewhere_conditions.append(['primary_hash', '=', hash_])

    if 'arrayjoin' in body:
        joins.append(u'ARRAY JOIN {}'.format(body['arrayjoin']))
    join_clause = ' '.join(joins)

    where_clause = ''
    if where_conditions:
        where_conditions = list(set(util.tuplify(where_conditions)))
        where_clause = u'WHERE {}'.format(util.condition_expr(where_conditions, body))

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
        if body.get('totals', False):
            group_clause = 'GROUP BY ({}) WITH TOTALS'.format(group_clause)
        else:
            group_clause = 'GROUP BY ({})'.format(group_clause)

    order_clause = ''
    if body.get('orderby'):
        orderby = [util.column_expr(util.tuplify(ob), body) for ob in util.to_list(body['orderby'])]
        orderby = [u'{} {}'.format(
            ob.lstrip('-'),
            'DESC' if ob.startswith('-') else 'ASC'
        ) for ob in orderby]
        order_clause = u'ORDER BY {}'.format(', '.join(orderby))

    limitby_clause = ''
    if 'limitby' in body:
        limitby_clause = 'LIMIT {} BY {}'.format(*body['limitby'])

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
        limitby_clause,
        limit_clause
    ] if c])

    timer.mark('prepare_query')

    stats.update({
        'clickhouse_table': table,
        'referrer': request.referrer,
        'num_days': (to_date - from_date).days,
        'num_projects': len(project_ids),
        'num_issues': len(issues),
        'num_hashes': sum(len(i[-1]) for i in issues) if not use_group_id_column else 0,
        'sample': sample,
    })

    return util.raw_query(validated_body, sql, clickhouse_ro, timer, stats)


# Special internal endpoints that compute global aggregate data that we want to
# use internally.

@application.route('/internal/sdk-stats', methods=['POST'])
@util.time_request('sdk-stats')
@util.validate_request(schemas.SDK_STATS_SCHEMA)
def sdk_distribution(validated_body, timer):
    validated_body['project'] = []
    validated_body['aggregations'] = [
        ['uniq', 'project_id', 'projects'],
        ['count()', None, 'count'],
    ]
    validated_body['groupby'].extend(['sdk_name', 'time'])
    result, status = parse_and_run_query(validated_body, timer)
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
            _, processed = process_message(event)
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

    @application.route('/tests/error')
    def error():
        1 / 0
