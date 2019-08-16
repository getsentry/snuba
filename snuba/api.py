import logging
import os

from copy import deepcopy
from datetime import datetime, timedelta
from flask import Flask, redirect, render_template, request
from markdown import markdown
from uuid import uuid1
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.integrations.gnu_backtrace import GnuBacktraceIntegration
import simplejson as json
from werkzeug.exceptions import BadRequest
import jsonschema

from snuba import schemas, settings, state, util
from snuba.clickhouse import ClickhousePool
from snuba.replacer import get_projects_query_flags
from snuba.split import split_query
from snuba.datasets.factory import InvalidDatasetError, get_dataset, get_enabled_dataset_names
from snuba.datasets.schema import local_dataset_mode
from snuba.redis import redis_client
from snuba.util import Timer


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
        clickhouse_tables = clickhouse_ro.execute('show tables')
        for name in get_enabled_dataset_names():
            dataset = get_dataset(name)
            table_name = dataset.get_schema().get_table_name()
            if (table_name,) not in clickhouse_tables:
                return False
        return True

    except Exception:
        return False


application = Flask(__name__, static_url_path='')
application.testing = settings.TESTING
application.debug = settings.DEBUG

sentry_sdk.init(
    dsn=settings.SENTRY_DSN,
    integrations=[FlaskIntegration(), GnuBacktraceIntegration()],
    release=os.getenv('SNUBA_RELEASE')
)


@application.errorhandler(BadRequest)
def handle_bad_request(exception: BadRequest):
    cause = getattr(exception, '__cause__', None)
    if isinstance(cause, json.errors.JSONDecodeError):
        data = {'error': {'type': 'json', 'message': str(cause)}}
    elif isinstance(cause, jsonschema.ValidationError):
        data = {'error': {
            'type': 'schema',
            'message': cause.message,
            'path': list(cause.path),
            'schema': cause.schema,
        }}
    else:
        data = {'error': {'type': 'request', 'message': str(exception)}}

    def default_encode(value):
        # XXX: This is necessary for rendering schema defaults values that are
        # generated by callables, rather than constants.
        if callable(value):
            return value()
        else:
            raise TypeError()

    return json.dumps(data, indent=4, default=default_encode), 400, {'Content-Type': 'application/json'}


@application.errorhandler(InvalidDatasetError)
def handle_invalid_dataset(exception: InvalidDatasetError):
    data = {'error': {'type': 'dataset', 'message': str(exception)}}
    return json.dumps(data, sort_keys=True, indent=4), 404, {'Content-Type': 'application/json'}


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
            state.set_configs(json.loads(request.data), user=request.headers.get('x-forwarded-email'))
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


def parse_request_body(request):
    try:
        return json.loads(request.data)
    except json.errors.JSONDecodeError as error:
        raise BadRequest(str(error)) from error


def validate_request_content(body, schema, timer):
    try:
        schemas.validate(body, schema)
    except jsonschema.ValidationError as error:
        raise BadRequest(str(error)) from error

    timer.mark('validate_schema')


@application.route('/query', methods=['GET', 'POST'])
@util.time_request('query')
def unqualified_query_view(*, timer: Timer):
    if request.method == 'GET':
        return redirect(f"/{settings.DEFAULT_DATASET_NAME}/query", code=302)
    elif request.method == 'POST':
        body = parse_request_body(request)
        dataset = get_dataset(body.pop('dataset', settings.DEFAULT_DATASET_NAME))
        return dataset_query(dataset, body, timer)
    else:
        assert False, 'unexpected fallthrough'


@application.route('/<dataset_name>/query', methods=['GET', 'POST'])
@util.time_request('query')
def dataset_query_view(*, dataset_name: str, timer: Timer):
    dataset = get_dataset(dataset_name)
    if request.method == 'GET':
        return render_template(
            'query.html',
            query_template=json.dumps(
                schemas.generate(dataset.get_query_schema()),
                indent=4,
            ),
        )
    elif request.method == 'POST':
        body = parse_request_body(request)
        return dataset_query(dataset, body, timer)
    else:
        assert False, 'unexpected fallthrough'


def dataset_query(dataset, body, timer):
    assert request.method == 'POST'
    ensure_table_exists(dataset)

    validate_request_content(body, dataset.get_query_schema(), timer)

    result, status = parse_and_run_query(dataset, body, timer)
    return (
        json.dumps(
            result,
            for_json=True,
            default=lambda obj: obj.isoformat() if isinstance(obj, datetime) else obj),
        status,
        {'Content-Type': 'application/json'}
    )


@split_query
def parse_and_run_query(dataset, validated_body, timer):
    body = deepcopy(validated_body)

    table = dataset.get_schema().get_table_name()

    max_days, date_align, config_sample = state.get_configs([
        ('max_days', None),
        ('date_align_seconds', 1),
        ('sample', 1),
    ])

    to_date = util.parse_datetime(body['to_date'], date_align)
    from_date = util.parse_datetime(body['from_date'], date_align)
    assert from_date <= to_date

    if max_days is not None and (to_date - from_date).days > max_days:
        from_date = to_date - timedelta(days=max_days)

    where_conditions = body.get('conditions', [])
    where_conditions.extend([
        ('timestamp', '>=', from_date),
        ('timestamp', '<', to_date),
    ])
    where_conditions.extend(dataset.default_conditions())

    # NOTE: we rely entirely on the schema to make sure that regular snuba
    # queries are required to send a project_id filter. Some other special
    # internal query types do not require a project_id filter.
    project_ids = util.to_list(body['project'])
    if project_ids:
        where_conditions.append(('project_id', 'IN', project_ids))

    having_conditions = body.get('having', [])

    aggregate_exprs = [
        util.column_expr(dataset, col, body, alias, agg)
        for (agg, col, alias) in body['aggregations']
    ]
    groupby = util.to_list(body['groupby'])
    group_exprs = [util.column_expr(dataset, gb, body) for gb in groupby]

    selected_cols = [util.column_expr(dataset, util.tuplify(colname), body)
                     for colname in body.get('selected_columns', [])]

    select_exprs = group_exprs + aggregate_exprs + selected_cols
    select_clause = u'SELECT {}'.format(', '.join(select_exprs))

    from_clause = u'FROM {}'.format(table)

    turbo = body.get('turbo', False)
    if not turbo:
        final, exclude_group_ids = get_projects_query_flags(project_ids)
        if not final and exclude_group_ids:
            # If the number of groups to exclude exceeds our limit, the query
            # should just use final instead of the exclusion set.
            max_group_ids_exclude = state.get_config('max_group_ids_exclude', settings.REPLACER_MAX_GROUP_IDS_TO_EXCLUDE)
            if len(exclude_group_ids) > max_group_ids_exclude:
                final = True
            else:
                where_conditions.append(('group_id', 'NOT IN', exclude_group_ids))
    else:
        final = False

    if final:
        from_clause = u'{} FINAL'.format(from_clause)

    sample = body.get('sample', settings.TURBO_SAMPLE_RATE if turbo else config_sample)
    if sample != 1:
        from_clause = u'{} SAMPLE {}'.format(from_clause, sample)

    joins = []

    if 'arrayjoin' in body:
        joins.append(u'ARRAY JOIN {}'.format(body['arrayjoin']))
    join_clause = ' '.join(joins)

    prewhere_conditions = []
    if settings.PREWHERE_KEYS:
        # Add any condition to PREWHERE if:
        # - It is a single top-level condition (not OR-nested), and
        # - Any of its referenced columns are in PREWHERE_KEYS
        prewhere_candidates = [
            (util.columns_in_expr(cond[0]), cond)
            for cond in where_conditions if util.is_condition(cond) and
            any(col in settings.PREWHERE_KEYS for col in util.columns_in_expr(cond[0]))
        ]
        # Use the condition that has the highest priority (based on the
        # position of its columns in the PREWHERE_KEYS list)
        prewhere_candidates = sorted([
            (min(settings.PREWHERE_KEYS.index(col) for col in cols if col in settings.PREWHERE_KEYS), cond)
            for cols, cond in prewhere_candidates
        ], key=lambda priority_and_col: priority_and_col[0])
        if prewhere_candidates:
            prewhere_conditions = [cond for _, cond in prewhere_candidates][:settings.MAX_PREWHERE_CONDITIONS]
            where_conditions = list(filter(lambda cond: cond not in prewhere_conditions, where_conditions))

    where_clause = ''
    if where_conditions:
        where_conditions = util.tuplify(where_conditions)
        where_clause = u'WHERE {}'.format(util.conditions_expr(dataset, where_conditions, body))

    prewhere_clause = ''
    if prewhere_conditions:
        prewhere_clause = u'PREWHERE {}'.format(util.conditions_expr(dataset, prewhere_conditions, body))

    having_clause = ''
    if having_conditions:
        assert groupby, 'found HAVING clause with no GROUP BY'
        having_clause = u'HAVING {}'.format(util.conditions_expr(dataset, having_conditions, body))

    group_clause = ', '.join(util.column_expr(dataset, gb, body) for gb in groupby)
    if group_clause:
        if body.get('totals', False):
            group_clause = 'GROUP BY ({}) WITH TOTALS'.format(group_clause)
        else:
            group_clause = 'GROUP BY ({})'.format(group_clause)

    order_clause = ''
    if body.get('orderby'):
        orderby = [util.column_expr(dataset, util.tuplify(ob), body) for ob in util.to_list(body['orderby'])]
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

    stats = {
        'clickhouse_table': table,
        'final': final,
        'referrer': request.referrer,
        'num_days': (to_date - from_date).days,
        'num_projects': len(project_ids),
        'sample': sample,
    }

    return util.raw_query(
        validated_body, sql, clickhouse_ro, timer, stats
    )


# Special internal endpoints that compute global aggregate data that we want to
# use internally.

@application.route('/internal/sdk-stats', methods=['POST'])
@util.time_request('sdk-stats')
def sdk_distribution(*, timer: Timer):
    body = parse_request_body(request)
    validate_request_content(body, schemas.SDK_STATS_SCHEMA, timer)

    body['project'] = []
    body['aggregations'] = [
        ['uniq', 'project_id', 'projects'],
        ['count()', None, 'count'],
    ]
    body['groupby'].extend(['sdk_name', 'rtime'])

    dataset = get_dataset('events')
    ensure_table_exists(dataset)

    result, status = parse_and_run_query(dataset, body, timer)
    return (
        json.dumps(
            result,
            for_json=True,
            default=lambda obj: obj.isoformat() if isinstance(obj, datetime) else obj),
        status,
        {'Content-Type': 'application/json'}
    )


@application.route('/subscriptions', methods=['POST'])
def create_subscription():
    return json.dumps({'subscription_id': uuid1().hex}), 202, {'Content-Type': 'application/json'}


@application.route('/subscriptions/<uuid>/renew', methods=['POST'])
def renew_subscription(uuid):
    return 'ok', 202, {'Content-Type': 'text/plain'}


@application.route('/subscriptions/<uuid>', methods=['DELETE'])
def delete_subscription(uuid):
    return 'ok', 202, {'Content-Type': 'text/plain'}


if application.debug or application.testing:
    # These should only be used for testing/debugging. Note that the database name
    # is checked to avoid scary production mishaps.

    _ensured = {}

    def ensure_table_exists(dataset, force=False):
        if not force and _ensured.get(dataset, False):
            return

        assert local_dataset_mode(), "Cannot create table in distributed mode"

        from snuba import migrate
        migrate.rename_dev_table(clickhouse_rw)

        # We cannot build distributed tables this way. So this only works in local
        # mode.
        clickhouse_rw.execute(
            dataset.get_schema().get_local_table_definition()
        )

        migrate.run(clickhouse_rw, dataset)

        _ensured[dataset] = True

    @application.route('/tests/<dataset_name>/insert', methods=['POST'])
    def write(dataset_name):
        from snuba.processor import MessageProcessor

        dataset = get_dataset(dataset_name)
        ensure_table_exists(dataset)

        rows = []
        for message in json.loads(request.data):
            action, row = dataset.get_processor().process_message(message)
            assert action is MessageProcessor.INSERT
            rows.append(row)

        dataset.get_writer().write(rows)

        return ('ok', 200, {'Content-Type': 'text/plain'})

    @application.route('/tests/<dataset_name>/eventstream', methods=['POST'])
    def eventstream(dataset_name):
        dataset = get_dataset(dataset_name)
        ensure_table_exists(dataset)
        record = json.loads(request.data)

        version = record[0]
        if version != 2:
            raise RuntimeError("Unsupported protocol version: %s" % record)

        class Message(object):
            def __init__(self, value):
                self._value = value

            def value(self):
                return self._value

            def partition(self):
                return None

            def offset(self):
                return None

        message = Message(request.data)

        type_ = record[1]
        if type_ == 'insert':
            from snuba.consumer import ConsumerWorker
            worker = ConsumerWorker(dataset, producer=None, replacements_topic=None)
        else:
            from snuba.replacer import ReplacerWorker
            worker = ReplacerWorker(clickhouse_rw, dataset)

        processed = worker.process_message(message)
        if processed is not None:
            batch = [processed]
            worker.flush_batch(batch)

        return ('ok', 200, {'Content-Type': 'text/plain'})

    @application.route('/tests/<dataset_name>/drop', methods=['POST'])
    def drop(dataset_name):
        dataset = get_dataset(dataset_name)
        table = dataset.get_schema().get_local_table_name()

        clickhouse_rw.execute("DROP TABLE IF EXISTS %s" % table)
        ensure_table_exists(dataset, force=True)
        redis_client.flushdb()
        return ('ok', 200, {'Content-Type': 'text/plain'})

    @application.route('/tests/error')
    def error():
        1 / 0

else:
    def ensure_table_exists(dataset, force=False):
        pass
