import simplejson as json
import os
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_datetime

from flask import Flask, render_template, request
from clickhouse_driver import Client
from markdown import markdown
from raven.contrib.flask import Sentry

from snuba import util, schemas


app = Flask(__name__)
if 'SNUBA_SETTINGS' not in os.environ:
    os.environ['SNUBA_SETTINGS'] = 'settings.py'
app.config.from_envvar('SNUBA_SETTINGS')

clickhouse = Client(
    host=app.config['CLICKHOUSE_SERVER'].split(':')[0],
    port=int(app.config['CLICKHOUSE_SERVER'].split(':')[1]),
    connect_timeout=1,
)

sentry = Sentry(app, dsn=app.config['SENTRY_DSN'])


@app.route('/')
def root():
    with open('README.md') as f:
        return render_template('index.html', body=markdown(f.read()))

# TODO if `issue` or `time` is specified in 2 places (eg group and where),
# we redundantly expand it twice
# TODO some aliases eg tags[foo] are invalid SQL
# and need escaping or otherwise sanitizing


@app.route('/query', methods=['GET', 'POST'])
@util.validate_request(schemas.QUERY_SCHEMA)
def query():
    body = request.validated_body

    to_date = parse_datetime(body['to_date'])
    from_date = parse_datetime(body['from_date'])
    assert from_date <= to_date

    conditions = body['conditions']
    conditions.extend([
        ('timestamp', '>=', from_date),
        ('timestamp', '<', to_date),
        ('project_id', 'IN', util.to_list(body['project'])),
    ])

    aggregate_columns = [
        util.column_expr(body['aggregateby'], body, app.config['AGGREGATE_COLUMN'])
    ]
    groupby = util.to_list(body['groupby'])
    group_columns = [util.column_expr(gb, body) for gb in groupby]

    select_columns = group_columns + aggregate_columns
    select_predicates = (
        '{} AS `{}`'.format(exp, alias) if exp != alias else exp
        for (exp, alias) in select_columns
    )
    select_clause = 'SELECT {}'.format(', '.join(select_predicates))
    from_clause = 'FROM {}'.format(app.config['CLICKHOUSE_TABLE'])
    join_clause = 'ARRAY JOIN {}'.format(body['arrayjoin']) if 'arrayjoin' in body else ''

    conditions = [
        (util.column_expr(col, body), op, tuple(lit) if isinstance(lit, list) else lit)
        for col, op, lit in conditions
    ]
    # If a where clause references a column already expanded in another clause
    # then we can just use the alias.
    where_predicates = (
        '{} {} {}'.format(
            alias if (col, alias) in select_columns else col,
            op,
            util.escape_literal(lit))
        for ((col, alias), op, lit) in set(conditions)
    )
    where_clause = 'WHERE {}'.format(' AND '.join(where_predicates)) if conditions else ''

    group_clause = ', '.join(alias for (_, alias) in group_columns)
    if group_clause:
        group_clause = 'GROUP BY ({})'.format(group_clause)

    order_clause = 'ORDER BY time' if 'time' in groupby else ''

    sql = ' '.join([c for c in [
        select_clause,
        from_clause,
        join_clause,
        where_clause,
        group_clause,
        order_clause,
    ] if c])

    result = util.raw_query(sql, clickhouse)
    return (json.dumps(result), 200, {'Content-Type': 'application/json'})


if app.debug or app.testing:
    # These should only be used for testing/debugging. Note that the database name
    # is hardcoded to 'test' on purpose to avoid scary production mishaps.
    TEST_TABLE = 'test'

    @app.route('/tests/insert', methods=['POST'])
    def write():
        from snuba.processor import process_raw_event
        from snuba.writer import row_from_processed_event, write_rows

        clickhouse.execute(
            util.get_table_definition(
                TEST_TABLE,
                'Memory',
                app.config['SCHEMA_COLUMNS']))

        body = json.loads(request.data)

        rows = []
        for event in body:
            processed = process_raw_event(event)
            row = row_from_processed_event(processed)
            rows.append(row)

        write_rows(clickhouse, table=TEST_TABLE, columns=app.config['WRITER_COLUMNS'], rows=rows)
        return ('ok', 200, {'Content-Type': 'text/plain'})

    @app.route('/tests/drop', methods=['POST'])
    def drop():
        clickhouse.execute("DROP TABLE IF EXISTS %s" % TEST_TABLE)
        return ('ok', 200, {'Content-Type': 'text/plain'})
