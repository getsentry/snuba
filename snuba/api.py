from flask import Flask, render_template, request

from clickhouse_driver import Client
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_datetime
import json
from markdown import markdown
from raven.contrib.flask import Sentry

import settings
import util
import schemas

app = Flask(__name__)
clickhouse = Client(
    settings.CLICKHOUSE_SERVER,
    port=settings.CLICKHOUSE_PORT,
    connect_timeout=1
)
sentry = Sentry(app, dsn=settings.SENTRY_DSN)


@app.route('/')
def root():
    with open('README.md') as f:
        return render_template('index.html', body=markdown(f.read()))

# TODO if there is a condition on `issue =` or `issue IN` we can prune
# issue_expr to only search for issues that would pass the filter
# TODO if `issue` or `time` is specified in 2 places (eg group and where),
# we redundantly expand it twice


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

    aggregate_columns = [(
        '{}({})'.format(body['aggregation'], util.column_expr(body['aggregateby'], body)),
        settings.AGGREGATE_RESULT_COLUMN
    )]
    groupby = util.to_list(body['groupby'])
    group_columns = [(util.column_expr(gb, body), gb) for gb in groupby]

    select_columns = group_columns + aggregate_columns
    select_clause = ', '.join('{} AS {}'.format(defn, alias) for (defn, alias) in select_columns)
    select_clause = 'SELECT {}'.format(select_clause)

    from_clause = 'FROM {}'.format(settings.CLICKHOUSE_TABLE)

    where_predicates = (
        '{} {} {}'.format(util.column_expr(col, body), op, util.escape_literal(lit))
        for (col, op, lit) in conditions
    )
    where_clause = 'WHERE {}'.format(' AND '.join(where_predicates)) if conditions else ''

    group_clause = ', '.join(alias for (_, alias) in group_columns)
    if group_clause:
        group_clause = 'GROUP BY ({})'.format(group_clause)

    order_clause = 'ORDER BY time' if 'time' in groupby else ''

    sql = '{} {} {} {} {}'.format(
        select_clause,
        from_clause,
        where_clause,
        group_clause,
        order_clause)

    result = util.raw_query(sql, clickhouse)
    return (json.dumps(result), 200, {'Content-Type': 'application/json'})
