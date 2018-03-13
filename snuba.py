from flask import Flask, render_template, request
from dateutil.parser import parse as parse_datetime
import json
from markdown import markdown
from datetime import datetime, timedelta
from raven.contrib.flask import Sentry

import settings, util, schemas

app = Flask(__name__)
sentry = Sentry(app, dsn=settings.SENTRY_DSN)

@app.route('/')
def root():
    with open('README.md') as f:
        return render_template('index.html', body=markdown(f.read()))

@app.route('/query', methods=['GET', 'POST'])
@util.validate_request(schemas.QUERY_SCHEMA)
def query():
    body = request.validated_body

    to_date = parse_datetime(body['to_date'])
    from_date = parse_datetime(body['from_date'])
    assert from_date <= to_date

    conditions = body['conditions']
    conditions.append(('timestamp', '>=', from_date))
    conditions.append(('timestamp', '<', to_date))
    if isinstance(body['project'], list):
        conditions.append(('project_id', 'IN', body['project']))
    else:
        conditions.append(('project_id', '=', body['project']))

    aggregate_columns = [
        ('{}({})'.format(body['aggregation'], body['aggregateby']), settings.AGGREGATE_RESULT_COLUMN)
    ]
    group_columns = [
        (settings.TIME_GROUPS.get(body['granularity'], settings.DEFAULT_TIME_GROUP), settings.TIME_GROUP_COLUMN)
    ]
    if body['groupby'] == 'issue':
        group_columns.append((util.issue_expr(body['issues']), 'issue'))
    else:
        group_columns.append((body['groupby'], body['groupby']))

    select_columns = group_columns + aggregate_columns

    select_clause = ', '.join('{} AS {}'.format(defn, alias) for (defn, alias) in select_columns)
    select_clause = 'SELECT {}'.format(select_clause)

    from_clause = 'FROM {}'.format(settings.CLICKHOUSE_TABLE)

    where_clause = ' AND '.join('{} {} {}'.format(col, op, util.escape_literal(lit)) for (col, op, lit) in conditions)
    if where_clause:
        where_clause = 'WHERE {}'.format(where_clause)

    group_clause = ', '.join(alias for (_, alias) in group_columns)
    if group_clause:
        group_clause = 'GROUP BY ({})'.format(group_clause)

    sql = '{} {} {} {}'.format(select_clause, from_clause, where_clause, group_clause)

    print sql
    result = util.raw_query(sql)
    return (json.dumps(result), 200, {'Content-Type': 'application/json'})
