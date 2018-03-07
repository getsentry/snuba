from flask import Flask, render_template, request
import isodate
from markdown import markdown
from datetime import datetime, timedelta

import settings, util, schemas

app = Flask(__name__)

@app.route('/')
def root():
    with open('README.md') as f:
        return render_template('index.html', body=markdown(f.read()))

@app.route('/query', methods=['GET', 'POST'])
@util.validate_request(schemas.QUERY_SCHEMA)
def query():
    # TODO allow GET with params=url-encoded-json?
    body = request.validated_body

    to_date = isodate.parse_datetime(body['to_date'])
    from_date = isodate.parse_datetime(body['from_date'])
    assert from_date <= to_date

    conditions = body['conditions']
    conditions.append(('timestamp', '>=', from_date))
    conditions.append(('timestamp', '<', to_date))
    conditions.append(('project_id', '=', body['project']))

    aggregate_columns = [
        ('COUNT()', 'count')
    ]
    group_columns = [
        (util.granularity_group(body['unit']), 'time')
    ]
    if body['groupby'] == 'issue':
        group_columns.append((util.issue_expr(body['issues']), 'issue'))
    else:
        # TODO make sure its a valid column, either in the schema or here
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
    # TODO handle clickhouse failures
    return (result, 200, {'Content-Type': 'application/json'})
