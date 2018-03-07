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
    conditions.append('timestamp >= {}'.format(util.escape_literal(from_date)))
    conditions.append('timestamp < {}'.format(util.escape_literal(to_date)))
    #conditions.append('project_id == {}'.format(util.escape_literal(project)))

    aggregate_columns = [
        ('COUNT()', 'count')
    ]
    group_columns = [
        (util.issue_expr(body['issues']), 'issue'),
        (util.granularity_group(body['unit']), 'time')
    ]
    select_columns = group_columns + aggregate_columns

    select_clause = ', '.join('{} AS {}'.format(defn, alias) for (defn, alias) in select_columns)
    where_clause = 'WHERE {}'.format(' AND '.join(conditions))
    group_clause = 'GROUP BY ({})'.format(', '.join(alias for (_, alias) in group_columns))

    sql = 'SELECT {} FROM {} {} {}'.format(
        select_clause,
        settings.CLICKHOUSE_TABLE,
        where_clause,
        group_clause
    )
    print sql
    result = util.raw_query(sql)
    return (result, 200, {'Content-Type': 'application/json'})
