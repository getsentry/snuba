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
    where_clause = 'WHERE {}'.format(' AND '.join(conditions))

    unit = util.granularity_group(body['unit'])

    sql = 'SELECT {} as time, COUNT() as count FROM {} {} GROUP BY time'.format(
        unit,
        settings.CLICKHOUSE_TABLE,
        where_clause,
    )

    result = util.raw_query(sql)
    return (result, 200, {'Content-Type': 'application/json'})
