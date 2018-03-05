from flask import Flask, render_template, request
from markdown import markdown
from datetime import datetime, timedelta

import settings, util

app = Flask(__name__)

@app.route('/')
def root():
    with open('README.md') as f:
        return render_template('index.html', body=markdown(f.read()))

@app.route('/query')
def query():
    # TODO this should probably be a JSON request instead of get params
    assert 'project' in request.args
    project = int(request.args.get('project'))

    to_date = request.args.get('to_date', datetime.utcnow())
    from_date = request.args.get('from_date', to_date - timedelta(days=1))
    assert from_date <= to_date

    conditions = request.args.getlist('where')
    conditions.append('timestamp >= {}'.format(util.escape_literal(from_date)))
    conditions.append('timestamp < {}'.format(util.escape_literal(to_date)))
    conditions.append('project_id == {}'.format(util.escape_literal(project)))
    where_clause = 'WHERE {}'.format(' AND '.join(conditions))

    unit = util.granularity_group(request.args.get('unit', 'hour'))

    sql = 'SELECT {} as time, COUNT() as count FROM {} {} GROUP BY time'.format(
        unit,
        settings.CLICKHOUSE_TABLE,
        where_clause,
    )

    result = util.raw_query(sql)
    return (result, 200, {'Content-Type': 'application/json'})
