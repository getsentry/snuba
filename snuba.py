from flask import Flask, render_template
from markdown import markdown
import requests

import settings

app = Flask(__name__)

@app.route('/')
def root():
    with open('README.md') as f:
        return render_template('index.html', body=markdown(f.read()))

@app.route('/query')
def query():
    sql = 'SELECT COUNT(*) FROM {0}'.format(settings.CLICKHOUSE_TABLE)
    result = requests.get(
        settings.CLICKHOUSE_SERVER,
        params={'query': sql},
    ).text

    return (result, 200, {'Content-Type': 'application/json'})
