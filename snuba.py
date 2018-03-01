from flask import Flask, render_template
from markdown import markdown
app = Flask(__name__)

@app.route('/')
def root():
    with open('README.md') as f:
        return render_template('index.html', body=markdown(f.read()))

@app.route('/query')
def query():
    return "Query result"
