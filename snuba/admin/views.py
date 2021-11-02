from flask import Flask, Response

from snuba.admin.clickhouse.system_queries import run_query

application = Flask(__name__, static_url_path="", static_folder="dist")


@application.route("/")
def root() -> Response:
    return application.send_static_file("index.html")


@application.route("/clickhouse")
def clickhouse() -> str:
    sql = """
    SELECT
      count(),
      is_currently_executing
    FROM system.replication_queue
    GROUP BY is_currently_executing
    """
    sql = """
    SELECT
        active,
        count()
    FROM system.parts
    GROUP BY active
    """
    results, columns = run_query("localhost", "transactions", sql)
    from prettytable import PrettyTable

    res = PrettyTable()
    res.field_names = [name for name, _ in columns]
    for row in results:
        res.add_row(row)
    return f"<pre><code>{str(res)}</code></pre>"
