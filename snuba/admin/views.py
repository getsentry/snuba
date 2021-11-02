from flask import Flask, Response, jsonify, make_response, request
from prettytable import PrettyTable

from snuba.admin.clickhouse.system_queries import SystemQuery, run_query

application = Flask(__name__, static_url_path="", static_folder="dist")


@application.route("/")
def root() -> Response:
    return application.send_static_file("index.html")


@application.route("/clickhouse_queries")
def clickhouse_queries() -> Response:
    res = [q.to_json() for q in SystemQuery.all_queries()]
    return make_response(jsonify(res), 200)


@application.route("/clickhouse", methods=["POST"])
def clickhouse() -> str:
    # TODO: You can do something like this to get all the hosts:
    # SELECT * FROM system.clusters
    req = request.get_json()
    results, columns = run_query(
        req.get("host", "localhost"),
        req.get("storage", "transactions"),
        req.get("query_name"),
    )

    res = PrettyTable()
    res.field_names = [name for name, _ in columns]
    for row in results:
        res.add_row(row)
    return f"<pre><code>{str(res)}</code></pre>"
