import simplejson as json
from flask import Flask, Response, jsonify, make_response, request

from snuba import state
from snuba.admin.clickhouse.system_queries import SystemQuery, run_system_query
from snuba.admin.runtime_config import get_config_type_from_value

application = Flask(__name__, static_url_path="/static", static_folder="dist")


@application.route("/")
def root() -> Response:
    return application.send_static_file("index.html")


@application.route("/health")
def health() -> Response:
    return Response("OK", 200)


@application.route("/clickhouse_queries")
def clickhouse_queries() -> Response:
    res = [q.to_json() for q in SystemQuery.all_queries()]
    return make_response(jsonify(res), 200)


@application.route("/run_clickhouse_system_query", methods=["POST"])
def clickhouse_system_query() -> Response:
    # TODO: You can do something like this to get all the hosts:
    # SELECT * FROM system.clusters
    req = request.get_json()
    results, columns = run_system_query(
        req.get("host", "localhost"),
        req.get("storage", "transactions"),
        req.get("query_name"),
    )
    res = {}
    res["column_names"] = [name for name, _ in columns]
    res["rows"] = []
    for row in results:
        res["rows"].append([str(col) for col in row])

    return make_response(jsonify(res), 200)


@application.route("/configs", methods=["GET"])
def configs() -> Response:
    config_data = [
        {"key": k, "value": v, "type": get_config_type_from_value(v)}
        for (k, v) in state.get_raw_configs().items()
    ]

    return Response(json.dumps(config_data), 200, {"Content-Type": "application/json"},)
