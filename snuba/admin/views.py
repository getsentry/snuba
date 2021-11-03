from typing import Dict, Text, Tuple, Union

import simplejson as json
from flask import Flask, Response, jsonify, make_response, request
from prettytable import PrettyTable

from snuba import state
from snuba.admin.clickhouse.system_queries import SystemQuery, run_query

application = Flask(__name__, static_url_path="", static_folder="dist")


@application.route("/")
def root() -> Response:
    return application.send_static_file("index.html")


@application.route("/test-slack")
def test_slack() -> Response:
    from typing import Dict, Union

    from snuba.admin.notifications.base import rc_log_client, rc_slack_client

    data: Dict[str, Union[str, float, int]] = {
        "option": "enable_events_read_only_table",
        "old": 0,
        "new": 1,
    }

    rc_slack_client.notify(action="updated", data=data, user="meredith@sentry.io")
    rc_log_client.notify(action="updated", data=data, user="meredith@sentry.io")

    return application.send_static_file("index.html")


@application.route("/clickhouse_queries")
def clickhouse_queries() -> Response:
    res = [q.to_json() for q in SystemQuery.all_queries()]
    return make_response(jsonify(res), 200)


@application.route("/run_clickhouse_query", methods=["POST"])
def clickhouse() -> str:
    # TODO: You can do something like this to get all the hosts:
    # SELECT * FROM system.clusters
    req = request.get_json()
    print("REQUEST: ", request, req)
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


@application.route("/configs")
def config() -> Tuple[Text, int, Dict[str, str]]:
    config_data = {
        k: {"value": v, "type": get_config_type(v)}
        for (k, v) in state.get_raw_configs().items()
    }

    return (
        json.dumps(config_data),
        200,
        {"Content-Type": "application/json"},
    )


def get_config_type(value: Union[str, int, float]) -> str:
    if isinstance(value, str):
        return "string"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    raise ValueError("Unexpected config type")
