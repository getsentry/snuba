from typing import Union

import simplejson as json
from flask import Flask, Response, jsonify, make_response, request
from prettytable import PrettyTable

from snuba import state
from snuba.admin.clickhouse.system_queries import SystemQuery, run_query

application = Flask(__name__, static_url_path="/static", static_folder="dist")


@application.route("/")
def root() -> Response:
    return application.send_static_file("index.html")


@application.route("/health")
def health() -> Response:
    return Response("OK", 200)


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


@application.route("/configs", methods=["GET", "POST"])
def configs() -> Response:
    if request.method == "POST":
        data = json.loads(request.data)
        try:
            key, value = data["key"], data["value"]

            assert isinstance(key, str), "Invalid key"
            assert isinstance(value, str), "Invalid value"

        except (KeyError, AssertionError) as exc:
            return Response(
                json.dumps({"error": f"Invalid config: {str(exc)}"}),
                400,
                {"Content-Type": "application/json"},
            )

        existing_config = state.get_uncached_config(key)
        if existing_config is not None:
            return Response(
                json.dumps({"error": f"Config with key {key} exists"}),
                400,
                {"Content-Type": "application/json"},
            )

        state.set_config(
            key, value, user=request.headers.get("X-Goog-Authenticated-User-Email"),
        )

        evaluated_value = state.get_uncached_config(key)
        assert evaluated_value is not None
        evaluated_type = get_config_type(evaluated_value)

        # Optimistically return the new config as it will take longer to refetch
        # the value
        config = {"key": key, "value": evaluated_value, "type": evaluated_type}

        return Response(json.dumps(config), 200, {"Content-Type": "application/json"})

    else:
        config_data = [
            {"key": k, "value": v, "type": get_config_type(v)}
            for (k, v) in state.get_raw_configs().items()
        ]

        return Response(
            json.dumps(config_data), 200, {"Content-Type": "application/json"},
        )


# TODO: This API means only characters that are valid in a URL can be
# used as a config key. Should we support other characters too?
@application.route("/configs/<config_key>", methods=["PUT", "DELETE"])
def config(config_key: str) -> Response:
    if request.method == "DELETE":
        state.delete_config(config_key)
        return Response("", 200)

    if request.method == "PUT":
        data = json.loads(request.data)

        try:
            key, value = data["key"], data["value"]

            # TODO: This is just a placeholder. It should do other stuff
            # like check that config actually exists and the types match

            assert isinstance(key, str), "Invalid key"
            assert isinstance(value, str), "Invalid value"

            state.set_config(
                key, value, user=request.headers.get("X-Goog-Authenticated-User-Email"),
            )

        except (KeyError, AssertionError) as exc:
            return Response(
                json.dumps({"error": f"Invalid config: {str(exc)}"}),
                400,
                {"Content-Type": "application/json"},
            )

        return Response(json.dumps({}), 200, {"Content-Type": "application/json"})

    raise


def get_config_type(value: Union[str, int, float]) -> str:
    if isinstance(value, str):
        return "string"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    raise ValueError("Unexpected config type")


@application.route("/config_auditlog")
def config_changes() -> Response:
    return Response(
        json.dumps(state.get_config_changes()),
        200,
        {"Content-Type": "application/json"},
    )
