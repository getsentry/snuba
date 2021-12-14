from typing import Optional

import simplejson as json
from flask import Flask, Response, jsonify, make_response, request

from snuba import state
from snuba.admin.clickhouse.system_queries import (
    InvalidNodeError,
    InvalidStorageError,
    NonExistentSystemQuery,
    SystemQuery,
    run_system_query_on_host_by_name,
)
from snuba.admin.runtime_config import (
    ConfigChange,
    ConfigType,
    get_config_type_from_value,
)

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


# Sample cURL command:
#
# curl -X POST \
#  -d '{"query_name": "ActivePartitions"}' \
#  -H 'Content-Type: application/json' \
#  http://localhost:1219/run_clickhouse_system_query
@application.route("/run_clickhouse_system_query", methods=["POST"])
def clickhouse_system_query() -> Response:
    req = request.get_json()
    try:
        results, columns = run_system_query_on_host_by_name(
            req.get("host", "localhost"),
            req.get("port", 9000),
            req.get("storage", "transactions"),
            req.get("query_name"),
        )
        res = {}
        res["column_names"] = [name for name, _ in columns]
        res["rows"] = []
        for row in results:
            res["rows"].append([str(col) for col in row])

        return make_response(jsonify(res), 200)
    except (InvalidNodeError, NonExistentSystemQuery, InvalidStorageError) as err:
        return make_response(
            jsonify({"error": err.__class__.__name__, "data": err.extra_data}), 400
        )


@application.route("/configs", methods=["GET"])
def configs() -> Response:
    config_data = [
        {
            "key": k,
            "value": str(v) if v is not None else None,
            "type": get_config_type_from_value(v),
        }
        for (k, v) in state.get_raw_configs().items()
    ]

    return Response(json.dumps(config_data), 200, {"Content-Type": "application/json"},)


@application.route("/config_auditlog")
def config_changes() -> Response:
    def serialize(
        key: str,
        ts: float,
        user: Optional[str],
        before: Optional[ConfigType],
        after: Optional[ConfigType],
    ) -> ConfigChange:
        return {
            "key": key,
            "timestamp": ts,
            "user": user,
            "before": str(before) if before is not None else None,
            "beforeType": get_config_type_from_value(before),
            "after": str(after) if after is not None else None,
            "afterType": get_config_type_from_value(after),
        }

    data = [
        serialize(key, ts, user, before, after)
        for [key, ts, user, before, after] in state.get_config_changes()
    ]

    return Response(json.dumps(data), 200, {"Content-Type": "application/json"})
