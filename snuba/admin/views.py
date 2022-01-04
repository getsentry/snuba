from typing import Any, List, Mapping, MutableMapping, Optional, cast

import simplejson as json
from flask import Flask, Response, jsonify, make_response, request

from snuba import state
from snuba.admin.clickhouse.common import (
    InvalidCustomQuery,
    InvalidNodeError,
    InvalidStorageError,
)
from snuba.admin.clickhouse.nodes import get_storage_info
from snuba.admin.clickhouse.system_queries import (
    InvalidResultError,
    NonExistentSystemQuery,
    SystemQuery,
    run_system_query_on_host_by_name,
    run_system_query_on_host_with_sql,
)
from snuba.admin.clickhouse.tracing import run_query_and_get_trace
from snuba.admin.notifications.base import RuntimeConfigAction, RuntimeConfigAutoClient
from snuba.admin.runtime_config import (
    ConfigChange,
    ConfigType,
    get_config_type_from_value,
)
from snuba.clickhouse.errors import ClickhouseError
from snuba.web import QueryException

application = Flask(__name__, static_url_path="/static", static_folder="dist")

notification_client = RuntimeConfigAutoClient()

USER_HEADER_KEY = "X-Goog-Authenticated-User-Email"


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
        host = req["host"]
        port = req["port"]
        storage = req["storage"]
    except KeyError:
        return make_response(jsonify({"error": "Invalid request"}), 400)

    is_predefined_query = req.get("query_name") is not None

    if is_predefined_query:
        try:
            result = run_system_query_on_host_by_name(
                host, port, storage, req.get("query_name"),
            )
            rows: List[List[str]] = []
            rows, columns = cast(List[List[str]], result.results), result.meta

            if columns:
                res: MutableMapping[str, Any] = {}
                res["column_names"] = [name for name, _ in columns]
                res["rows"] = [[str(col) for col in row] for row in rows]

                return make_response(jsonify(res), 200)
            else:
                raise InvalidResultError
        except (
            InvalidNodeError,
            NonExistentSystemQuery,
            InvalidStorageError,
            InvalidResultError,
        ) as err:
            return make_response(
                jsonify({"error": err.__class__.__name__, "data": err.extra_data}), 400
            )

    else:
        try:
            raw_sql = req["sql"]
        except KeyError:
            return make_response(jsonify({"error": "Invalid request"}), 400)

        try:
            result = run_system_query_on_host_with_sql(host, port, storage, raw_sql)
            rows = []
            rows, columns = cast(List[List[str]], result.results), result.meta

            if columns:
                res = {}
                res["column_names"] = [name for name, _ in columns]
                res["rows"] = [[str(col) for col in row] for row in rows]

                return make_response(jsonify(res), 200)
        except InvalidCustomQuery as err:
            return make_response(
                jsonify({"error": err.message or "Invalid query"}), 400
            )

    # We should never get here
    return make_response(jsonify({"error": "Something went wrong"}), 400)


# Sample cURL command:
#
# curl -X POST \
#  -H 'Content-Type: application/json' \
#  http://localhost:1219/clickhouse_trace_query?query=SELECT+count()+FROM+errors_local
@application.route("/clickhouse_trace_query", methods=["POST"])
def clickhouse_trace_query() -> Response:
    req = json.loads(request.data)
    try:
        host = req["host"]
        port = req["port"]
        storage = req["storage"]
        raw_sql = req["sql"]
    except KeyError as e:
        return make_response(
            jsonify({"error": f"Invalid request, missing key {e.args[0]}"}), 400
        )

    try:
        result = run_query_and_get_trace(host, port, storage, raw_sql)
        trace_output = result.trace_output
        return make_response(jsonify({"trace_output": trace_output}), 200)
    except InvalidCustomQuery as err:
        return make_response(jsonify({"error": err.message or "Invalid query"}), 400)
    except QueryException as err:
        status = 500
        details: Mapping[str, Any]

        cause = err.__cause__
        if isinstance(cause, ClickhouseError):
            details = {
                "type": "clickhouse",
                "message": str(cause),
                "code": cause.code,
            }
        if isinstance(cause, Exception):
            details = {
                "type": "unknown",
                "message": str(cause),
            }
        else:
            raise  # exception should have been chained

        return make_response(jsonify({"error": details, **err.extra}), status)


@application.route("/configs", methods=["GET", "POST"])
def configs() -> Response:
    if request.method == "POST":
        data = json.loads(request.data)
        try:
            key, value = data["key"], data["value"]

            assert isinstance(key, str), "Invalid key"
            assert isinstance(value, str), "Invalid value"
            assert key != "", "Key cannot be empty string"

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

        user = request.headers.get(USER_HEADER_KEY)

        state.set_config(
            key, value, user=user,
        )

        evaluated_value = state.get_uncached_config(key)
        assert evaluated_value is not None
        evaluated_type = get_config_type_from_value(evaluated_value)

        config = {"key": key, "value": str(evaluated_value), "type": evaluated_type}

        notification_client.notify(
            RuntimeConfigAction.ADDED,
            {"option": key, "old": None, "new": evaluated_value},
            user,
        )

        return Response(json.dumps(config), 200, {"Content-Type": "application/json"})

    else:

        config_data = [
            {
                "key": k,
                "value": str(v) if v is not None else None,
                "type": get_config_type_from_value(v),
            }
            for (k, v) in state.get_raw_configs().items()
        ]

        return Response(
            json.dumps(config_data), 200, {"Content-Type": "application/json"},
        )


@application.route("/configs/<config_key>", methods=["PUT", "DELETE"])
def config(config_key: str) -> Response:
    if request.method == "DELETE":
        user = request.headers.get(USER_HEADER_KEY)

        # Get the old value for notifications
        old = state.get_uncached_config(config_key)

        state.delete_config(config_key, user=user)

        notification_client.notify(
            RuntimeConfigAction.REMOVED,
            {"option": config_key, "old": old, "new": None},
            user,
        )

        return Response("", 200)

    else:
        # PUT currently only supports editing existing config when old and
        # new types match. Does not currently support passing force to
        # set_config to override the type check.

        user = request.headers.get(USER_HEADER_KEY)
        data = json.loads(request.data)

        # Get the previous value for notifications
        old = state.get_uncached_config(config_key)

        try:
            new_value = data["value"]

            assert isinstance(config_key, str), "Invalid key"
            assert isinstance(new_value, str), "Invalid value"
            assert config_key != "", "Key cannot be empty string"

            state.set_config(
                config_key, new_value, user=user,
            )

        except (KeyError, AssertionError) as exc:
            return Response(
                json.dumps({"error": f"Invalid config: {str(exc)}"}),
                400,
                {"Content-Type": "application/json"},
            )
        except (state.MismatchedTypeException):
            return Response(
                json.dumps({"error": "Mismatched type"}),
                400,
                {"Content-Type": "application/json"},
            )

        # Value was updated successfully, refetch and return it
        evaluated_value = state.get_uncached_config(config_key)
        assert evaluated_value is not None
        evaluated_type = get_config_type_from_value(evaluated_value)

        # Send notification
        notification_client.notify(
            RuntimeConfigAction.UPDATED,
            {"option": config_key, "old": old, "new": evaluated_value},
            user=user,
        )

        config = {
            "key": config_key,
            "value": str(evaluated_value),
            "type": evaluated_type,
        }

        return Response(json.dumps(config), 200, {"Content-Type": "application/json"})


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


@application.route("/clickhouse_nodes")
def clickhouse_nodes() -> Response:
    return Response(
        json.dumps(get_storage_info()), 200, {"Content-Type": "application/json"}
    )
