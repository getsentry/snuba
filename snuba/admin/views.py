from __future__ import annotations

import logging
from typing import List, Optional, cast

import simplejson as json
from flask import Flask, Response, g, jsonify, make_response, request

from snuba import state
from snuba.admin.auth import UnauthorizedException, authorize_request
from snuba.admin.clickhouse.common import InvalidCustomQuery
from snuba.admin.clickhouse.nodes import get_storage_info
from snuba.admin.clickhouse.predefined_system_queries import SystemQuery
from snuba.admin.clickhouse.system_queries import run_system_query_on_host_with_sql
from snuba.admin.clickhouse.tracing import run_query_and_get_trace
from snuba.admin.notifications.base import RuntimeConfigAction, RuntimeConfigAutoClient
from snuba.admin.runtime_config import (
    ConfigChange,
    ConfigType,
    get_config_type_from_value,
)
from snuba.clickhouse.errors import ClickhouseError

logger = logging.getLogger(__name__)

application = Flask(__name__, static_url_path="/static", static_folder="dist")

notification_client = RuntimeConfigAutoClient()

USER_HEADER_KEY = "X-Goog-Authenticated-User-Email"


@application.errorhandler(UnauthorizedException)
def handle_invalid_json(exception: UnauthorizedException) -> Response:
    return Response(
        json.dumps({"error": "Unauthorized"}),
        401,
        {"Content-Type": "application/json"},
    )


@application.before_request
def authorize() -> None:
    user = authorize_request()
    g.user = user


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
#  -d '{"host": "localhost", "port": 9000, "sql": "select count() from system.parts;", storage: "errors"}' \
#  -H 'Content-Type: application/json' \
#  http://localhost:1219/run_clickhouse_system_query
@application.route("/run_clickhouse_system_query", methods=["POST"])
def clickhouse_system_query() -> Response:
    req = request.get_json()
    try:
        host = req["host"]
        port = req["port"]
        storage = req["storage"]
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
        return make_response(jsonify({"error": err.message or "Invalid query"}), 400)

    except ClickhouseError as err:
        logger.error(err, exc_info=True)
        return make_response(jsonify({"error": err.message or "Invalid query"}), 400)

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
        storage = req["storage"]
        raw_sql = req["sql"]
    except KeyError as e:
        return make_response(
            jsonify(
                {
                    "error": {
                        "type": "request",
                        "message": f"Invalid request, missing key {e.args[0]}",
                    }
                }
            ),
            400,
        )

    try:
        result = run_query_and_get_trace(storage, raw_sql)
        trace_output = result.trace_output
        return make_response(jsonify({"trace_output": trace_output}), 200)
    except InvalidCustomQuery as err:
        return make_response(
            jsonify(
                {
                    "error": {
                        "type": "validation",
                        "message": err.message or "Invalid query",
                    }
                }
            ),
            400,
        )
    except ClickhouseError as err:
        details = {
            "type": "clickhouse",
            "message": str(err),
            "code": err.code,
        }
        return make_response(jsonify({"error": details}), 400)
    except Exception as err:
        return make_response(
            jsonify({"error": {"type": "unknown", "message": str(err)}}), 500,
        )


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
