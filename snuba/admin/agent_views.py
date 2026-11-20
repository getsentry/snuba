"""
Read-only query passthrough for automation clients such as Junior.

Clients reach these endpoints through IAP like any other admin request, as a
Google service account that the IAM policy binds to the ``AgentQueries`` role.
Queries go through the same validation as the System Queries and Querylog
tools, and never run in sudo or clusterless mode.
"""

from __future__ import annotations

from typing import Any

import structlog
from flask import Blueprint, Response, g, jsonify, make_response, request

from snuba.admin.clickhouse.common import InvalidCustomQuery, InvalidNodeError
from snuba.admin.clickhouse.nodes import get_query_node
from snuba.admin.clickhouse.querylog import run_querylog_query
from snuba.admin.clickhouse.system_queries import (
    run_system_query_on_host_with_sql,
)
from snuba.admin.tool_policies import AdminTools, check_tool_perms
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.pool import ClickhouseResult
from snuba.datasets.storages.storage_key import StorageKey

logger = structlog.get_logger().bind(module=__name__)

agent = Blueprint("agent", __name__, url_prefix="/agent")

# Keeps responses small enough for an agent to read. Clients should add LIMIT.
MAX_ROWS = 1000
# The person the client acts for. The client sets it, so it is only logged,
# never trusted for authorization.
REQUESTER_HEADER = "X-Snuba-Admin-Requester"


class _BadRequest(Exception):
    pass


def _error(message: str) -> Response:
    return make_response(jsonify({"error": message}), 400)


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [_json_value(v) for v in value]
    return str(value)


def _serialize(result: ClickhouseResult) -> dict[str, Any]:
    rows = result.results
    return {
        "column_names": [name for name, _ in result.meta or []],
        "rows": [[_json_value(col) for col in row] for row in rows[:MAX_ROWS]],
        "truncated": len(rows) > MAX_ROWS,
    }


def _run_system_query(body: dict[str, Any], sql: str) -> ClickhouseResult:
    storage = body.get("storage")
    if not isinstance(storage, str):
        raise _BadRequest("system queries need a storage, see GET /clickhouse_nodes")

    host, port = body.get("host"), body.get("port")
    if host is None:
        node = get_query_node(StorageKey(storage))
        if node is None:
            raise _BadRequest(f"Unknown storage {storage}, or it has no query node")
        host, port = node["host"], node["port"]
    if not isinstance(host, str) or not isinstance(port, int):
        raise _BadRequest("host must be a string and port an integer")

    return run_system_query_on_host_with_sql(
        host,
        port,
        storage,
        sql,
        sudo_mode=False,
        clusterless_mode=False,
        user=g.user,
    )


# curl -X POST -H 'Content-Type: application/json' \
#   -d '{"target": "system", "storage": "errors", "sql": "SELECT count() FROM system.parts"}' \
#   http://127.0.0.1:1219/agent/query
# curl -X POST -H 'Content-Type: application/json' \
#   -d '{"target": "querylog", "sql": "SELECT count() FROM querylog_local"}' \
#   http://127.0.0.1:1219/agent/query
@agent.route("/query", methods=["POST"])
@check_tool_perms(tools=[AdminTools.AGENT])
def query() -> Response:
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return _error("Expected a JSON object body")
    target, sql = body.get("target"), body.get("sql")
    if not isinstance(sql, str) or not sql.strip():
        return _error("Missing sql")

    logger.info(
        "agent_query",
        user=g.user.email,
        requester=request.headers.get(REQUESTER_HEADER),
        target=target,
        storage=body.get("storage"),
        sql=sql,
    )
    try:
        if target == "querylog":
            result = run_querylog_query(sql, g.user.email)
        elif target == "system":
            result = _run_system_query(body, sql)
        else:
            return _error("target must be 'system' or 'querylog'")
    except _BadRequest as err:
        return _error(str(err))
    except (InvalidCustomQuery, InvalidNodeError) as err:
        return _error(err.message or "Invalid query")
    except ClickhouseError as err:
        # Positive codes are ClickHouse rejecting the query; anything else is
        # an infra failure that should surface as a 500.
        if err.code > 0:
            return _error(f"ClickHouse error {err.code}: {err.message}")
        raise

    return make_response(jsonify(_serialize(result)), 200)
