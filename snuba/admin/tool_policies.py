from __future__ import annotations

from enum import Enum
from functools import wraps
from typing import Any, Callable

from flask import Response, g, jsonify, make_response

from snuba.admin.auth_roles import InteractToolAction
from snuba.admin.user import AdminUser


class AdminTools(Enum):
    """
    These correspond to the different tabs in the Admin tool. Specifically, the
    id of the nav items in the front end.
    Every endpoint should be annotated with one of these tools to ensure correct permissions.
    """

    ALL = "all"
    CONFIGURATION = "configuration"
    SNQL_TO_SQL = "snql-to-sql"
    SYSTEM_QUERIES = "system-queries"
    MIGRATIONS = "migrations"
    QUERY_TRACING = "tracing"
    QUERYLOG = "querylog"
    AUDIT_LOG = "audit-log"
    KAFKA = "kafka"
    CAPACITY_MANAGEMENT = "capacity-management"
    PRODUCTION_QUERIES = "production-queries"
    CARDINALITY_ANALYZER = "cardinality-analyzer"
    SNUBA_EXPLAIN = "snuba_explain"


DEVELOPER_TOOLS: set[AdminTools] = {AdminTools.SNQL_TO_SQL, AdminTools.QUERY_TRACING}


def get_user_allowed_tools(user: AdminUser) -> set[AdminTools]:
    user_allowed_tools = set()
    for role in user.roles:
        for action in role.actions:
            if not isinstance(action, InteractToolAction):
                continue

            for resource in action._resources:
                try:
                    tool = AdminTools(resource.name)
                    user_allowed_tools.add(AdminTools(tool))
                except Exception:
                    pass

    return user_allowed_tools


def check_tool_perms(
    tools: list[AdminTools],
) -> Callable[[Callable[..., Response]], Callable[..., Response]]:
    """
    A wrapper decorator that should be applied to all endpoints. A user must have access to all tools
    or one of the specified tools.
    """
    error_message = f"No permissions on {', '.join(t.value for t in tools)}"

    # This extra decorator is necessary, otherwise the inner function will get called
    # without a Flask application context and fail
    def decorator(f: Callable[..., Response]) -> Callable[..., Response]:
        @wraps(f)
        def check_perms(*args: Any, **kwargs: Any) -> Response:
            allowed_tools = get_user_allowed_tools(g.user)
            if AdminTools.ALL in allowed_tools:
                return f(*args, **kwargs)

            for tool in tools:
                if tool in allowed_tools:
                    return f(*args, **kwargs)

            return make_response(jsonify({"error": error_message}), 403)

        return check_perms

    return decorator
