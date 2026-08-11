from __future__ import annotations

from collections.abc import Callable, MutableMapping
from datetime import UTC, datetime
from enum import Enum
from functools import partial, wraps
from typing import Any, TypeVar

from snuba.admin.audit_log.action import AuditLogAction
from snuba.admin.audit_log.base import AuditLog

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

Return = TypeVar("Return")


class QueryExecutionStatus(Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"


__query_audit_log_notification_client = AuditLog()


def audit_log(fn: Callable[..., Return]) -> Callable[..., Return]:
    """
    Decorator function for querylog query runner.

    Logs the user, query, start/end timestamps, and whether or not
    the query was successful.

    Expects the wrapped function to take ``query`` and ``user`` as the first
    two positional arguments. Additional args/kwargs are forwarded unchanged.

    A ``params`` keyword is recorded alongside the query. Values bound as driver
    parameters never appear in the query text, so without this the audit record
    would show `referrer = %(referrer)s` and not what was actually filtered on.
    """

    @wraps(fn)
    def audit_log_wrapper(query: str, user: str, *args: Any, **kwargs: Any) -> Return:
        data: MutableMapping[str, str | int] = {
            "query": query,
        }
        params = kwargs.get("params")
        if params:
            data["params"] = repr(params)
        audit_log_notify = partial(
            __query_audit_log_notification_client.record,
            user=user,
            action=AuditLogAction.RAN_QUERY,
        )
        try:
            result = fn(query, user, *args, **kwargs)
        except Exception:
            data["status"] = QueryExecutionStatus.FAILED.value
            data["end_timestamp"] = datetime.now(UTC).strftime(DATETIME_FORMAT)
            audit_log_notify(data=data)
            raise
        data["status"] = QueryExecutionStatus.SUCCEEDED.value
        data["end_timestamp"] = datetime.now(UTC).strftime(DATETIME_FORMAT)
        audit_log_notify(data=data)
        return result

    return audit_log_wrapper
