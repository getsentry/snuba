from __future__ import annotations

from datetime import datetime
from enum import Enum
from functools import partial
from typing import Callable, MutableMapping, TypeVar, Union

DATETIME_FORMAT = "%B %d, %Y %H:%M:%S %p"

from snuba.admin.audit_log.action import AuditLogAction
from snuba.admin.audit_log.base import AuditLog

Return = TypeVar("Return")


class QueryExecutionStatus(Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"


__query_audit_log_notification_client = AuditLog()


def audit_log(fn: Callable[[str, str], Return]) -> Callable[[str, str], Return]:
    """
    Decorator function for querylog query runner.

    Logs the user, query, start/end timestamps, and whether or not
    the query was successful.
    """

    def audit_log_wrapper(query: str, user: str) -> Return:
        data: MutableMapping[str, Union[str, QueryExecutionStatus]] = {
            "query": query,
        }
        audit_log_notify = partial(
            __query_audit_log_notification_client.record,
            user=user,
            action=AuditLogAction.RAN_QUERY,
        )
        try:
            result = fn(query, user)
        except Exception:
            data["status"] = QueryExecutionStatus.FAILED.value
            data["end_timestamp"] = datetime.now().strftime(DATETIME_FORMAT)
            audit_log_notify(data=data)
            raise
        data["status"] = QueryExecutionStatus.SUCCEEDED.value
        data["end_timestamp"] = datetime.now().strftime(DATETIME_FORMAT)
        audit_log_notify(data=data)
        return result

    return audit_log_wrapper
