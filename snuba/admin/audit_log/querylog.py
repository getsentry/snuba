from __future__ import annotations

from datetime import datetime
from enum import Enum
from functools import partial
from typing import Any, Callable, Mapping, MutableMapping, Union

import structlog

from snuba.clickhouse.native import ClickhouseResult

DATETIME_FORMAT = "%B %d, %Y %H:%M:%S %p"

from snuba.admin.audit_log.base import AuditLog


class QueryExecutionStatus(Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class QuerylogAuditLog(AuditLog):
    def __init__(self) -> None:
        self.logger = structlog.get_logger().bind(module=__name__)

    def _record(
        self, user: str, timestamp: str, action: str, data: Mapping[str, Any]
    ) -> None:
        query = data.get("query")
        status = data.get("status")
        self.logger.info(
            event=action,
            user=user,
            query=query,
            status=status.value if status else "unknown",
            start_timestamp=timestamp,
            end_timestamp=datetime.now().strftime(DATETIME_FORMAT),
        )

    def _notify(
        self, user: str, timestamp: str, action: str, data: Mapping[str, Any]
    ) -> None:
        pass


__querylog_audit_log_notification_client = QuerylogAuditLog()


def audit_log(
    fn: Callable[[str, str], ClickhouseResult]
) -> Callable[[str, str], ClickhouseResult]:
    """
    Decorator function for querylog query runner.

    Logs the user, query, start/end timestamps, and whether or not
    the query was successful.
    """

    def audit_log_wrapper(query: str, user: str) -> ClickhouseResult:
        data: MutableMapping[str, Union[str, QueryExecutionStatus]] = {
            "query": query,
        }
        audit_log_notify = partial(
            __querylog_audit_log_notification_client.audit,
            user=user,
            timestamp=datetime.now().strftime(DATETIME_FORMAT),
            action="run_query",
        )
        try:
            result = fn(query, user)
        except Exception:
            data["status"] = QueryExecutionStatus.FAILED
            audit_log_notify(data=data)
            raise
        data["status"] = QueryExecutionStatus.SUCCEEDED
        audit_log_notify(data=data)
        return result

    return audit_log_wrapper
