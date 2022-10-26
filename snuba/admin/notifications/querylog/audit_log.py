from __future__ import annotations

from datetime import datetime
from enum import Enum
from functools import partial
from typing import Callable

import structlog

from snuba.clickhouse.native import ClickhouseResult

DATETIME_FORMAT = "%B %d, %Y %H:%M:%S %p"


class QueryExecutionStatus(Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class QuerylogAuditLogClient:
    def __init__(self) -> None:
        self.logger = structlog.get_logger().bind(module=__name__)

    def notify(
        self, user: str, query: str, start_timestamp: str, status: QueryExecutionStatus
    ) -> None:
        self.logger.info(
            event="run_query",
            user=user,
            query=query,
            status=status.value,
            start_timestamp=start_timestamp,
            end_timestamp=datetime.now().strftime(DATETIME_FORMAT),
        )


querylog_audit_log_notification_client = QuerylogAuditLogClient()


def audit_log(
    fn: Callable[[str, str], ClickhouseResult]
) -> Callable[[str, str], ClickhouseResult]:
    """
    Decorator function for querylog query runner.

    Logs the user, query, start/end timestamps, and whether or not
    the query was successful.
    """

    def audit_log_wrapper(query: str, user: str) -> ClickhouseResult:
        audit_log_notify = partial(
            querylog_audit_log_notification_client.notify,
            user=user,
            query=query,
            start_timestamp=datetime.now().strftime(DATETIME_FORMAT),
        )
        try:
            result = fn(query, user)
        except Exception:
            audit_log_notify(status=QueryExecutionStatus.FAILED)
            raise
        audit_log_notify(status=QueryExecutionStatus.SUCCEEDED)
        return result

    return audit_log_wrapper
