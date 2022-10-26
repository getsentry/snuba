import pytest
from structlog.testing import capture_logs

from snuba.admin.notifications.querylog.audit_log import QueryExecutionStatus, audit_log
from snuba.clickhouse.native import ClickhouseResult


def test_audit_log_success() -> None:
    with capture_logs() as cap_logs:

        @audit_log
        def successful_query(query: str, user: str) -> ClickhouseResult:
            return ClickhouseResult([])

        successful_query("test_good_query", "test_good_user")

    assert len(cap_logs) == 1
    log = cap_logs[0]
    assert log["status"] == QueryExecutionStatus.SUCCEEDED.value
    assert log["query"] == "test_good_query"
    assert log["user"] == "test_good_user"
    assert log["module"] == "snuba.admin.notifications.querylog.audit_log"


def test_audit_log_failure() -> None:
    with capture_logs() as cap_logs:

        @audit_log
        def failed_query(query: str, user: str) -> ClickhouseResult:
            raise Exception()

        with pytest.raises(Exception):
            failed_query("test_bad_query", "test_bad_user")

    assert len(cap_logs) == 1
    log = cap_logs[0]
    assert log["status"] == QueryExecutionStatus.FAILED.value
    assert log["query"] == "test_bad_query"
    assert log["user"] == "test_bad_user"
    assert log["module"] == "snuba.admin.notifications.querylog.audit_log"
