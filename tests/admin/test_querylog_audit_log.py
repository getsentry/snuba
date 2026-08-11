import importlib

import pytest
from structlog.testing import capture_logs

import snuba.admin.audit_log.query
from snuba.admin.audit_log.query import QueryExecutionStatus, audit_log
from snuba.clickhouse.native import ClickhouseResult


def test_audit_log_success() -> None:
    with capture_logs() as cap_logs:
        importlib.reload(snuba.admin.audit_log.query)

        @audit_log
        def successful_query(query: str, user: str) -> ClickhouseResult:
            return ClickhouseResult([])

        successful_query("test_good_query", "test_good_user")

    assert len(cap_logs) == 1
    log = cap_logs[0]
    assert log["status"] == QueryExecutionStatus.SUCCEEDED.value
    assert log["query"] == "test_good_query"
    assert log["user"] == "test_good_user"
    assert "timestamp" in log
    assert "end_timestamp" in log


def test_audit_log_records_bound_params() -> None:
    """Bound values never reach the query text, so the record must carry them.

    Without this the audit trail shows `referrer = %(referrer)s` and not what
    the operator actually filtered on.
    """
    with capture_logs() as cap_logs:

        @audit_log
        def query_with_params(
            query: str, user: str, params: dict[str, str] | None = None
        ) -> ClickhouseResult:
            return ClickhouseResult([])

        query_with_params(
            "SELECT * FROM querylog_dist WHERE referrer = %(referrer)s",
            "test_user",
            params={"referrer": "api.organization-events"},
        )

    log = cap_logs[0]
    assert "api.organization-events" in log["params"]


def test_audit_log_omits_params_when_absent() -> None:
    with capture_logs() as cap_logs:

        @audit_log
        def query_without_params(query: str, user: str) -> ClickhouseResult:
            return ClickhouseResult([])

        query_without_params("SELECT 1", "test_user")

    assert "params" not in cap_logs[0]


def test_audit_log_failure() -> None:
    with capture_logs() as cap_logs:

        @audit_log
        def failed_query(query: str, user: str) -> ClickhouseResult:
            raise Exception()

        with pytest.raises(Exception):  # noqa: B017 decorated fn raises bare Exception; verifying audit_log re-raises it
            failed_query("test_bad_query", "test_bad_user")

    assert len(cap_logs) == 1
    log = cap_logs[0]
    assert log["status"] == QueryExecutionStatus.FAILED.value
    assert log["query"] == "test_bad_query"
    assert log["user"] == "test_bad_user"
    assert "timestamp" in log
    assert "end_timestamp" in log
