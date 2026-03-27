"""Unit tests for query_metadata module"""

from typing import Any, Dict

from clickhouse_driver.errors import ErrorCodes

from snuba.clickhouse.errors import ClickhouseError
from snuba.querylog.query_metadata import (
    SLO,
    RequestStatus,
    get_request_status,
)
from snuba.state.rate_limit import RateLimitExceeded


class TestGetRequestStatus:
    """Tests for get_request_status function"""

    def test_get_request_status_success(self) -> None:
        """Test that no exception returns SUCCESS status"""
        status = get_request_status(None)
        assert status.status == RequestStatus.SUCCESS
        assert status.slo == SLO.FOR

    def test_get_request_status_rate_limit_exceeded(self) -> None:
        """Test that RateLimitExceeded returns RATE_LIMITED status"""
        error = RateLimitExceeded("Rate limit exceeded")
        status = get_request_status(error)
        assert status.status == RequestStatus.RATE_LIMITED
        assert status.slo == SLO.FOR

    def test_get_request_status_clickhouse_error_generic(self) -> None:
        """Test that generic ClickhouseError returns ERROR status"""
        error = ClickhouseError("Generic error", code=999)
        status = get_request_status(error)
        assert status.status == RequestStatus.ERROR
        assert status.slo == SLO.AGAINST

    def test_get_request_status_too_many_bytes_with_policy(self) -> None:
        """Test that TOO_MANY_BYTES with allocation policy flag returns RATE_LIMITED"""
        error = ClickhouseError("Too many bytes", code=ErrorCodes.TOO_MANY_BYTES)
        context: Dict[str, Any] = {"max_bytes_to_read_set_by_policy": True}
        status = get_request_status(error, context)
        assert status.status == RequestStatus.RATE_LIMITED
        assert status.slo == SLO.FOR

    def test_get_request_status_too_many_bytes_without_policy(self) -> None:
        """Test that TOO_MANY_BYTES without allocation policy flag returns ERROR"""
        error = ClickhouseError("Too many bytes", code=ErrorCodes.TOO_MANY_BYTES)
        context: Dict[str, Any] = {"max_bytes_to_read_set_by_policy": False}
        status = get_request_status(error, context)
        assert status.status == RequestStatus.ERROR
        assert status.slo == SLO.AGAINST

    def test_get_request_status_too_many_bytes_no_context(self) -> None:
        """Test that TOO_MANY_BYTES without context returns ERROR"""
        error = ClickhouseError("Too many bytes", code=ErrorCodes.TOO_MANY_BYTES)
        status = get_request_status(error)
        assert status.status == RequestStatus.ERROR
        assert status.slo == SLO.AGAINST

    def test_get_request_status_too_many_bytes_empty_context(self) -> None:
        """Test that TOO_MANY_BYTES with empty context returns ERROR"""
        error = ClickhouseError("Too many bytes", code=ErrorCodes.TOO_MANY_BYTES)
        context: Dict[str, Any] = {}
        status = get_request_status(error, context)
        assert status.status == RequestStatus.ERROR
        assert status.slo == SLO.AGAINST

    def test_get_request_status_memory_exceeded(self) -> None:
        """Test that MEMORY_LIMIT_EXCEEDED is mapped correctly"""
        error = ClickhouseError("Memory exceeded", code=ErrorCodes.MEMORY_LIMIT_EXCEEDED)
        status = get_request_status(error)
        assert status.status == RequestStatus.MEMORY_EXCEEDED
        assert status.slo == SLO.FOR

    def test_get_request_status_timeout(self) -> None:
        """Test that timeout errors are mapped correctly"""
        error = ClickhouseError("Timeout", code=ErrorCodes.TIMEOUT_EXCEEDED)
        status = get_request_status(error)
        assert status.status == RequestStatus.CLICKHOUSE_TIMEOUT
        assert status.slo == SLO.AGAINST
