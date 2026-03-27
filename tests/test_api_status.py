from datetime import datetime, timedelta
from typing import Any, Callable
from unittest.mock import MagicMock, patch

import pytest
import simplejson as json
from clickhouse_driver.errors import ErrorCodes

from snuba.clickhouse.errors import ClickhouseError
from snuba.state.cache.abstract import ExecutionTimeoutError
from snuba.state.rate_limit import (
    PROJECT_RATE_LIMIT_NAME,
    TABLE_RATE_LIMIT_NAME,
    RateLimitExceeded,
)
from tests.base import BaseApiTest
from tests.fixtures import get_raw_event


class TestApiCodes(BaseApiTest):
    def post(self) -> Any:
        return self.app.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (discover_events)
                    SELECT count() AS count BY project_id, tags[custom_tag]
                    WHERE type != 'transaction'
                    AND project_id = {self.project_id}
                    AND timestamp >= toDateTime('{self.base_time.isoformat()}')
                    AND timestamp < toDateTime('{self.next_time.isoformat()}')""",
                    "turbo": False,
                    "consistent": True,
                    "debug": True,
                    "tenant_ids": {"organization_id": 132, "referrer": "r"},
                }
            ),
            headers={"referer": "test"},
        )

    def setup_method(self, test_method: Callable[..., Any]) -> None:
        super().setup_method(test_method)
        self.event = get_raw_event()
        self.project_id = self.event["project_id"]
        self.base_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(
            minutes=180
        )
        self.next_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0) + timedelta(
            minutes=180
        )

    @patch("snuba.settings.RECORD_QUERIES", True)
    @patch("snuba.state.record_query")
    @patch("snuba.web.db_query.execute_query")
    @pytest.mark.events_db
    @pytest.mark.redis_db
    def test_correct_error_codes(self, execute_mock: MagicMock, record_query: MagicMock) -> None:
        # pytest.param doesn't play well with patch so put the list here
        tests = [
            (
                RateLimitExceeded(
                    "stuff",
                    scope=TABLE_RATE_LIMIT_NAME,
                    name="things",
                ),
                "table-rate-limited",
                "against",
            ),
            (
                RateLimitExceeded(
                    "stuff",
                    scope=PROJECT_RATE_LIMIT_NAME,
                    name="things",
                ),
                "rate-limited",
                "for",
            ),
            (
                ClickhouseError("test", code=ErrorCodes.TOO_SLOW),
                "predicted-timeout",
                "for",
            ),
            (
                ClickhouseError("test", code=ErrorCodes.TIMEOUT_EXCEEDED),
                "clickhouse-timeout",
                "against",
            ),
            (
                ClickhouseError("test", code=ErrorCodes.SOCKET_TIMEOUT),
                "network-timeout",
                "against",
            ),
            (
                ClickhouseError("test", code=ErrorCodes.ILLEGAL_TYPE_OF_ARGUMENT),
                "invalid-typing",
                "for",
            ),
            (
                ClickhouseError("test", code=ErrorCodes.MEMORY_LIMIT_EXCEEDED),
                "memory-exceeded",
                "for",
            ),
            (TimeoutError("test"), "query-timeout", "for"),
            (ExecutionTimeoutError("test"), "cache-wait-timeout", "against"),
            (
                ClickhouseError(
                    "DB::Exception: There is no supertype for types UInt32, String because some of them are String/FixedString and some of them are not: While processing has(exception_frames.colno AS `_snuba_exception_frames.colno`, '300'). Stack trace:",
                    code=ErrorCodes.NO_COMMON_TYPE,
                ),
                "invalid-typing",
                "for",
            ),
        ]

        for exception, status, slo in tests:
            execute_mock.side_effect = exception
            self.post()

            metadata = record_query.call_args[0][0]
            assert metadata["request_status"] == status, exception
            assert metadata["slo"] == slo, exception
            execute_mock.reset_mock()
            record_query.reset_mock()

    @patch("snuba.settings.RECORD_QUERIES", True)
    @patch("snuba.state.record_query")
    @patch("snuba.web.db_query.execute_query")
    @patch("snuba.web.db_query._apply_allocation_policies_quota")
    @pytest.mark.events_db
    @pytest.mark.redis_db
    def test_too_many_bytes_from_allocation_policy(
        self, apply_policies_mock: MagicMock, execute_mock: MagicMock, record_query: MagicMock
    ) -> None:
        """Test that TOO_MANY_BYTES from allocation policy is classified as RATE_LIMITED in SLO metrics"""

        # Mock allocation policy to set the flag
        def set_policy_flag(
            query_settings: Any,
            attribution_info: Any,
            formatted_query: Any,
            stats: Any,
            *args: Any,
            **kwargs: Any,
        ) -> None:
            # Set the flag in the stats dict (which is passed by reference)
            stats["max_bytes_to_read_set_by_policy"] = True
            stats["quota_allowance"] = {}

        apply_policies_mock.side_effect = set_policy_flag

        # Mock execute_query to raise TOO_MANY_BYTES error
        execute_mock.side_effect = ClickhouseError("Too many bytes", code=ErrorCodes.TOO_MANY_BYTES)

        response = self.post()

        # Verify the response has rate-limited type (because calculated_cause becomes RateLimitExceeded)
        data = json.loads(response.data)
        assert data["error"]["type"] == "rate-limited", (
            f"Expected rate-limited, got {data['error']['type']}"
        )

        # Verify SLO metrics - this is the critical part for the fix
        metadata = record_query.call_args[0][0]
        assert metadata["request_status"] == "rate-limited", (
            "TOO_MANY_BYTES from policy should be RATE_LIMITED"
        )
        assert metadata["slo"] == "for", "Rate limited requests should not count against SLO"

    @patch("snuba.settings.RECORD_QUERIES", True)
    @patch("snuba.state.record_query")
    @patch("snuba.web.db_query.execute_query")
    @pytest.mark.events_db
    @pytest.mark.redis_db
    def test_too_many_bytes_without_allocation_policy(
        self, execute_mock: MagicMock, record_query: MagicMock
    ) -> None:
        """Test that TOO_MANY_BYTES without allocation policy is classified as ERROR in SLO metrics"""
        # Mock execute_query to raise TOO_MANY_BYTES error
        # No allocation policy mock, so max_bytes_to_read_set_by_policy will not be set
        execute_mock.side_effect = ClickhouseError("Too many bytes", code=ErrorCodes.TOO_MANY_BYTES)

        response = self.post()

        # Verify the response has clickhouse error type (because calculated_cause stays as ClickhouseError)
        data = json.loads(response.data)
        assert data["error"]["type"] == "clickhouse", (
            f"Expected clickhouse, got {data['error']['type']}"
        )

        # Verify SLO metrics - this is the critical part for the fix
        metadata = record_query.call_args[0][0]
        assert metadata["request_status"] == "error", (
            "TOO_MANY_BYTES without policy should be ERROR"
        )
        assert metadata["slo"] == "against", "Error requests should count against SLO"
