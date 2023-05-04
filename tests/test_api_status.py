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
        self.base_time = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(minutes=180)
        self.next_time = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0
        ) + timedelta(minutes=180)

    @patch("snuba.settings.RECORD_QUERIES", True)
    @patch("snuba.state.record_query")
    @patch("snuba.web.db_query.execute_query_with_rate_limits")
    @pytest.mark.clickhouse_db
    @pytest.mark.redis_db
    def test_correct_error_codes(
        self, execute_mock: MagicMock, record_query: MagicMock
    ) -> None:
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
            (TimeoutError("test"), "cache-set-timeout", "against"),
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
