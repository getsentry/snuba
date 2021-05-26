from datetime import datetime, timedelta
from typing import Optional

import pytest

from snuba.query.exceptions import InvalidQueryException
from snuba.subscriptions.data import (
    LegacySubscriptionData,
    SnQLSubscriptionData,
    SubscriptionData,
)
from snuba.utils.metrics.timer import Timer
from snuba.web.query import parse_and_run_query
from tests.subscriptions import BaseSubscriptionTest

TESTS = [
    pytest.param(
        LegacySubscriptionData(
            project_id=1,
            conditions=[["platform", "IN", ["a"]]],
            aggregations=[["count()", "", "count"]],
            time_window=timedelta(minutes=500),
            resolution=timedelta(minutes=1),
        ),
        None,
        id="Legacy subscription",
    ),
    pytest.param(
        SnQLSubscriptionData(
            project_id=1,
            query=(
                "MATCH (events) "
                "SELECT count() AS count "
                "WHERE "
                "platform IN tuple('a') "
            ),
            time_window=timedelta(minutes=500),
            resolution=timedelta(minutes=1),
        ),
        None,
        id="SnQL subscription",
    ),
    pytest.param(
        SnQLSubscriptionData(
            project_id=1,
            query=(
                "MATCH (events) "
                "SELECT count() AS count, avg(timestamp) AS average_t "
                "WHERE "
                "platform IN tuple('a') "
            ),
            time_window=timedelta(minutes=500),
            resolution=timedelta(minutes=1),
        ),
        InvalidQueryException,
        id="SnQL subscription with 2 many aggregates",
    ),
    pytest.param(
        SnQLSubscriptionData(
            project_id=1,
            query=(
                "MATCH (events) "
                "SELECT count() AS count "
                "WHERE platform IN tuple('a') "
                "AND project_id IN tuple(1, 2) "
            ),
            time_window=timedelta(minutes=500),
            resolution=timedelta(minutes=1),
        ),
        InvalidQueryException,
        id="SnQL subscription with 2 many project ids",
    ),
    pytest.param(
        SnQLSubscriptionData(
            project_id=1,
            query=(
                "MATCH (events) "
                "SELECT count() AS count BY project_id "
                "WHERE platform IN tuple('a') "
                "AND project_id IN tuple(1) "
            ),
            time_window=timedelta(minutes=500),
            resolution=timedelta(minutes=1),
        ),
        InvalidQueryException,
        id="SnQL subscription with disallowed clause",
    ),
]


class TestBuildRequest(BaseSubscriptionTest):
    @pytest.mark.parametrize("subscription, exception", TESTS)  # type: ignore
    def test_conditions(
        self, subscription: SubscriptionData, exception: Optional[Exception]
    ) -> None:
        timer = Timer("test")
        if exception is not None:
            with pytest.raises(exception):
                request = subscription.build_request(
                    self.dataset, datetime.utcnow(), 100, timer,
                )
                result = parse_and_run_query(self.dataset, request, timer)
            return

        request = subscription.build_request(
            self.dataset, datetime.utcnow(), 100, timer,
        )
        result = parse_and_run_query(self.dataset, request, timer)
        assert result.result["data"][0]["count"] == 10
