from datetime import datetime, timedelta
from typing import Generator, Optional

import pytest

from snuba import state
from snuba.datasets.factory import get_dataset
from snuba.query.exceptions import InvalidQueryException
from snuba.subscriptions.data import (
    DelegateSubscriptionData,
    LegacySubscriptionData,
    SnQLSubscriptionData,
    SubscriptionData,
)
from snuba.utils.metrics.timer import Timer
from snuba.web.query import parse_and_run_query
from tests.subscriptions import BaseSubscriptionTest
from tests.test_sessions_api import BaseSessionsMockTest

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
        DelegateSubscriptionData(
            project_id=1,
            query=(
                "MATCH (events) "
                "SELECT count() AS count "
                "WHERE "
                "platform IN tuple('a') "
            ),
            conditions=[["platform", "IN", ["a"]]],
            aggregations=[["count()", "", "count"]],
            time_window=timedelta(minutes=500),
            resolution=timedelta(minutes=1),
        ),
        None,
        id="Delegate subscription",
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

TESTS_OVER_SESSIONS = [
    pytest.param(
        DelegateSubscriptionData(
            project_id=1,
            query=(
                "MATCH (sessions) "
                "SELECT multiply(minus(1, divide(sessions_crashed, sessions)), 100) "
                "AS crash_free_percentage "
                "WHERE org_id = 1 AND project_id IN tuple(1) "
                "LIMIT 1 "
                "OFFSET 0 "
                "GRANULARITY 3600"
            ),
            conditions=[],
            aggregations=[
                [
                    "multiply(minus(1, divide(sessions_crashed, sessions)), 100)",
                    None,
                    "crash_free_percentage",
                ]
            ],
            time_window=timedelta(minutes=120),
            resolution=timedelta(minutes=1),
            organization=1,
            granularity=60,
            limit=1,
            offset=0,
        ),
        None,
        id="Delegate subscription",
    ),
    pytest.param(
        LegacySubscriptionData(
            project_id=1,
            conditions=[],
            aggregations=[
                [
                    "multiply(minus(1, divide(sessions_crashed, sessions)), 100)",
                    None,
                    "crash_free_percentage",
                ]
            ],
            time_window=timedelta(minutes=120),
            resolution=timedelta(minutes=1),
            granularity=60,
            limit=1,
            offset=0,
        ),
        InvalidQueryException,
        id="No organization provided for Sessions subscription",
    ),
]


class TestBuildRequestBase:
    @pytest.fixture(autouse=True)
    def subscription_rollout(self) -> Generator[None, None, None]:
        state.set_config("snql_subscription_rollout_pct", 1.0)
        yield
        state.set_config("snql_subscription_rollout", 0.0)

    def compare_conditions(
        self,
        subscription: SubscriptionData,
        exception: Optional[Exception],
        aggregate: str,
        value: int,
    ):
        timer = Timer("test")
        if exception is not None:
            with pytest.raises(exception):
                request = subscription.build_request(
                    self.dataset, datetime.utcnow(), 100, timer,
                )
                parse_and_run_query(self.dataset, request, timer)
            return

        request = subscription.build_request(
            self.dataset, datetime.utcnow(), 100, timer,
        )
        result = parse_and_run_query(self.dataset, request, timer)

        assert result.result["data"][0][aggregate] == value


class TestBuildRequest(BaseSubscriptionTest, TestBuildRequestBase):
    @pytest.mark.parametrize("subscription, exception", TESTS)  # type: ignore
    def test_conditions(
        self, subscription: SubscriptionData, exception: Optional[Exception]
    ) -> None:
        self.compare_conditions(subscription, exception, "count", 10)


class TestBuildRequestSessions(BaseSessionsMockTest, TestBuildRequestBase):
    def setup_method(self):
        self.dataset = get_dataset("sessions")
        self.generate_manual_session_events(1)

    @pytest.mark.parametrize("subscription, exception", TESTS_OVER_SESSIONS)  # type: ignore
    def test_conditions(
        self, subscription: SubscriptionData, exception: Optional[Exception]
    ) -> None:
        self.compare_conditions(subscription, exception, "crash_free_percentage", 95)
