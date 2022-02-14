from datetime import datetime, timedelta
from typing import Optional, Type, Union

import pytest

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.factory import get_dataset
from snuba.query.exceptions import InvalidQueryException
from snuba.subscriptions.data import SnQLSubscriptionData, SubscriptionData
from snuba.utils.metrics.timer import Timer
from snuba.web.query import parse_and_run_query
from tests.subscriptions import BaseSubscriptionTest
from tests.subscriptions.subscriptions_utils import create_entity_subscription
from tests.test_sessions_api import BaseSessionsMockTest

TESTS = [
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
            entity_subscription=create_entity_subscription(),
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
            entity_subscription=create_entity_subscription(),
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
            entity_subscription=create_entity_subscription(),
        ),
        InvalidQueryException,
        id="SnQL subscription with disallowed clause",
    ),
]

TESTS_OVER_SESSIONS = [
    pytest.param(
        SnQLSubscriptionData(
            project_id=1,
            query=(
                """
                MATCH (sessions) SELECT if(greater(sessions,0),
                divide(sessions_crashed,sessions),null)
                AS _crash_rate_alert_aggregate, identity(sessions) AS _total_sessions,
                identity(sessions_crashed)
                WHERE org_id = 1 AND project_id IN tuple(1) LIMIT 1
                OFFSET 0 GRANULARITY 3600
                """
            ),
            time_window=timedelta(minutes=120),
            resolution=timedelta(minutes=1),
            entity_subscription=create_entity_subscription(EntityKey.SESSIONS, 1),
        ),
        InvalidQueryException,
        id="Snql subscription",
    ),
]


class TestBuildRequestBase:
    dataset: Dataset

    def compare_conditions(
        self,
        subscription: SubscriptionData,
        exception: Optional[Type[Exception]],
        aggregate: str,
        value: Union[int, float],
    ) -> None:
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
    @pytest.mark.parametrize("subscription, exception", TESTS)
    def test_conditions(
        self, subscription: SubscriptionData, exception: Optional[Type[Exception]]
    ) -> None:
        self.compare_conditions(subscription, exception, "count", 10)


class TestBuildRequestSessions(BaseSessionsMockTest, TestBuildRequestBase):
    def setup_method(self) -> None:
        self.dataset = get_dataset("sessions")
        self.generate_manual_session_events(1)

    @pytest.mark.parametrize("subscription, exception", TESTS_OVER_SESSIONS)
    def test_conditions(
        self, subscription: SubscriptionData, exception: Optional[Type[Exception]]
    ) -> None:
        self.compare_conditions(
            subscription, exception, "_crash_rate_alert_aggregate", 0.05
        )
