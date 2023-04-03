from datetime import datetime
from typing import Optional, Type, Union

import pytest

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query.exceptions import InvalidQueryException
from snuba.subscriptions.data import SubscriptionData
from snuba.utils.metrics.timer import Timer
from snuba.web.query import parse_and_run_query
from tests.subscriptions import BaseSubscriptionTest

TESTS = [
    pytest.param(
        SubscriptionData(
            project_id=1,
            query=(
                "MATCH (events) "
                "SELECT count() AS count "
                "WHERE "
                "platform IN tuple('a') "
            ),
            time_window_sec=500 * 60,
            resolution_sec=60,
            entity=get_entity(EntityKey.EVENTS),
            metadata={},
        ),
        None,
        id="SnQL subscription",
    ),
    pytest.param(
        SubscriptionData(
            project_id=1,
            query=(
                "MATCH (events) "
                "SELECT count() AS count, avg(timestamp) AS average_t "
                "WHERE "
                "platform IN tuple('a') "
            ),
            time_window_sec=500 * 60,
            resolution_sec=60,
            entity=get_entity(EntityKey.EVENTS),
            metadata={},
        ),
        InvalidQueryException,
        id="SnQL subscription with 2 many aggregates",
    ),
    pytest.param(
        SubscriptionData(
            project_id=1,
            query=(
                "MATCH (events) "
                "SELECT count() AS count BY project_id "
                "WHERE platform IN tuple('a') "
                "AND project_id IN tuple(1) "
            ),
            time_window_sec=500 * 60,
            resolution_sec=60,
            entity=get_entity(EntityKey.EVENTS),
            metadata={},
        ),
        InvalidQueryException,
        id="SnQL subscription with disallowed clause",
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
                    self.dataset,
                    datetime.utcnow(),
                    100,
                    timer,
                )
                parse_and_run_query(self.dataset, request, timer)
            return

        request = subscription.build_request(
            self.dataset,
            datetime.utcnow(),
            100,
            timer,
        )
        result = parse_and_run_query(self.dataset, request, timer)

        assert result.result["data"][0][aggregate] == value


class TestBuildRequest(BaseSubscriptionTest, TestBuildRequestBase):
    @pytest.mark.parametrize("subscription, exception", TESTS)
    @pytest.mark.clickhouse_db
    @pytest.mark.redis_db
    def test_conditions(
        self, subscription: SubscriptionData, exception: Optional[Type[Exception]]
    ) -> None:
        self.compare_conditions(subscription, exception, "count", 10)
