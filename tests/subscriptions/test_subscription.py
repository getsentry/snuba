from datetime import timedelta
from typing import List, Tuple, cast
from uuid import UUID

import pytest
from pytest import raises

from snuba.redis import redis_client
from snuba.subscriptions.data import (
    InvalidSubscriptionError,
    LegacySubscriptionData,
    SnQLSubscriptionData,
    SubscriptionData,
)
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.subscriptions.subscription import SubscriptionCreator, SubscriptionDeleter
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryException
from tests.subscriptions import BaseSubscriptionTest

TESTS_CREATE = [
    pytest.param(
        LegacySubscriptionData(
            project_id=123,
            conditions=[["platform", "IN", ["a"]]],
            aggregations=[["count()", "", "count"]],
            time_window=timedelta(minutes=10),
            resolution=timedelta(minutes=1),
        ),
        id="Legacy subscription",
    ),
    pytest.param(
        SnQLSubscriptionData(
            project_id=123,
            query=(
                "MATCH (events) "
                "SELECT count() AS count BY time "
                "WHERE "
                "platform IN tuple('a') "
            ),
            time_window=timedelta(minutes=10),
            resolution=timedelta(minutes=1),
        ),
        id="SnQL subscription",
    ),
]

TESTS_INVALID = [
    pytest.param(
        LegacySubscriptionData(
            project_id=123,
            conditions=[["platfo", "IN", ["a"]]],
            aggregations=[["count()", "", "count"]],
            time_window=timedelta(minutes=10),
            resolution=timedelta(minutes=1),
        ),
        id="Legacy subscription",
    ),
    pytest.param(
        SnQLSubscriptionData(
            project_id=123,
            query=(
                "MATCH (events) "
                "SELECT count() AS count BY time "
                "WHERE "
                "platfo IN tuple('a') "
            ),
            time_window=timedelta(minutes=10),
            resolution=timedelta(minutes=1),
        ),
        id="SnQL subscription",
    ),
]


class TestSubscriptionCreator(BaseSubscriptionTest):

    timer = Timer("test")

    @pytest.mark.parametrize("subscription", TESTS_CREATE)
    def test(self, subscription: SubscriptionData) -> None:
        creator = SubscriptionCreator(self.dataset)
        identifier = creator.create(subscription, self.timer)
        assert (
            cast(
                List[Tuple[UUID, SubscriptionData]],
                RedisSubscriptionDataStore(
                    redis_client, self.dataset, identifier.partition,
                ).all(),
            )[0][1]
            == subscription
        )

    @pytest.mark.parametrize("subscription", TESTS_INVALID)
    def test_invalid_condition_column(self, subscription: SubscriptionData) -> None:
        creator = SubscriptionCreator(self.dataset)
        with raises(QueryException):
            creator.create(
                LegacySubscriptionData(
                    123,
                    timedelta(minutes=1),
                    timedelta(minutes=10),
                    [["platfo", "IN", ["a"]]],
                    [["count()", "", "count"]],
                ),
                self.timer,
            )

    def test_invalid_aggregation(self) -> None:
        creator = SubscriptionCreator(self.dataset)
        with raises(QueryException):
            creator.create(
                LegacySubscriptionData(
                    123,
                    timedelta(minutes=1),
                    timedelta(minutes=10),
                    [["platform", "IN", ["a"]]],
                    [["cout()", "", "count"]],
                ),
                self.timer,
            )

    def test_invalid_time_window(self) -> None:
        creator = SubscriptionCreator(self.dataset)
        with raises(InvalidSubscriptionError):
            creator.create(
                LegacySubscriptionData(
                    123,
                    timedelta(minutes=1),
                    timedelta(),
                    [["platfo", "IN", ["a"]]],
                    [["count()", "", "count"]],
                ),
                self.timer,
            )

        with raises(InvalidSubscriptionError):
            creator.create(
                SnQLSubscriptionData(
                    project_id=123,
                    query=(
                        "MATCH (events) "
                        "SELECT count() AS count BY time "
                        "WHERE "
                        "platform IN tuple('a') "
                    ),
                    time_window=timedelta(minutes=1),
                    resolution=timedelta(),
                ),
                self.timer,
            )

        with raises(InvalidSubscriptionError):
            creator.create(
                LegacySubscriptionData(
                    123,
                    timedelta(minutes=1),
                    timedelta(hours=48),
                    [["platfo", "IN", ["a"]]],
                    [["count()", "", "count"]],
                ),
                self.timer,
            )

    def test_invalid_resolution(self) -> None:
        creator = SubscriptionCreator(self.dataset)
        with raises(InvalidSubscriptionError):
            creator.create(
                LegacySubscriptionData(
                    123,
                    timedelta(),
                    timedelta(minutes=1),
                    [["platfo", "IN", ["a"]]],
                    [["count()", "", "count"]],
                ),
                self.timer,
            )


class TestSubscriptionDeleter(BaseSubscriptionTest):
    def test(self) -> None:
        creator = SubscriptionCreator(self.dataset)
        subscription = LegacySubscriptionData(
            project_id=1,
            conditions=[],
            aggregations=[["count()", "", "count"]],
            time_window=timedelta(minutes=10),
            resolution=timedelta(minutes=1),
        )
        identifier = creator.create(subscription, Timer("test"))
        assert (
            cast(
                List[Tuple[UUID, SubscriptionData]],
                RedisSubscriptionDataStore(
                    redis_client, self.dataset, identifier.partition,
                ).all(),
            )[0][1]
            == subscription
        )

        SubscriptionDeleter(self.dataset, identifier.partition).delete(identifier.uuid)
        assert (
            RedisSubscriptionDataStore(
                redis_client, self.dataset, identifier.partition,
            ).all()
            == []
        )
