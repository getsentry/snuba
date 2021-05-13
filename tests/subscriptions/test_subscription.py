from datetime import timedelta
from typing import List, Tuple, cast
from uuid import UUID

from pytest import raises

from snuba.redis import redis_client
from snuba.subscriptions.data import InvalidSubscriptionError, SubscriptionData
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.subscriptions.subscription import SubscriptionCreator, SubscriptionDeleter
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryException
from tests.subscriptions import BaseSubscriptionTest


class TestSubscriptionCreator(BaseSubscriptionTest):

    timer = Timer("test")

    def test(self) -> None:
        creator = SubscriptionCreator(self.dataset)
        subscription = SubscriptionData(
            project_id=123,
            conditions=[["platform", "IN", ["a"]]],
            aggregations=[["count()", "", "count"]],
            time_window=timedelta(minutes=10),
            resolution=timedelta(minutes=1),
        )
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

    def test_invalid_condition_column(self) -> None:
        creator = SubscriptionCreator(self.dataset)
        with raises(QueryException):
            creator.create(
                SubscriptionData(
                    123,
                    [["platfo", "IN", ["a"]]],
                    [["count()", "", "count"]],
                    timedelta(minutes=10),
                    timedelta(minutes=1),
                ),
                self.timer,
            )

    def test_invalid_aggregation(self) -> None:
        creator = SubscriptionCreator(self.dataset)
        with raises(QueryException):
            creator.create(
                SubscriptionData(
                    123,
                    [["platform", "IN", ["a"]]],
                    [["cout()", "", "count"]],
                    timedelta(minutes=10),
                    timedelta(minutes=1),
                ),
                self.timer,
            )

    def test_invalid_time_window(self) -> None:
        creator = SubscriptionCreator(self.dataset)
        with raises(InvalidSubscriptionError):
            creator.create(
                SubscriptionData(
                    123,
                    [["platfo", "IN", ["a"]]],
                    [["count()", "", "count"]],
                    timedelta(),
                    timedelta(minutes=1),
                ),
                self.timer,
            )

        with raises(InvalidSubscriptionError):
            creator.create(
                SubscriptionData(
                    123,
                    [["platfo", "IN", ["a"]]],
                    [["count()", "", "count"]],
                    timedelta(hours=48),
                    timedelta(minutes=1),
                ),
                self.timer,
            )

    def test_invalid_resolution(self) -> None:
        creator = SubscriptionCreator(self.dataset)
        with raises(InvalidSubscriptionError):
            creator.create(
                SubscriptionData(
                    123,
                    [["platfo", "IN", ["a"]]],
                    [["count()", "", "count"]],
                    timedelta(minutes=1),
                    timedelta(),
                ),
                self.timer,
            )


class TestSubscriptionDeleter(BaseSubscriptionTest):
    def test(self) -> None:
        creator = SubscriptionCreator(self.dataset)
        subscription = SubscriptionData(
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
