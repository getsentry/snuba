from datetime import timedelta
from unittest.mock import Mock

from pytest import raises

from snuba.redis import redis_client
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.subscriptions.data import InvalidSubscriptionError, SubscriptionData
from snuba.subscriptions.subscription import SubscriptionCreator
from snuba.web.query import RawQueryException
from tests.subscriptions import BaseSubscriptionTest


class TestSubscriptionCreator(BaseSubscriptionTest):
    def test(self):
        creator = SubscriptionCreator(self.dataset)
        subscription = SubscriptionData(
            project_id=123,
            conditions=[["platform", "IN", ["a"]]],
            aggregations=[["count()", "", "count"]],
            time_window=timedelta(minutes=10),
            resolution=timedelta(minutes=1),
        )
        identifier = creator.create(subscription, Mock())
        RedisSubscriptionDataStore(
            redis_client, self.dataset, identifier.partition,
        ).all()[0][1] == subscription

    def test_invalid_condition_column(self):
        creator = SubscriptionCreator(self.dataset)
        with raises(RawQueryException):
            creator.create(
                SubscriptionData(
                    123,
                    [["platfo", "IN", ["a"]]],
                    [["count()", "", "count"]],
                    timedelta(minutes=10),
                    timedelta(minutes=1),
                ),
                Mock(),
            )

    def test_invalid_aggregation(self):
        creator = SubscriptionCreator(self.dataset)
        with raises(RawQueryException):
            creator.create(
                SubscriptionData(
                    123,
                    [["platform", "IN", ["a"]]],
                    [["cout()", "", "count"]],
                    timedelta(minutes=10),
                    timedelta(minutes=1),
                ),
                Mock(),
            )

    def test_invalid_time_window(self):
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
                Mock(),
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
                Mock(),
            )

    def test_invalid_resolution(self):
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
                Mock(),
            )
