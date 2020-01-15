from datetime import timedelta
from unittest.mock import Mock

from pytest import raises

from snuba.redis import redis_client
from snuba.subscriptions.store import RedisSubscriptionStore
from snuba.subscriptions.subscription import (
    InvalidSubscriptionError,
    SubscriptionCreator,
)
from snuba.web.query import RawQueryException
from tests.subscriptions import BaseSubscriptionTest


class TestSubscriptionCreator(BaseSubscriptionTest):
    def test(self):
        creator = SubscriptionCreator(self.dataset)
        project_id = 123
        conditions = [["platform", "IN", ["a"]]]
        aggregations = [["count()", "", "count"]]
        time_window = timedelta(minutes=10)
        resolution = timedelta(minutes=1)
        partition_id, subscription = creator.create(
            project_id, conditions, aggregations, time_window, resolution, Mock(),
        )
        assert subscription.project_id == project_id
        assert subscription.conditions == conditions
        assert subscription.aggregations == aggregations
        assert subscription.time_window == time_window
        assert subscription.resolution == resolution
        assert RedisSubscriptionStore(redis_client, partition_id).all() == [
            subscription
        ]

    def test_invalid_condition_column(self):
        creator = SubscriptionCreator(self.dataset)
        with raises(RawQueryException):
            creator.create(
                123,
                [["platfo", "IN", ["a"]]],
                [["count()", "", "count"]],
                timedelta(minutes=10),
                timedelta(minutes=1),
                Mock(),
            )

    def test_invalid_aggregation(self):
        creator = SubscriptionCreator(self.dataset)
        with raises(RawQueryException):
            creator.create(
                123,
                [["platform", "IN", ["a"]]],
                [["cout()", "", "count"]],
                timedelta(minutes=10),
                timedelta(minutes=1),
                Mock(),
            )

    def test_invalid_time_window(self):
        creator = SubscriptionCreator(self.dataset)
        with raises(InvalidSubscriptionError):
            creator.create(
                123,
                [["platfo", "IN", ["a"]]],
                [["count()", "", "count"]],
                timedelta(),
                timedelta(minutes=1),
                Mock(),
            )

    def test_invalid_resolution(self):
        creator = SubscriptionCreator(self.dataset)
        with raises(InvalidSubscriptionError):
            creator.create(
                123,
                [["platfo", "IN", ["a"]]],
                [["count()", "", "count"]],
                timedelta(minutes=1),
                timedelta(),
                Mock(),
            )
