from datetime import timedelta
from typing import Iterator

import pytest
from pytest import raises

from snuba.datasets.dataset import Dataset
from snuba.redis import RedisClientType
from snuba.subscriptions.data import InvalidSubscriptionError, SubscriptionData
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.subscriptions.subscription import SubscriptionCreator, SubscriptionDeleter
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryException
from tests.assertions import assert_changes
from tests.base import redis_manager


@pytest.fixture
def redis() -> Iterator[RedisClientType]:
    with redis_manager() as instance:
        yield instance


@pytest.fixture
def creator(redis: RedisClientType, dataset: Dataset) -> SubscriptionCreator:
    # XXX: Weird coupling to RedisClientType here.
    return SubscriptionCreator(dataset)


timer = Timer("test")


def test_basic_operations(
    redis: RedisClientType, dataset: Dataset, creator: SubscriptionCreator
) -> None:
    subscription_data = SubscriptionData(
        project_id=123,
        conditions=[["platform", "IN", ["a"]]],
        aggregations=[["count()", "", "count"]],
        time_window=timedelta(minutes=10),
        resolution=timedelta(minutes=1),
    )

    identifier = creator.create(subscription_data, timer)

    # NOTE: This can't use ``assert_changes`` since we don't know the partition
    # until after the write has completed -- we just have to assume the data
    # wasn't already there for some reason.
    store = RedisSubscriptionDataStore(redis, dataset, identifier.partition)
    assert dict(store.all())[identifier.uuid] == subscription_data

    with assert_changes(store.all, [(identifier.uuid, subscription_data)], []):
        SubscriptionDeleter(dataset, identifier.partition).delete(identifier.uuid)


def test_invalid_condition_column(creator: SubscriptionCreator) -> None:
    with raises(QueryException):
        creator.create(
            SubscriptionData(
                123,
                [["platfo", "IN", ["a"]]],  # invalid column
                [["count()", "", "count"]],
                timedelta(minutes=10),
                timedelta(minutes=1),
            ),
            timer,
        )


def test_invalid_aggregation(creator: SubscriptionCreator) -> None:
    with raises(QueryException):
        creator.create(
            SubscriptionData(
                123,
                [["platform", "IN", ["a"]]],
                [["cout()", "", "count"]],
                timedelta(minutes=10),
                timedelta(minutes=1),
            ),
            timer,
        )


def test_invalid_time_window(creator: SubscriptionCreator) -> None:
    with raises(InvalidSubscriptionError):
        creator.create(
            SubscriptionData(
                123,
                [["platfo", "IN", ["a"]]],
                [["count()", "", "count"]],
                timedelta(),
                timedelta(minutes=1),
            ),
            timer,
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
            timer,
        )


def test_invalid_resolution(creator: SubscriptionCreator) -> None:
    with raises(InvalidSubscriptionError):
        creator.create(
            SubscriptionData(
                123,
                [["platfo", "IN", ["a"]]],
                [["count()", "", "count"]],
                timedelta(minutes=1),
                timedelta(),
            ),
            timer,
        )
