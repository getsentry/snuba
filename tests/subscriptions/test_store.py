from typing import Iterator
import pytest
from datetime import timedelta
from uuid import uuid1

from snuba.datasets.dataset import Dataset
from snuba.redis import RedisClientType
from snuba.subscriptions.data import SubscriptionData
from snuba.subscriptions.store import SubscriptionDataStore, RedisSubscriptionDataStore
from tests.assertions import assert_changes, assert_does_not_change
from tests.base import redis_manager
from tests.subscriptions.fixtures import dataset  # NOQA


@pytest.fixture
def redis() -> Iterator[RedisClientType]:
    with redis_manager() as instance:
        yield instance


def test_basic_operations(redis: RedisClientType, dataset: Dataset) -> None:
    store: SubscriptionDataStore = RedisSubscriptionDataStore(redis, dataset, 1)

    subscription_id = uuid1()
    subscription_data = SubscriptionData(
        project_id=1,
        conditions=[["platform", "IN", ["a"]]],
        aggregations=[["count()", "", "count"]],
        time_window=timedelta(minutes=500),
        resolution=timedelta(minutes=1),
    )

    # Deleting a key that does not exist should work without erroring.
    with assert_does_not_change(store.all, []):
        store.delete(subscription_id)

    with assert_changes(store.all, [], [(subscription_id, subscription_data)]):
        store.create(subscription_id, subscription_data)

    new_subscription_id = uuid1()
    new_subscription_data = SubscriptionData(
        project_id=1,
        conditions=[["platform", "IN", ["b"]]],
        aggregations=[["count()", "", "something"]],
        time_window=timedelta(minutes=400),
        resolution=timedelta(minutes=2),
    )

    with assert_changes(
        lambda: sorted(store.all()),
        [(subscription_id, subscription_data)],
        sorted(
            [
                (subscription_id, subscription_data),
                (new_subscription_id, new_subscription_data),
            ]
        ),
    ):
        store.create(new_subscription_id, new_subscription_data)

    with assert_changes(
        lambda: sorted(store.all()),
        sorted(
            [
                (subscription_id, subscription_data),
                (new_subscription_id, new_subscription_data),
            ]
        ),
        [(new_subscription_id, new_subscription_data)],
    ):
        store.delete(subscription_id)

    new_subscription_data_updated = SubscriptionData(
        project_id=1,
        conditions=[["platform", "IN", ["a", "b"]]],
        aggregations=[["count()", "", "count"]],
        time_window=timedelta(minutes=500),
        resolution=timedelta(minutes=1),
    )

    # Overwriting a key that already exists should not create a duplicate
    # record but replace the previous state.
    with assert_changes(
        store.all,
        [(new_subscription_id, new_subscription_data)],
        [(new_subscription_id, new_subscription_data_updated)],
    ):
        store.create(new_subscription_id, new_subscription_data_updated)

    with assert_changes(
        store.all, [(new_subscription_id, new_subscription_data_updated)], []
    ):
        store.delete(new_subscription_id)


def test_partition_isolation(redis: RedisClientType, dataset: Dataset) -> None:
    store_1: SubscriptionDataStore = RedisSubscriptionDataStore(redis, dataset, 1)
    store_2: SubscriptionDataStore = RedisSubscriptionDataStore(redis, dataset, 2)

    subscription_id = uuid1()
    subscription_data = SubscriptionData(
        project_id=1,
        conditions=[["platform", "IN", ["a"]]],
        aggregations=[["count()", "", "count"]],
        time_window=timedelta(minutes=500),
        resolution=timedelta(minutes=1),
    )

    with assert_changes(
        store_1.all, [], [(subscription_id, subscription_data)]
    ), assert_does_not_change(store_2.all, []):
        store_1.create(subscription_id, subscription_data)

    with assert_changes(
        store_2.all, [], [(subscription_id, subscription_data)]
    ), assert_does_not_change(store_1.all, [(subscription_id, subscription_data)]):
        store_2.create(subscription_id, subscription_data)
