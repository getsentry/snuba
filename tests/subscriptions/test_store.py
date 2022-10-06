from typing import Sequence
from uuid import uuid1

from snuba.redis import RedisClientKey, get_redis_client
from snuba.subscriptions.data import PartitionId, SubscriptionData
from snuba.subscriptions.store import RedisSubscriptionDataStore
from tests.subscriptions import BaseSubscriptionTest
from tests.subscriptions.subscriptions_utils import create_entity_subscription


class TestRedisSubscriptionStore(BaseSubscriptionTest):
    @property
    def subscription(self) -> Sequence[SubscriptionData]:
        return [
            SubscriptionData(
                project_id=self.project_id,
                query="MATCH (events) SELECT count() WHERE in(platform, 'a')",
                time_window_sec=500 * 60,
                resolution_sec=60,
                entity_subscription=create_entity_subscription(),
            ),
            SubscriptionData(
                project_id=self.project_id,
                time_window_sec=500 * 60,
                resolution_sec=60,
                query="MATCH (events) SELECT count() WHERE in(platform, 'a')",
                entity_subscription=create_entity_subscription(),
            ),
        ]

    def build_store(self, key: int = 1) -> RedisSubscriptionDataStore:
        return RedisSubscriptionDataStore(
            get_redis_client(RedisClientKey.SUBSCRIPTION_STORE),
            self.entity_key,
            PartitionId(key),
        )

    def test_create(self) -> None:
        store = self.build_store()
        subscription_id_1 = uuid1()
        store.create(subscription_id_1, self.subscription[0])
        subscription_id_2 = uuid1()
        store.create(subscription_id_2, self.subscription[1])
        assert sorted(store.all()) == [
            (subscription_id_1, self.subscription[0]),
            (subscription_id_2, self.subscription[1]),
        ]

    def test_delete(self) -> None:
        store = self.build_store()
        subscription_id = uuid1()
        store.create(subscription_id, self.subscription[0])
        assert store.all() == [(subscription_id, self.subscription[0])]
        store.delete(subscription_id)
        assert store.all() == []

    def test_all(self) -> None:
        store = self.build_store()
        assert store.all() == []
        subscription_id = uuid1()
        store.create(subscription_id, self.subscription[0])
        assert store.all() == [(subscription_id, self.subscription[0])]
        new_subscription_id = uuid1()
        store.create(new_subscription_id, self.subscription[1])
        assert sorted(store.all(), key=lambda row: row[0]) == [
            (subscription_id, self.subscription[0]),
            (new_subscription_id, self.subscription[1]),
        ]

    def test_partitions(self) -> None:
        store_1 = self.build_store(1)
        store_2 = self.build_store(2)
        subscription_id = uuid1()
        store_1.create(subscription_id, self.subscription[0])
        assert store_2.all() == []
        assert store_1.all() == [(subscription_id, self.subscription[0])]

        new_subscription_id = uuid1()
        store_2.create(new_subscription_id, self.subscription[1])
        assert store_1.all() == [(subscription_id, self.subscription[0])]
        assert store_2.all() == [(new_subscription_id, self.subscription[1])]
