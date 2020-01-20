import json
from datetime import timedelta
from typing import Collection

from snuba.redis import RedisClientType
from snuba.subscriptions.data import Subscription
from snuba.utils.codecs import Codec


class SubscriptionCodec(Codec[bytes, Subscription]):
    def encode(self, value: Subscription) -> bytes:
        return json.dumps(
            {
                "id": value.id,
                "project_id": value.project_id,
                "conditions": value.conditions,
                "aggregations": value.aggregations,
                "time_window": int(value.time_window.total_seconds()),
                "resolution": int(value.resolution.total_seconds()),
            }
        ).encode("utf-8")

    def decode(self, value: bytes) -> Subscription:
        data = json.loads(value.decode("utf-8"))
        return Subscription(
            id=data["id"],
            project_id=data["project_id"],
            conditions=data["conditions"],
            aggregations=data["aggregations"],
            time_window=timedelta(seconds=data["time_window"]),
            resolution=timedelta(seconds=data["resolution"]),
        )


class RedisSubscriptionStore:
    """
    A Redis backed store for Subscriptions. Stores subscriptions using
    `SubscriptionCodec`. Each instance of the store operates on a partition of data,
    defined by the `key` constructor param.
    """

    KEY_TEMPLATE = "subscriptions:{}"

    def __init__(self, client: RedisClientType, key: str):
        self.client = client
        self.key = key
        self.codec = SubscriptionCodec()

    @property
    def _key(self) -> str:
        return self.KEY_TEMPLATE.format(self.key)

    def create(self, subscription: Subscription) -> str:
        """
        Stores a `Subscription` in Redis. Will overwrite any existing `Subscriptions`
        with the same id.
        :param subscription:
        :return: A str representing the id of the subscription
        """
        payload = self.codec.encode(subscription)
        self.client.hset(self._key, subscription.id, payload)
        return subscription.id

    def delete(self, subscription_id: str) -> None:
        """
        Removes a `Subscription` from the Redis store.
        """
        self.client.hdel(self._key, subscription_id)

    def all(self) -> Collection[Subscription]:
        """
        Fetches all `Subscriptions` from the store
        :return: A collection of `Subscriptions`.
        """
        return [
            self.codec.decode(val) for val in self.client.hgetall(self._key).values()
        ]
