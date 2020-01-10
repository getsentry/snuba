import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Collection

from rediscluster import StrictRedisCluster

from snuba.subscriptions.data import Subscription
from snuba.utils.codecs import Codec


@dataclass(frozen=True)
class RedisPayload:
    __slots__ = ["key", "value"]

    key: str
    value: bytes


class SubscriptionCodec(Codec[RedisPayload, Subscription]):
    def encode(self, value: Subscription) -> RedisPayload:
        return RedisPayload(
            value.id,
            json.dumps(
                {
                    "id": value.id,
                    "project_id": value.project_id,
                    "conditions": value.conditions,
                    "aggregations": value.aggregations,
                    "time_window": int(value.time_window.total_seconds()),
                    "resolution": int(value.resolution.total_seconds()),
                }
            ).encode("utf-8"),
        )

    def decode(self, value: RedisPayload) -> Subscription:
        data = json.loads(value.value.decode("utf-8"))
        return Subscription(
            id=data["id"],
            project_id=data["project_id"],
            conditions=data["conditions"],
            aggregations=data["aggregations"],
            time_window=timedelta(seconds=data["time_window"]),
            resolution=timedelta(seconds=data["resolution"]),
        )


class SubscriptionDoesNotExist(Exception):
    pass


class RedisSubscriptionStore:
    """
    A Redis backed store for Subscriptions. Stores subscriptions using
    `SubscriptionCodec`. Each instance of the store operates on a partition of data,
    defined by the `key` constructor param.
    """

    KEY_TEMPLATE = "subscriptions:{}"

    def __init__(self, key: str, client: StrictRedisCluster):
        self.key = key
        self.client = client

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
        payload = SubscriptionCodec().encode(subscription)
        self.client.hset(self._key, payload.key, payload.value)
        return payload.key

    def delete(self, subscription_id: str) -> None:
        """
        Removes a `Subscription` from the Redis store.
        """
        self.client.hdel(self._key, subscription_id)

    def get(self, subscription_id: str) -> Subscription:
        """
        Fetches a `Subscription` from the Redis store via id.
        :return: The `Subscription` instance
        """
        value = self.client.hget(self._key, subscription_id)
        if value is None:
            raise SubscriptionDoesNotExist()
        return SubscriptionCodec().decode(RedisPayload(subscription_id, value))

    def all(self) -> Collection[Subscription]:
        """
        Fetches all `Subscriptions` from the store
        :return: A collection of `Subscriptions`.
        """
        return [
            SubscriptionCodec().decode(RedisPayload(key, val))
            for key, val in self.client.hgetall(self._key).items()
        ]
