import abc
import logging
import time
from typing import Iterable, Tuple
from uuid import UUID

import sentry_sdk

from snuba import environment
from snuba.datasets.entities.entity_key import EntityKey
from snuba.redis import RedisClientType
from snuba.subscriptions.codecs import SubscriptionDataCodec
from snuba.subscriptions.data import PartitionId, SubscriptionData
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger(__name__)
metrics = MetricsWrapper(environment.metrics, "subscription_store")


class SubscriptionDataStore(abc.ABC):
    @abc.abstractmethod
    def create(self, key: UUID, data: SubscriptionData) -> None:
        """
        Creates a `Subscription` in the store. Will overwrite any existing `Subscriptions`
        with the same id.
        """
        pass

    @abc.abstractmethod
    def delete(self, key: UUID) -> None:
        """
        Removes a `Subscription` from the store.
        """
        pass

    @abc.abstractmethod
    def all(self) -> Iterable[Tuple[UUID, SubscriptionData]]:
        """
        Fetches all `Subscriptions` from the store
        :return: An iterable of `Subscriptions`.
        """
        pass


class RedisSubscriptionDataStore(SubscriptionDataStore):
    """
    A Redis backed store for subscription data. Stores subscriptions using
    `SubscriptionDataCodec`. Each instance of the store operates on a
    partition of data, defined by the `key` constructor param.
    """

    def __init__(self, client: RedisClientType, entity: EntityKey, partition_id: PartitionId):
        self.client = client
        self.codec = SubscriptionDataCodec(entity)
        self.__key = f"subscriptions:{entity.value}:{partition_id}"

    def create(self, key: UUID, data: SubscriptionData) -> None:
        """
        Stores subscription data in Redis. Will overwrite any existing
        subscriptions with the same id.
        """
        try:
            self.client.hset(self.__key, key.hex.encode("utf-8"), self.codec.encode(data))
        except Exception as e:
            logger.error(f"Failed to create subscription {key} in Redis: {e}")
            sentry_sdk.capture_exception(e)
            metrics.increment("redis_error", tags={"operation": "create"})
            raise

    def delete(self, key: UUID) -> None:
        """
        Removes a subscription from the Redis store.
        """
        try:
            self.client.hdel(self.__key, key.hex.encode("utf-8"))
        except Exception as e:
            logger.error(f"Failed to delete subscription {key} from Redis: {e}")
            sentry_sdk.capture_exception(e)
            metrics.increment("redis_error", tags={"operation": "delete"})
            raise

    def all(self) -> Iterable[Tuple[UUID, SubscriptionData]]:
        """
        Fetches all subscriptions from the store.
        :return: An iterable of `Subscriptions`.
        """
        start = time.time()
        try:
            res = [
                (UUID(key.decode("utf-8")), self.codec.decode(val))
                for key, val in self.client.hgetall(self.__key).items()
            ]
        except Exception as e:
            logger.error(f"Failed to fetch subscriptions from Redis: {e}")
            sentry_sdk.capture_exception(e)
            metrics.increment("redis_error", tags={"operation": "all"})
            raise
        fetch_time = time.time() - start

        metrics.timing("all_fetch_time", fetch_time)
        return res
