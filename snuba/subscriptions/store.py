import abc
from uuid import UUID
from typing import Collection, Tuple

from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset_name
from snuba.redis import RedisClientType
from snuba.subscriptions.codecs import SubscriptionDataCodec
from snuba.subscriptions.data import PartitionId, SubscriptionData


class SubscriptionStore(abc.ABC):
    @abc.abstractmethod
    def create(self, subscription_id: str, subscription: SubscriptionData) -> None:
        """
        Creates a `Subscription` in the store. Will overwrite any existing `Subscriptions`
        with the same id.
        """
        pass

    @abc.abstractmethod
    def delete(self, subscription_id: str) -> None:
        """
        Removes a `Subscription` from the store.
        """
        pass

    @abc.abstractmethod
    def all(self) -> Collection[Tuple[str, SubscriptionData]]:
        """
        Fetches all `Subscriptions` from the store
        :return: A collection of `Subscriptions`.
        """
        pass


class RedisSubscriptionDataStore(SubscriptionStore):
    """
    A Redis backed store for subscription data. Stores subscriptions using
    `SubscriptionDataCodec`. Each instance of the store operates on a
    partition of data, defined by the `key` constructor param.
    """

    KEY_TEMPLATE = "subscriptions:{}:{}"

    def __init__(
        self, client: RedisClientType, dataset: Dataset, partition_id: PartitionId
    ):
        self.client = client
        self.codec = SubscriptionDataCodec()
        self.__key = f"subscriptions:{get_dataset_name(dataset)}:{partition_id}"

    def create(self, key: UUID, data: SubscriptionData) -> None:
        """
        Stores subscription data in Redis. Will overwrite any existing
        subscriptions with the same id.
        """
        self.client.hset(self.__key, key.hex.encode("utf-8"), self.codec.encode(data))

    def delete(self, key: UUID) -> None:
        """
        Removes a subscription from the Redis store.
        """
        self.client.hdel(self.__key, key.hex.encode("utf-8"))

    def all(self) -> Collection[Tuple[UUID, SubscriptionData]]:
        """
        Fetches all subscriptions from the store.
        :return: A collection of `Subscriptions`.
        """
        return [
            (UUID(key.decode("utf-8")), self.codec.decode(val))
            for key, val in self.client.hgetall(self.__key).items()
        ]
