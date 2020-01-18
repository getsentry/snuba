from typing import Collection, Tuple

from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset_name
from snuba.redis import RedisClientType
from snuba.subscriptions.codecs import SubscriptionDataCodec
from snuba.subscriptions.data import PartitionId, SubscriptionData, SubscriptionKey


class RedisSubscriptionDataStore:
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

    def create(self, key: SubscriptionKey, data: SubscriptionData) -> None:
        """
        Stores subscription data in Redis. Will overwrite any existing
        subscriptions with the same id.
        """
        self.client.hset(self.__key, key.encode("utf-8"), self.codec.encode(data))

    def delete(self, key: SubscriptionKey) -> None:
        """
        Removes a subscription from the Redis store.
        """
        self.client.hdel(self.__key, key.encode("utf-8"))

    def all(self) -> Collection[Tuple[SubscriptionKey, SubscriptionData]]:
        """
        Fetches all subscriptions from the store.
        :return: A collection of `Subscriptions`.
        """
        return [
            (SubscriptionKey(key.decode("utf-8")), self.codec.decode(val))
            for key, val in self.client.hgetall(self.__key).items()
        ]
