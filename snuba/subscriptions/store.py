from typing import Collection, Tuple

from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset_name
from snuba.redis import RedisClientType
from snuba.subscriptions.codecs import SubscriptionDataCodec
from snuba.subscriptions.data import SubscriptionData


class RedisSubscriptionDataStore:
    """
    A Redis backed store for subscription data. Stores subscriptions using
    `SubscriptionDataCodec`. Each instance of the store operates on a
    partition of data, defined by the `key` constructor param.
    """

    KEY_TEMPLATE = "subscriptions:{}:{}"

    def __init__(self, client: RedisClientType, dataset: Dataset, key: str):
        self.client = client
        self.dataset = dataset
        self.key = key
        self.codec = SubscriptionDataCodec()

    @property
    def _key(self) -> str:
        return self.KEY_TEMPLATE.format(get_dataset_name(self.dataset), self.key)

    def create(self, subscription_id: str, data: SubscriptionData) -> None:
        """
        Stores subscription data in Redis. Will overwrite any existing
        subscriptions with the same id.
        """
        payload = self.codec.encode(data)
        self.client.hset(self._key, subscription_id.encode("utf-8"), payload)

    def delete(self, subscription_id: str) -> None:
        """
        Removes a subscription from the Redis store.
        """
        self.client.hdel(self._key, subscription_id)

    def all(self) -> Collection[Tuple[str, SubscriptionData]]:
        """
        Fetches all subscriptions from the store.
        :return: A collection of `Subscriptions`.
        """
        return [
            (key.decode("utf-8"), self.codec.decode(val))
            for key, val in self.client.hgetall(self._key).items()
        ]
