from abc import abstractmethod, ABC
from binascii import crc32

from snuba.datasets.dataset import Dataset
from snuba.subscriptions.data import Subscription


class SubscriptionPartitioner(ABC):
    @abstractmethod
    def build_partition_id(self, subscription: Subscription) -> int:
        pass


class DatasetSubscriptionPartitioner(SubscriptionPartitioner):
    """
    Partitions a subscription based on the Dataset that we're going to store it in.
    """

    PARTITION_COUNT = 64

    def __init__(self, dataset: Dataset):
        self.__dataset = dataset

    def build_partition_id(self, subscription: Subscription) -> int:
        # TODO: Use something from the dataset to determine the number of partitions
        return (
            crc32(str(subscription.project_id).encode("utf-8")) % self.PARTITION_COUNT
        )
