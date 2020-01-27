from abc import abstractmethod, ABC
from binascii import crc32

from snuba.datasets.dataset import Dataset
from snuba.subscriptions.data import PartitionId, SubscriptionData


class SubscriptionDataPartitioner(ABC):
    @abstractmethod
    def build_partition_id(self, data: SubscriptionData) -> PartitionId:
        pass


class DatasetSubscriptionDataPartitioner(SubscriptionDataPartitioner):
    """
    Partitions a subscription based on the Dataset that we're going to store it in.
    """

    PARTITION_COUNT = 64

    def __init__(self, dataset: Dataset):
        self.__dataset = dataset

    def build_partition_id(self, data: SubscriptionData) -> PartitionId:
        # TODO: Use something from the dataset to determine the number of partitions
        return PartitionId(
            crc32(str(data.project_id).encode("utf-8")) % self.PARTITION_COUNT
        )
