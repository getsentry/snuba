from abc import ABC, abstractmethod
from binascii import crc32

from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.subscriptions.data import PartitionId, SnQLSubscriptionData


class SubscriptionDataPartitioner(ABC):
    @abstractmethod
    def build_partition_id(self, data: SnQLSubscriptionData) -> PartitionId:
        pass


class TopicSubscriptionDataPartitioner(SubscriptionDataPartitioner):
    """
    Identifies the partition index that contains the source data for a subscription.
    """

    def __init__(self, topic: KafkaTopicSpec):
        self.__topic = topic

    def build_partition_id(self, data: SnQLSubscriptionData) -> PartitionId:
        return PartitionId(
            crc32(str(data.project_id).encode("utf-8")) % self.__topic.partitions_number
        )
