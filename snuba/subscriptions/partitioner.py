from abc import abstractmethod, ABC
from binascii import crc32

from snuba.subscriptions.data import PartitionId, SubscriptionData
from snuba.utils.streams.topics import KafkaTopicSpec


class SubscriptionDataPartitioner(ABC):
    @abstractmethod
    def build_partition_id(self, data: SubscriptionData) -> PartitionId:
        pass


class TopicSubscriptionDataPartitioner(SubscriptionDataPartitioner):
    """
    Identifies the partition index that contains the source data for a subscription.
    """

    def __init__(self, topic: KafkaTopicSpec):
        self.__topic = topic

    def build_partition_id(self, data: SubscriptionData) -> PartitionId:
        return PartitionId(
            crc32(str(data.project_id).encode("utf-8")) % self.__topic.partitions_number
        )
