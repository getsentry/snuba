from datetime import timedelta

from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.subscriptions.data import SubscriptionData
from snuba.subscriptions.partitioner import TopicSubscriptionDataPartitioner


def test_partitioning() -> None:
    data = SubscriptionData(123, [], [], timedelta(minutes=10), timedelta(minutes=1))
    partitioner = TopicSubscriptionDataPartitioner(
        KafkaTopicSpec("topic", partitions_number=64)
    )
    assert partitioner.build_partition_id(data) == 18
