from datetime import timedelta

from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.subscriptions.data import SubscriptionData
from snuba.subscriptions.partitioner import TopicSubscriptionDataPartitioner
from tests.subscriptions import BaseSubscriptionTest
from snuba.utils.streams.topics import Topic


class TestBuildRequest(BaseSubscriptionTest):
    def test(self) -> None:
        data = SubscriptionData(
            123, [], [], timedelta(minutes=10), timedelta(minutes=1)
        )
        partitioner = TopicSubscriptionDataPartitioner(
            KafkaTopicSpec(Topic.EVENTS, "events-topic", partitions_number=64)
        )
        assert partitioner.build_partition_id(data) == 18
