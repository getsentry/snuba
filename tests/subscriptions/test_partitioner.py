from datetime import timedelta

from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.subscriptions.data import SubscriptionData
from snuba.subscriptions.partitioner import TopicSubscriptionDataPartitioner
from tests.subscriptions import BaseSubscriptionTest


class TestBuildRequest(BaseSubscriptionTest):
    def test(self):
        data = SubscriptionData(
            123, [], [], timedelta(minutes=10), timedelta(minutes=1)
        )
        assert (
            TopicSubscriptionDataPartitioner(
                KafkaTopicSpec("topic", partitions_number=64)
            ).build_partition_id(data)
            == 18
        )
