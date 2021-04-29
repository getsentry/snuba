from datetime import timedelta

from snuba import settings
from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.subscriptions.data import SubscriptionData
from snuba.subscriptions.partitioner import TopicSubscriptionDataPartitioner
from tests.subscriptions import BaseSubscriptionTest
from snuba.utils.streams.topics import Topic


class TestBuildRequest(BaseSubscriptionTest):
    def test(self) -> None:
        settings.TOPIC_PARTITION_COUNTS = {"events": 64}
        data = SubscriptionData(
            123, [], [], timedelta(minutes=10), timedelta(minutes=1)
        )
        partitioner = TopicSubscriptionDataPartitioner(KafkaTopicSpec(Topic.EVENTS))
        assert partitioner.build_partition_id(data) == 18
