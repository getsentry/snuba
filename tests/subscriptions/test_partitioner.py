from datetime import timedelta

from snuba import settings
from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.subscriptions.data import LegacySubscriptionData
from snuba.subscriptions.partitioner import TopicSubscriptionDataPartitioner
from snuba.utils.streams.topics import Topic
from tests.subscriptions import BaseSubscriptionTest


class TestBuildRequest(BaseSubscriptionTest):
    def test(self) -> None:
        settings.TOPIC_PARTITION_COUNTS = {"events": 64}
        data = LegacySubscriptionData(
            project_id=123,
            conditions=[],
            aggregations=[],
            time_window=timedelta(minutes=10),
            resolution=timedelta(minutes=1),
        )
        partitioner = TopicSubscriptionDataPartitioner(KafkaTopicSpec(Topic.EVENTS))

        assert partitioner.build_partition_id(data) == 18
