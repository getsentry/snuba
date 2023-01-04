import pytest
from confluent_kafka.admin import AdminClient

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.subscriptions.data import SubscriptionData
from snuba.subscriptions.partitioner import TopicSubscriptionDataPartitioner
from snuba.utils.manage_topics import recreate_topics
from snuba.utils.streams.configuration_builder import get_default_kafka_configuration
from snuba.utils.streams.topics import Topic
from tests.subscriptions import BaseSubscriptionTest

TESTS = [
    pytest.param(
        SubscriptionData(
            project_id=123,
            query="MATCH (events) SELECT count() AS count WHERE platform IN tuple('a')",
            time_window_sec=10 * 60,
            resolution_sec=60,
            entity=get_entity(EntityKey.EVENTS),
            metadata={},
        ),
        id="Legacy subscription",
    ),
    pytest.param(
        SubscriptionData(
            project_id=123,
            query=(
                "MATCH (events) "
                "SELECT count() AS count BY time "
                "WHERE "
                "platform IN tuple('a') "
            ),
            time_window_sec=10 * 60,
            resolution_sec=60,
            entity=get_entity(EntityKey.EVENTS),
            metadata={},
        ),
        id="SnQL subscription",
    ),
]


class TestBuildRequest(BaseSubscriptionTest):
    @pytest.mark.parametrize("subscription", TESTS)
    def test(self, subscription: SubscriptionData) -> None:
        admin_client = AdminClient(get_default_kafka_configuration())
        recreate_topics(admin_client, [Topic.EVENTS], num_partitions=64)
        partitioner = TopicSubscriptionDataPartitioner(KafkaTopicSpec(Topic.EVENTS))
        assert partitioner.build_partition_id(subscription) == 18
        recreate_topics(admin_client, [Topic.EVENTS])
