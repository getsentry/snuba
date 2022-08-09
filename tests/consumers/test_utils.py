from unittest.mock import Mock, patch

import pytest
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, ClusterMetadata

from snuba import settings
from snuba.consumers.utils import TopicNotFound, get_partition_count
from snuba.datasets.entities import EntityKey, EntityKeys
from snuba.datasets.entities.factory import get_entity
from snuba.utils.manage_topics import create_topics
from snuba.utils.streams.configuration_builder import get_default_kafka_configuration
from snuba.utils.streams.topics import Topic


def test_get_partition_count() -> None:
    admin_client = AdminClient(get_default_kafka_configuration())
    create_topics(admin_client, [Topic.SUBSCRIPTION_SCHEDULED_TRANSACTIONS])

    entity = get_entity(EntityKey("transactions"))
    storage = entity.get_writable_storage()

    assert storage is not None
    stream_loader = storage.get_table_writer().get_stream_loader()

    scheduled_topic_spec = stream_loader.get_subscription_scheduled_topic_spec()
    assert scheduled_topic_spec is not None

    assert get_partition_count(scheduled_topic_spec.topic) == 1


@patch("snuba.consumers.utils.AdminClient.list_topics")
def test_topic_not_found(mock_fn: Mock) -> None:
    mock_fn.return_value = ClusterMetadata()
    with pytest.raises(TopicNotFound):
        get_partition_count(Topic.TRANSACTIONS)


def test_invalid_broker_config() -> None:
    settings.KAFKA_BROKER_CONFIG = {
        "transactions": {
            "bootstrap.servers": "invalid.broker:9092",
        }
    }
    with pytest.raises(KafkaException):
        assert get_partition_count(Topic.TRANSACTIONS, 0.2)
