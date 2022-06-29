import pytest
from confluent_kafka import KafkaException

from snuba import settings
from snuba.consumers.utils import get_partition_count
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.utils.streams.topics import Topic


def test_get_partition_count() -> None:
    entity = get_entity(EntityKey("events"))
    storage = entity.get_writable_storage()

    assert storage is not None
    stream_loader = storage.get_table_writer().get_stream_loader()

    scheduled_topic_spec = stream_loader.get_subscription_scheduled_topic_spec()
    assert scheduled_topic_spec is not None

    assert get_partition_count(scheduled_topic_spec.topic) == 1


def test_invalid_broker_config() -> None:
    settings.KAFKA_BROKER_CONFIG = {
        "transactions": {
            "bootstrap.servers": "invalid.broker:9092",
        }
    }
    with pytest.raises(KafkaException):
        assert get_partition_count(Topic.TRANSACTIONS, 0.2)
