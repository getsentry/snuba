from confluent_kafka.admin import AdminClient

from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.settings import SLICED_KAFKA_TOPIC_MAP
from snuba.utils.manage_topics import create_topics
from snuba.utils.streams.configuration_builder import get_default_kafka_configuration
from snuba.utils.streams.topics import Topic
from snuba.utils.streams.topics import Topic as SnubaTopic


def test_get_physical_topic_name(monkeypatch) -> None:  # type: ignore

    monkeypatch.setitem(
        SLICED_KAFKA_TOPIC_MAP, ("ingest-replay-events", 2), "ingest-replay-events-2"
    )

    storage_key = StorageKey.REPLAYS
    storage = get_writable_storage(storage_key)

    stream_loader = storage.get_table_writer().get_stream_loader()

    default_topic_spec = stream_loader.get_default_topic_spec()

    physical_topic_name = default_topic_spec.get_physical_topic_name(slice_id=2)

    assert physical_topic_name == "ingest-replay-events-2"


def test_partitions_number() -> None:
    admin_client = AdminClient(get_default_kafka_configuration())
    create_topics(admin_client, [SnubaTopic.REPLAYEVENTS])

    topic_spec = KafkaTopicSpec(Topic.REPLAYEVENTS)
    assert topic_spec.partitions_number == 1


def test_topic_current_config_values() -> None:
    admin_client = AdminClient(get_default_kafka_configuration())
    create_topics(admin_client, [SnubaTopic.REPLAYEVENTS])

    topic_spec = KafkaTopicSpec(Topic.REPLAYEVENTS)
    config = topic_spec.topic_current_config_values

    assert config["message.timestamp.type"] in ("CreateTime", "LogAppendTime")
