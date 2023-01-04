from arroyo import Topic
from confluent_kafka import Producer

from snuba import environment
from snuba.consumers.consumer_builder import (
    ConsumerBuilder,
    KafkaParameters,
    ProcessingParameters,
)
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.wrapper import MetricsWrapper

test_storage_key = StorageKey("errors")
consumer_group_name = "my_consumer_group"

# Below, a ConsumerBuilder with only required args
consumer_builder = ConsumerBuilder(
    storage_key=test_storage_key,
    kafka_params=KafkaParameters(
        raw_topic=None,
        replacements_topic=None,
        bootstrap_servers=None,
        group_id=consumer_group_name,
        commit_log_topic=None,
        auto_offset_reset="earliest",
        strict_offset_reset=None,
        queued_max_messages_kbytes=1,
        queued_min_messages=2,
    ),
    processing_params=ProcessingParameters(
        processes=None,
        input_block_size=None,
        output_block_size=None,
    ),
    max_batch_size=3,
    max_batch_time_ms=4,
    metrics=MetricsWrapper(
        environment.metrics,
        "test_consumer",
        tags={"group": consumer_group_name, "storage": test_storage_key.value},
    ),
    slice_id=None,
)

optional_kafka_params = KafkaParameters(
    raw_topic="raw",
    replacements_topic="replacements",
    bootstrap_servers=["cli.server:9092", "cli2.server:9092"],
    group_id=consumer_group_name,
    commit_log_topic="commit_log",
    auto_offset_reset="earliest",
    strict_offset_reset=False,
    queued_max_messages_kbytes=1,
    queued_min_messages=2,
)

# Below, a ConsumerBuilder with all required
# and optional args, but only those with
# no default values
consumer_builder_with_opt = ConsumerBuilder(
    storage_key=test_storage_key,
    kafka_params=optional_kafka_params,
    processing_params=ProcessingParameters(
        processes=5,
        input_block_size=6,
        output_block_size=7,
    ),
    max_batch_size=3,
    max_batch_time_ms=4,
    metrics=MetricsWrapper(
        environment.metrics,
        "test_consumer",
        tags={"group": consumer_group_name, "storage": test_storage_key.value},
    ),
    slice_id=None,
)


def test_consumer_builder_non_optional_attributes() -> None:
    # Ensures that the ConsumerBuilders are assigning a
    # not-None value to the required attributes

    # Depending on the attribute, we can verify this
    # to different degrees

    assert consumer_builder.storage == get_writable_storage(test_storage_key)
    assert consumer_builder_with_opt.storage == get_writable_storage(test_storage_key)

    assert consumer_builder.consumer_group == consumer_group_name
    assert consumer_builder_with_opt.consumer_group == consumer_group_name

    assert isinstance(consumer_builder.raw_topic, Topic)
    assert isinstance(consumer_builder_with_opt.raw_topic, Topic)

    assert consumer_builder.broker_config is not None
    assert consumer_builder_with_opt.broker_config is not None

    assert consumer_builder.producer_broker_config is not None
    assert consumer_builder_with_opt.producer_broker_config is not None

    assert isinstance(consumer_builder.producer, Producer)
    assert isinstance(consumer_builder_with_opt.producer, Producer)

    assert isinstance(consumer_builder.metrics, MetricsBackend)
    assert isinstance(consumer_builder_with_opt.metrics, MetricsBackend)

    assert consumer_builder.max_batch_size == 3
    assert consumer_builder.max_batch_time_ms == 4
    assert consumer_builder.auto_offset_reset == "earliest"
    assert consumer_builder.queued_max_messages_kbytes == 1
    assert consumer_builder.queued_min_messages == 2

    assert consumer_builder_with_opt.max_batch_size == 3
    assert consumer_builder_with_opt.max_batch_time_ms == 4
    assert consumer_builder_with_opt.auto_offset_reset == "earliest"
    assert consumer_builder_with_opt.queued_max_messages_kbytes == 1
    assert consumer_builder_with_opt.queued_min_messages == 2


def test_consumer_builder_optional_attributes() -> None:
    # Ensures that the ConsumerBuilders are assigning
    # some value, None or not, to the optional attributes

    # In the case that optional Kafka topic overrides
    # are passed in, stronger checks are performed
    # in a separate test below

    consumer_builder.bootstrap_servers
    consumer_builder_with_opt.bootstrap_servers

    consumer_builder.replacements_topic
    consumer_builder.commit_log_topic

    consumer_builder.strict_offset_reset
    consumer_builder_with_opt.strict_offset_reset

    consumer_builder.processes
    consumer_builder_with_opt.processes

    consumer_builder.input_block_size
    consumer_builder_with_opt.input_block_size

    consumer_builder.output_block_size
    consumer_builder_with_opt.output_block_size


def test_optional_kafka_overrides() -> None:

    # In the case that Kafka topic overrides are provided,
    # verify that these attributes are populated
    # as expected

    if optional_kafka_params.raw_topic is not None:
        assert (
            consumer_builder_with_opt.raw_topic.name == optional_kafka_params.raw_topic
        ), "Raw topic name should match raw Kafka topic override"

    if optional_kafka_params.replacements_topic is not None:
        assert consumer_builder_with_opt.replacements_topic is not None
        assert (
            consumer_builder_with_opt.replacements_topic.name
            == optional_kafka_params.replacements_topic
        ), "Replacements topic name should match replacements Kafka topic override"

    if optional_kafka_params.commit_log_topic is not None:
        assert consumer_builder_with_opt.commit_log_topic is not None
        assert (
            consumer_builder_with_opt.commit_log_topic.name
            == optional_kafka_params.commit_log_topic
        ), "Commit log topic name should match commit log Kafka topic override"
