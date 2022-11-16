from arroyo import Topic
from confluent_kafka import Producer

from snuba import environment
from snuba.consumers.consumer_builder import (
    ConsumerBuilder,
    KafkaParameters,
    ProcessingParameters,
)
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.wrapper import MetricsWrapper

consumer_builder = ConsumerBuilder(
    storage_key=StorageKey("errors"),
    kafka_params=KafkaParameters(
        raw_topic=None,
        replacements_topic=None,
        bootstrap_servers=None,
        group_id="my_consumer_group",
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
        tags={"group": "my_consumer_group", "storage": StorageKey("errors").value},
    ),
    parallel_collect=False,
    slice_id=None,
)


def test_consumer_non_optional_attributes() -> None:
    try:
        consumer_builder.storage
    except NameError:
        assert False, "ConsumerBuilder must have storage defined"
    assert isinstance(consumer_builder.storage, WritableTableStorage)

    try:
        consumer_builder.consumer_group
    except NameError:
        assert False, "ConsumerBuilder must have consumer group defined"
    assert isinstance(consumer_builder.consumer_group, str)

    try:
        consumer_builder.raw_topic
    except NameError:
        assert False, "ConsumerBuilder must have a raw/default topic defined"
    assert isinstance(consumer_builder.raw_topic, Topic)

    try:
        consumer_builder.broker_config
    except NameError:
        assert False, "ConsumerBuilder must have a default broker config defined"
    # assert isinstance(consumer_builder.broker_config, KafkaBrokerConfig)

    try:
        consumer_builder.producer_broker_config
    except NameError:
        assert (
            False
        ), "ConsumerBuilder must have a broker config defined for producer(s)"
    # assert isinstance(consumer_builder.producer_broker_config, KafkaBrokerConfig)

    try:
        consumer_builder.producer
    except NameError:
        assert False, "ConsumerBuilder must have a default producer defined"
    assert isinstance(consumer_builder.producer, Producer)

    try:
        consumer_builder.metrics
    except NameError:
        assert False, "ConsumerBuilder must have metrics defined"
    assert isinstance(consumer_builder.metrics, MetricsBackend)

    assert consumer_builder.max_batch_size == 3
    assert consumer_builder.max_batch_time_ms == 4
    assert consumer_builder.auto_offset_reset == "earliest"
    assert consumer_builder.queued_max_messages_kbytes == 1
    assert consumer_builder.queued_min_messages == 2


# def test_consumer_optional_attributes() -> None:
