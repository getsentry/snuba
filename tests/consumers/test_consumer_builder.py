from arroyo import Topic
from confluent_kafka import Producer

from snuba import environment
from snuba.consumers.consumer_builder import (
    ConsumerBuilder,
    KafkaParameters,
    ProcessingParameters,
)
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.wrapper import MetricsWrapper

test_storage_key = StorageKey("errors")
consumer_group_name = "my_consumer_group"

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
    parallel_collect=False,
    slice_id=None,
)

# the point of these tests is to try out
# different configurations of builder
# and see if any initialization breaks


def test_consumer_non_optional_attributes() -> None:
    assert consumer_builder.storage is not None
    assert isinstance(consumer_builder.storage, WritableTableStorage)
    assert consumer_builder.storage == get_writable_storage(test_storage_key)

    assert consumer_builder.consumer_group is not None
    assert isinstance(consumer_builder.consumer_group, str)
    assert consumer_builder.consumer_group == consumer_group_name

    assert consumer_builder.raw_topic is not None
    assert isinstance(consumer_builder.raw_topic, Topic)

    assert consumer_builder.broker_config is not None

    assert consumer_builder.producer_broker_config is not None

    assert consumer_builder.producer is not None
    assert isinstance(consumer_builder.producer, Producer)

    assert consumer_builder.metrics is not None
    assert isinstance(consumer_builder.metrics, MetricsBackend)

    assert consumer_builder.max_batch_size == 3
    assert consumer_builder.max_batch_time_ms == 4
    assert consumer_builder.auto_offset_reset == "earliest"
    assert consumer_builder.queued_max_messages_kbytes == 1
    assert consumer_builder.queued_min_messages == 2


def test_consumer_optional_attributes() -> None:
    try:
        consumer_builder.bootstrap_servers
    except NameError:
        assert (
            False
        ), "ConsumerBuilder's bootstrap_servers must either be defined to some value or None"

    try:
        consumer_builder.replacements_topic
    except NameError:
        assert (
            False
        ), "ConsumerBuilder's replacements_topic must either be defined to some value or None"

    try:
        consumer_builder.commit_log_topic
    except NameError:
        assert (
            False
        ), "ConsumerBuilder's commit_log_topic must either be defined to some value or None"

    try:
        consumer_builder.stats_callback
    except NameError:
        assert (
            False
        ), "ConsumerBuilder's stats_callback must either be defined to some value or None"

    try:
        consumer_builder.strict_offset_reset
    except NameError:
        assert (
            False
        ), "ConsumerBuilder's strict_offset_reset must either be defined to some value or None"

    try:
        consumer_builder.processes
    except NameError:
        assert (
            False
        ), "ConsumerBuilder's processes must either be defined to some value or None"

    try:
        consumer_builder.input_block_size
    except NameError:
        assert (
            False
        ), "ConsumerBuilder's input_block_size must either be defined to some value or None"

    try:
        consumer_builder.output_block_size
    except NameError:
        assert (
            False
        ), "ConsumerBuilder's output_block_size must either be defined to some value or None"
