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
    parallel_collect=False,
    slice_id=None,
)


# the point of these tests is to try out
# different configurations of builder
# and see if any initialization breaks


def test_consumer_builder_non_optional_attributes() -> None:
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
    try:
        consumer_builder.bootstrap_servers
    except NameError:
        assert (
            False
        ), "ConsumerBuilder's bootstrap_servers must either be defined as some value or None"

    try:
        consumer_builder_with_opt.bootstrap_servers
    except NameError:
        assert (
            False
        ), "ConsumerBuilder's bootstrap_servers must either be defined as some value or None"

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
        consumer_builder_with_opt.stats_callback
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
        consumer_builder_with_opt.strict_offset_reset
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
        consumer_builder_with_opt.processes
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
        consumer_builder_with_opt.input_block_size
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

    try:
        consumer_builder_with_opt.output_block_size
    except NameError:
        assert (
            False
        ), "ConsumerBuilder's output_block_size must either be defined to some value or None"


def test_optional_kafka_overrides() -> None:

    if optional_kafka_params.raw_topic is not None:
        assert (
            consumer_builder_with_opt.raw_topic.name == optional_kafka_params.raw_topic
        )

    if optional_kafka_params.replacements_topic is not None:
        assert consumer_builder_with_opt.replacements_topic is not None
        assert (
            consumer_builder_with_opt.replacements_topic.name
            == optional_kafka_params.replacements_topic
        )

    if optional_kafka_params.commit_log_topic is not None:
        assert consumer_builder_with_opt.commit_log_topic is not None
        assert (
            consumer_builder_with_opt.commit_log_topic.name
            == optional_kafka_params.commit_log_topic
        )
