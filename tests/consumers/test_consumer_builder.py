import pytest
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


@pytest.mark.parametrize("con_build", [consumer_builder, consumer_builder_with_opt])
def test_consumer_builder_non_optional_attributes(con_build) -> None:  # type: ignore
    # Ensures that the ConsumerBuilders are assigning a
    # not-None value to the required attributes

    # Depending on the attribute, we can verify this
    # to different degrees

    assert con_build.storage == get_writable_storage(test_storage_key)

    assert con_build.consumer_group == consumer_group_name

    assert isinstance(con_build.raw_topic, Topic)

    assert con_build.broker_config is not None

    assert con_build.producer_broker_config is not None

    assert isinstance(con_build.producer, Producer)

    assert isinstance(con_build.metrics, MetricsBackend)

    assert con_build.max_batch_size == 3
    assert con_build.max_batch_time_ms == 4
    assert con_build.auto_offset_reset == "earliest"
    assert con_build.queued_max_messages_kbytes == 1
    assert con_build.queued_min_messages == 2


@pytest.mark.parametrize("con_build", [consumer_builder, consumer_builder_with_opt])
def test_consumer_builder_optional_attributes(con_build) -> None:  # type: ignore
    # Ensures that the ConsumerBuilders are assigning
    # some value, None or not, to the optional attributes

    # In the case that optional Kafka topic overrides
    # are passed in, stronger checks are performed
    # in a separate test below

    consumer_builder.replacements_topic
    consumer_builder.commit_log_topic

    con_build.bootstrap_servers
    con_build.strict_offset_reset
    con_build.processes
    con_build.input_block_size
    con_build.output_block_size


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


# next tests should
# 1) create a Builder object
# 2) __build_streaming_strategy_factory should return a valid ProcessingStrategyFactory object
