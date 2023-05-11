import json
import time
from datetime import datetime
from unittest.mock import Mock

import pytest
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import BrokerValue, Message, Partition, Topic

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
from tests.fixtures import get_raw_event
from tests.test_consumer import get_row_count

test_storage_key = StorageKey("errors")
consumer_group_name = "my_consumer_group"

# Below, a ConsumerBuilder with only required args
consumer_builder = ConsumerBuilder(
    storage_key=test_storage_key,
    kafka_params=KafkaParameters(
        raw_topic=None,
        replacements_topic=None,
        bootstrap_servers=None,
        commit_log_bootstrap_servers=None,
        replacements_bootstrap_servers=None,
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
    replacements_topic="event-replacements",
    bootstrap_servers=None,
    commit_log_bootstrap_servers=None,
    replacements_bootstrap_servers=None,
    group_id=consumer_group_name,
    commit_log_topic="snuba-commit-log",
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

    con_build.replacements_topic
    con_build.commit_log_topic

    con_build.replacements_producer
    con_build.commit_log_producer

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


@pytest.mark.clickhouse_db
def test_run_processing_strategy() -> None:
    assert get_row_count(get_writable_storage(StorageKey.ERRORS)) == 0

    commit = Mock()
    partitions = Mock()
    strategy_factory = consumer_builder.build_streaming_strategy_factory()
    strategy = strategy_factory.create_with_partitions(commit, partitions)

    raw_message = get_raw_event()
    json_string = json.dumps([2, "insert", raw_message, []])

    message = Message(
        BrokerValue(
            KafkaPayload(None, json_string.encode("utf-8"), []),
            Partition(Topic("events"), 0),
            0,
            datetime.now(),
        )
    )

    strategy.submit(message)

    # Wait for the commit
    for i in range(10):
        time.sleep(0.5)
        strategy.poll()
        if commit.call_count == 1:
            break

    assert commit.call_count == 1
    assert get_row_count(get_writable_storage(StorageKey.ERRORS)) == 1

    strategy.close()
    strategy.join()
