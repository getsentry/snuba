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
from snuba.consumers.consumer_config import resolve_consumer_config
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.wrapper import MetricsWrapper
from tests.fixtures import get_raw_error_message
from tests.test_consumer import get_row_count

test_storage_key = StorageKey("errors")
consumer_group_name = "my_consumer_group"

consumer_config = resolve_consumer_config(
    storage_names=[test_storage_key.value],
    raw_topic=None,
    commit_log_topic=None,
    replacements_topic=None,
    bootstrap_servers=[],
    commit_log_bootstrap_servers=[],
    replacement_bootstrap_servers=[],
    slice_id=None,
    max_batch_size=3,
    max_batch_time_ms=4,
)

# Below, a ConsumerBuilder with only required args
consumer_builder = ConsumerBuilder(
    consumer_config=consumer_config,
    kafka_params=KafkaParameters(
        group_id=consumer_group_name,
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
    max_insert_batch_size=None,
    max_insert_batch_time_ms=None,
    metrics=MetricsWrapper(
        environment.metrics,
        "test_consumer",
        tags={"group": consumer_group_name, "storage": test_storage_key.value},
    ),
    slice_id=None,
    join_timeout=5,
    enforce_schema=True,
    metrics_tags={},
)

optional_consumer_config = resolve_consumer_config(
    storage_names=[test_storage_key.value],
    raw_topic="raw",
    commit_log_topic="snuba-commit-log",
    replacements_topic="event-replacements",
    bootstrap_servers=[],
    commit_log_bootstrap_servers=[],
    replacement_bootstrap_servers=[],
    slice_id=None,
    max_batch_size=3,
    max_batch_time_ms=4,
)

optional_kafka_params = KafkaParameters(
    group_id=consumer_group_name,
    auto_offset_reset="earliest",
    strict_offset_reset=False,
    queued_max_messages_kbytes=1,
    queued_min_messages=2,
)

# Below, a ConsumerBuilder with all required
# and optional args, but only those with
# no default values
consumer_builder_with_opt = ConsumerBuilder(
    consumer_config=optional_consumer_config,
    kafka_params=optional_kafka_params,
    processing_params=ProcessingParameters(
        processes=5,
        input_block_size=6,
        output_block_size=7,
    ),
    max_batch_size=3,
    max_batch_time_ms=4,
    max_insert_batch_size=None,
    max_insert_batch_time_ms=None,
    metrics=MetricsWrapper(
        environment.metrics,
        "test_consumer",
        tags={"group": consumer_group_name, "storage": test_storage_key.value},
    ),
    slice_id=None,
    join_timeout=5,
    enforce_schema=True,
    metrics_tags={},
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


@pytest.mark.clickhouse_db
def test_run_processing_strategy() -> None:
    assert get_row_count(get_writable_storage(StorageKey.ERRORS)) == 0

    commit = Mock()
    partitions = {}
    strategy_factory = consumer_builder.build_streaming_strategy_factory()
    strategy = strategy_factory.create_with_partitions(commit, partitions)

    json_string = json.dumps(get_raw_error_message())

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

    assert get_row_count(get_writable_storage(StorageKey.ERRORS)) == 1

    strategy.close()
    strategy.join()
