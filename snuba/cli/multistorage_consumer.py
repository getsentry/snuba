import logging
import signal
from dataclasses import dataclass
from typing import Any, Optional, Sequence

import click
import rapidjson
from arroyo import Topic, configure_metrics
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload, KafkaProducer
from arroyo.commit import IMMEDIATE
from arroyo.dlq import DlqLimit, DlqPolicy, KafkaDlqProducer
from arroyo.processing import StreamProcessor
from arroyo.processing.strategies import ProcessingStrategyFactory
from arroyo.utils.profiler import ProcessingStrategyProfilerWrapperFactory
from confluent_kafka import Producer as ConfluentKafkaProducer

from snuba import environment, settings
from snuba.consumers.consumer import (
    CommitLogConfig,
    MultistorageConsumerProcessingStrategyFactory,
)
from snuba.datasets.configuration.utils import DlqConfig
from snuba.datasets.slicing import validate_passed_slice
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.factory import (
    get_writable_storage,
    get_writable_storage_keys,
)
from snuba.datasets.storages.storage_key import StorageKey
from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.environment import setup_logging, setup_sentry
from snuba.state import get_config
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.configuration_builder import (
    build_kafka_consumer_configuration,
    build_kafka_producer_configuration,
)
from snuba.utils.streams.metrics_adapter import StreamMetricsAdapter
from snuba.utils.streams.topics import Topic as SnubaTopic

logger = logging.getLogger(__name__)


@click.command(hidden=True)
@click.option(
    "--storage",
    "storage_names",
    type=click.Choice(
        [storage_key.value for storage_key in get_writable_storage_keys()]
    ),
    multiple=True,
    required=True,
)
@click.option("--raw-events-topic", help="Topic to consume raw events from.")
@click.option(
    "--replacements-topic",
    help="Topic to produce replacement messages info.",
)
@click.option(
    "--commit-log-topic",
    help="Topic for committed offsets to be written to, triggering post-processing task(s)",
)
@click.option(
    "--consumer-group",
    default="snuba-consumers",
)
@click.option(
    "--bootstrap-server",
    multiple=True,
    help="Kafka bootstrap server to use.",
)
@click.option(
    "--slice-id",
    "slice_id",
    type=int,
    help="The slice id for the storage",
)
@click.option(
    "--max-batch-size",
    default=settings.DEFAULT_MAX_BATCH_SIZE,
    type=int,
    help="Max number of messages to batch in memory before writing to Kafka.",
)
@click.option(
    "--max-batch-time-ms",
    default=settings.DEFAULT_MAX_BATCH_TIME_MS,
    type=int,
    help="Max length of time to buffer messages in memory before writing to Kafka.",
)
@click.option(
    "--auto-offset-reset",
    default="error",
    type=click.Choice(["error", "earliest", "latest"]),
    help="Kafka consumer auto offset reset.",
)
@click.option(
    "--no-strict-offset-reset",
    is_flag=True,
    help="Forces the kafka consumer auto offset reset.",
)
@click.option(
    "--queued-max-messages-kbytes",
    default=settings.DEFAULT_QUEUED_MAX_MESSAGE_KBYTES,
    type=int,
    help="Maximum number of kilobytes per topic+partition in the local consumer queue.",
)
@click.option(
    "--queued-min-messages",
    default=settings.DEFAULT_QUEUED_MIN_MESSAGES,
    type=int,
    help="Minimum number of messages per topic+partition librdkafka tries to maintain in the local consumer queue.",
)
@click.option("--processes", type=int)
@click.option(
    "--input-block-size",
    type=int,
)
@click.option(
    "--output-block-size",
    type=int,
)
@click.option("--log-level")
@click.option(
    "--profile-path", type=click.Path(dir_okay=True, file_okay=False, exists=True)
)
def multistorage_consumer(
    storage_names: Sequence[str],
    raw_events_topic: Optional[str],
    replacements_topic: Optional[str],
    commit_log_topic: Optional[str],
    consumer_group: str,
    bootstrap_server: Sequence[str],
    slice_id: Optional[int],
    max_batch_size: int,
    max_batch_time_ms: int,
    auto_offset_reset: str,
    no_strict_offset_reset: bool,
    queued_max_messages_kbytes: int,
    queued_min_messages: int,
    processes: Optional[int],
    input_block_size: Optional[int],
    output_block_size: Optional[int],
    log_level: Optional[str] = None,
    profile_path: Optional[str] = None,
) -> None:

    DEFAULT_BLOCK_SIZE = int(32 * 1e6)

    if processes is not None:
        if input_block_size is None:
            input_block_size = DEFAULT_BLOCK_SIZE

        if output_block_size is None:
            output_block_size = DEFAULT_BLOCK_SIZE

    setup_logging(log_level)
    setup_sentry()

    logger.info("Consumer Starting")
    storages = {
        key: get_writable_storage(key)
        for key in (getattr(StorageKey, name.upper()) for name in storage_names)
    }
    writable_storages = [*storages.values()]

    for storage in writable_storages:
        validate_passed_slice(storage.get_storage_set_key(), slice_id)

    consumer_config = get_consumer_config(writable_storages, slice_id)

    if raw_events_topic:
        topic = Topic(raw_events_topic)
    else:
        topic = consumer_config.logical_raw_topic

    commit_log: Optional[Topic]
    if commit_log_topic:
        commit_log = Topic(commit_log_topic)
    else:
        # XXX: The ``CommitLogConsumer`` also only supports a single topic at this
        # time. (It is less easily modified.) This also assumes the commit log
        # topic is on the same Kafka cluster as the input topic.
        commit_log = consumer_config.logical_commit_log_topic

    replacements: Optional[Topic]
    if replacements_topic:
        replacements = Topic(replacements_topic)
    else:
        replacements = consumer_config.logical_replacements_topic

    # XXX: This requires that all storages are associated with the same Kafka
    # cluster so that they can be consumed by the same consumer instance.
    # Unfortunately, we don't have the concept of independently configurable
    # Kafka clusters in settings, only consumer configurations that are
    # associated with storages and/or global default configurations. To avoid
    # implementing yet another method of configuring Kafka clusters, this just
    # piggybacks on the existing configuration method(s), with the assumption
    # that most deployments are going to be using the default configuration.
    storage_keys = [*storages.keys()]

    consumer_configuration = build_kafka_consumer_configuration(
        SnubaTopic(consumer_config.logical_raw_topic.name),
        consumer_group,
        slice_id,
        auto_offset_reset=auto_offset_reset,
        strict_offset_reset=not no_strict_offset_reset,
        queued_max_messages_kbytes=queued_max_messages_kbytes,
        queued_min_messages=queued_min_messages,
        bootstrap_servers=bootstrap_server,
    )

    metrics_tags = {
        "group": consumer_group,
        "storage": "_".join([storage_keys[0].value, "m"]),
    }

    if slice_id:
        metrics_tags["slice_id"] = str(slice_id)

    metrics = MetricsWrapper(
        environment.metrics,
        "consumer",
        tags=metrics_tags,
    )
    # Collect metrics from librdkafka if we have stats_collection_freq_ms set
    # for the consumer group, or use the default.
    stats_collection_frequency_ms = get_config(
        f"stats_collection_freq_ms_{consumer_group}",
        get_config("stats_collection_freq_ms", 0),
    )

    if stats_collection_frequency_ms and stats_collection_frequency_ms > 0:

        def stats_callback(stats_json: str) -> None:
            stats = rapidjson.loads(stats_json)
            metrics.gauge("librdkafka.total_queue_size", stats.get("replyq", 0))

        consumer_configuration.update(
            {
                "statistics.interval.ms": stats_collection_frequency_ms,
                "stats_cb": stats_callback,
            }
        )

    consumer = KafkaConsumer(consumer_configuration)

    if commit_log is None:
        commit_log_config = None
    else:
        # XXX: This relies on the assumptions that a.) all storages are
        # located on the same Kafka cluster (validated above.)

        assert consumer_config.logical_commit_log_topic is not None

        commit_log_producer = ConfluentKafkaProducer(
            build_kafka_producer_configuration(
                SnubaTopic(consumer_config.logical_commit_log_topic.name)
            )
        )

        commit_log_config = CommitLogConfig(
            commit_log_producer, commit_log, consumer_group
        )

    strategy_factory = build_multistorage_streaming_strategy_factory(
        writable_storages,
        max_batch_size,
        max_batch_time_ms,
        processes,
        input_block_size,
        output_block_size,
        metrics,
        commit_log_config,
        replacements,
        slice_id,
        profile_path,
    )

    configure_metrics(StreamMetricsAdapter(metrics))

    dlq_config = consumer_config.dlq_config

    if dlq_config is not None:
        dlq_producer = KafkaProducer(
            build_kafka_producer_configuration(
                dlq_config.topic,
                slice_id,
            )
        )
        dlq_topic_spec = KafkaTopicSpec(dlq_config.topic)
        resolved_topic = Topic(dlq_topic_spec.get_physical_topic_name(slice_id))

        dlq_policy = DlqPolicy(
            KafkaDlqProducer(dlq_producer, resolved_topic), DlqLimit(), None
        )
    else:
        dlq_policy = None

    processor = StreamProcessor(
        consumer, topic, strategy_factory, IMMEDIATE, dlq_policy=dlq_policy
    )

    def handler(signum: int, frame: Any) -> None:
        processor.signal_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    processor.run()


@dataclass(frozen=True)
class ConsumerConfig:
    logical_raw_topic: Topic
    logical_commit_log_topic: Optional[Topic]
    logical_replacements_topic: Optional[Topic]
    dlq_config: Optional[DlqConfig]


def get_consumer_config(
    storages: Sequence[WritableTableStorage], slice_id: Optional[int]
) -> ConsumerConfig:
    stream_loaders = {
        storage.get_table_writer().get_stream_loader() for storage in storages
    }

    default_topics = {
        stream_loader.get_default_topic_spec().get_physical_topic_name(slice_id)
        for stream_loader in stream_loaders
    }

    commit_log_topics = {
        spec.get_physical_topic_name(slice_id)
        for spec in (
            stream_loader.get_commit_log_topic_spec()
            for stream_loader in stream_loaders
        )
        if spec is not None
    }

    replacement_topics = {
        spec.get_physical_topic_name(slice_id)
        for spec in (
            stream_loader.get_replacement_topic_spec()
            for stream_loader in stream_loaders
        )
        if spec is not None
    }

    dlq_configs = {stream_loader.get_dlq_config() for stream_loader in stream_loaders}

    # XXX: The ``StreamProcessor`` only supports a single topic at this time,
    # but is easily modified. The topic routing in the processing strategy is a
    # bit trickier (but also shouldn't be too bad.)
    topic = Topic(default_topics.pop())
    if default_topics:
        raise ValueError("only one topic is supported")

    if commit_log_topics:
        commit_log_topic = Topic(commit_log_topics.pop())
    else:
        commit_log_topic = None
    if commit_log_topics:
        raise ValueError("only one commit log topic is supported")

    if replacement_topics:
        replacement_topic = Topic(replacement_topics.pop())
    else:
        replacement_topic = None
    if replacement_topics:
        raise ValueError("only one replacement topic is supported")

    # Only one dead letter policy is supported. All storages must share the same
    # dead letter policy creator
    dlq_config = dlq_configs.pop()
    if dlq_config:
        raise ValueError("only one dead letter policy is supported")

    return ConsumerConfig(topic, commit_log_topic, replacement_topic, dlq_config)


def build_multistorage_streaming_strategy_factory(
    storages: Sequence[WritableTableStorage],
    max_batch_size: int,
    max_batch_time_ms: int,
    processes: Optional[int],
    input_block_size: Optional[int],
    output_block_size: Optional[int],
    metrics: MetricsBackend,
    commit_log_config: Optional[CommitLogConfig],
    replacements: Optional[Topic],
    slice_id: Optional[int],
    profile_path: Optional[str],
) -> ProcessingStrategyFactory[KafkaPayload]:

    strategy_factory: ProcessingStrategyFactory[
        KafkaPayload
    ] = MultistorageConsumerProcessingStrategyFactory(
        storages,
        max_batch_size,
        max_batch_time_ms / 1000.0,
        processes=processes,
        input_block_size=input_block_size,
        output_block_size=output_block_size,
        metrics=metrics,
        slice_id=slice_id,
        commit_log_config=commit_log_config,
        replacements=replacements,
    )

    if profile_path is not None:
        strategy_factory = ProcessingStrategyProfilerWrapperFactory(
            strategy_factory,
            profile_path,
        )

    return strategy_factory
