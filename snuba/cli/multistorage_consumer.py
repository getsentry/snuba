import signal
from typing import Any, Optional, Sequence

import click
from arroyo import Topic, configure_metrics
from arroyo.backends.kafka import (
    KafkaConsumer,
    KafkaPayload,
    KafkaProducer,
    build_kafka_configuration,
    build_kafka_consumer_configuration,
)
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
from snuba.consumers.consumer_config import resolve_consumer_config
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.factory import (
    get_writable_storage,
    get_writable_storage_keys,
)
from snuba.datasets.storages.storage_key import StorageKey
from snuba.environment import setup_logging, setup_sentry
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.metrics_adapter import StreamMetricsAdapter


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

    consumer_config = resolve_consumer_config(
        storage_names=storage_names,
        raw_topic=raw_events_topic,
        commit_log_topic=commit_log_topic,
        replacements_topic=replacements_topic,
        bootstrap_servers=bootstrap_server,
        # Unlike the main consumer, commit log and replacement bootstrap servers are not
        # currently able to be specified independently in the multistorage consumer.
        commit_log_bootstrap_servers=[],
        replacement_bootstrap_servers=[],
        slice_id=slice_id,
        max_batch_size=max_batch_size,
        max_batch_time_ms=max_batch_time_ms,
        queued_max_messages_kbytes=queued_max_messages_kbytes,
        queued_min_messages=queued_min_messages,
    )

    storages = {
        key: get_writable_storage(key)
        for key in (getattr(StorageKey, name.upper()) for name in storage_names)
    }
    writable_storages = [*storages.values()]

    topic = Topic(consumer_config.raw_topic.physical_topic_name)

    replacements = (
        Topic(consumer_config.replacements_topic.physical_topic_name)
        if consumer_config.replacements_topic is not None
        else None
    )

    # XXX: This requires that all storages are associated with the same Kafka
    # cluster so that they can be consumed by the same consumer instance.
    # Unfortunately, we don't have the concept of independently configurable
    # Kafka clusters in settings, only consumer configurations that are
    # associated with storages and/or global default configurations. To avoid
    # implementing yet another method of configuring Kafka clusters, this just
    # piggybacks on the existing configuration method(s), with the assumption
    # that most deployments are going to be using the default configuration.
    storage_keys = [*storages.keys()]

    configuration = build_kafka_consumer_configuration(
        consumer_config.raw_topic.broker_config,
        group_id=consumer_group,
        auto_offset_reset=auto_offset_reset,
        strict_offset_reset=not no_strict_offset_reset,
        queued_max_messages_kbytes=queued_max_messages_kbytes,
        queued_min_messages=queued_min_messages,
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

    consumer = KafkaConsumer(configuration)

    if consumer_config.commit_log_topic is None:
        commit_log_config = None
    else:
        # XXX: This relies on the assumptions that a.) all storages are
        # located on the same Kafka cluster (validated above.)
        commit_log_producer = ConfluentKafkaProducer(
            build_kafka_configuration(consumer_config.commit_log_topic.broker_config)
        )

        commit_log_config = CommitLogConfig(
            commit_log_producer,
            Topic(consumer_config.commit_log_topic.physical_topic_name),
            consumer_group,
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

    if consumer_config.dlq_topic is not None:
        dlq_producer = KafkaProducer(
            build_kafka_configuration(consumer_config.dlq_topic.broker_config)
        )

        dlq_policy = DlqPolicy(
            KafkaDlqProducer(
                dlq_producer, Topic(consumer_config.dlq_topic.physical_topic_name)
            ),
            DlqLimit(
                max_invalid_ratio=0.01,
                max_consecutive_count=1000,
            ),
            None,
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
