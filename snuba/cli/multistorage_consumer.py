import logging
import signal
from contextlib import closing
from typing import Any, Optional, Sequence

import click
import rapidjson
from arroyo import Topic, configure_metrics
from arroyo.backends.kafka import KafkaConsumer, KafkaProducer
from arroyo.processing import StreamProcessor
from confluent_kafka import Producer as ConfluentKafkaProducer

from snuba import environment, settings
from snuba.consumers.consumer import MultistorageConsumerProcessingStrategyFactory
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import WRITABLE_STORAGES, get_writable_storage
from snuba.environment import setup_logging, setup_sentry
from snuba.state import get_config
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.configuration_builder import (
    build_kafka_consumer_configuration,
    build_kafka_producer_configuration,
)
from snuba.utils.streams.kafka_consumer_with_commit_log import (
    KafkaConsumerWithCommitLog,
)
from snuba.utils.streams.metrics_adapter import StreamMetricsAdapter
from snuba.utils.streams.topics import Topic as StreamsTopic

logger = logging.getLogger(__name__)


@click.command(hidden=True)
@click.option(
    "--storage",
    "storage_names",
    type=click.Choice([storage_key.value for storage_key in WRITABLE_STORAGES.keys()]),
    multiple=True,
    required=True,
)
@click.option(
    "--consumer-group",
    default="snuba-consumers",
)
@click.option(
    "--commit-log-topic",
    help="Topic for committed offsets to be written to, triggering post-processing task(s)",
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
@click.option(
    "--parallel-collect",
    is_flag=True,
    default=True,
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
    "--dead-letter-topic", help="Dead letter topic to send failed insert messages."
)
# TODO: For testing alternate rebalancing strategies. To be eventually removed.
@click.option(
    "--cooperative-rebalancing",
    is_flag=True,
    default=False,
    help="Use cooperative-sticky partition assignment strategy",
)
def multistorage_consumer(
    storage_names: Sequence[str],
    consumer_group: str,
    commit_log_topic: str,
    max_batch_size: int,
    max_batch_time_ms: int,
    auto_offset_reset: str,
    no_strict_offset_reset: bool,
    queued_max_messages_kbytes: int,
    queued_min_messages: int,
    parallel_collect: bool,
    processes: Optional[int],
    input_block_size: Optional[int],
    output_block_size: Optional[int],
    log_level: Optional[str] = None,
    dead_letter_topic: Optional[str] = None,
    cooperative_rebalancing: bool = False,
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

    topics = {
        storage.get_table_writer()
        .get_stream_loader()
        .get_default_topic_spec()
        .topic_name
        for storage in storages.values()
    }

    # XXX: The ``StreamProcessor`` only supports a single topic at this time,
    # but is easily modified. The topic routing in the processing strategy is a
    # bit trickier (but also shouldn't be too bad.)
    topic = Topic(topics.pop())
    if topics:
        raise ValueError("only one topic is supported")

    commit_log: Optional[Topic]
    if commit_log_topic:
        commit_log = Topic(commit_log_topic)
    else:
        # XXX: The ``CommitLogConsumer`` also only supports a single topic at this
        # time. (It is less easily modified.) This also assumes the commit log
        # topic is on the same Kafka cluster as the input topic.
        commit_log_topics = {
            spec.topic_name
            for spec in (
                storage.get_table_writer()
                .get_stream_loader()
                .get_commit_log_topic_spec()
                for storage in storages.values()
            )
            if spec is not None
        }

        if commit_log_topics:
            commit_log = Topic(commit_log_topics.pop())
        else:
            commit_log = None

        if commit_log_topics:
            raise ValueError("only one commit log topic is supported")

    # XXX: This requires that all storages are associated with the same Kafka
    # cluster so that they can be consumed by the same consumer instance.
    # Unfortunately, we don't have the concept of independently configurable
    # Kafka clusters in settings, only consumer configurations that are
    # associated with storages and/or global default configurations. To avoid
    # implementing yet another method of configuring Kafka clusters, this just
    # piggybacks on the existing configuration method(s), with the assumption
    # that most deployments are going to be using the default configuration.
    storage_keys = [*storages.keys()]

    kafka_topic = (
        storages[storage_keys[0]]
        .get_table_writer()
        .get_stream_loader()
        .get_default_topic_spec()
        .topic
    )

    consumer_configuration = build_kafka_consumer_configuration(
        kafka_topic,
        consumer_group,
        auto_offset_reset=auto_offset_reset,
        strict_offset_reset=not no_strict_offset_reset,
        queued_max_messages_kbytes=queued_max_messages_kbytes,
        queued_min_messages=queued_min_messages,
    )

    if cooperative_rebalancing is True:
        consumer_configuration["partition.assignment.strategy"] = "cooperative-sticky"

    for storage_key in storage_keys[1:]:
        if (
            build_kafka_consumer_configuration(
                storages[storage_key]
                .get_table_writer()
                .get_stream_loader()
                .get_default_topic_spec()
                .topic,
                consumer_group,
            )["bootstrap.servers"]
            != consumer_configuration["bootstrap.servers"]
        ):
            raise ValueError("storages cannot be located on different Kafka clusters")

    metrics = MetricsWrapper(
        environment.metrics,
        "consumer",
        tags={
            "group": consumer_group,
            "storage": "_".join([storage_keys[0].value, "m"]),
        },
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
    if commit_log is None:
        consumer = KafkaConsumer(consumer_configuration)
    else:
        # XXX: This relies on the assumptions that a.) all storages are
        # located on the same Kafka cluster (validated above.)

        commit_log_topic_spec = (
            storages[storage_keys[0]]
            .get_table_writer()
            .get_stream_loader()
            .get_commit_log_topic_spec()
        )
        assert commit_log_topic_spec is not None

        producer = ConfluentKafkaProducer(
            build_kafka_producer_configuration(commit_log_topic_spec.topic)
        )
        consumer = KafkaConsumerWithCommitLog(
            consumer_configuration,
            producer=producer,
            commit_log_topic=commit_log,
        )

    dead_letter_producer: Optional[KafkaProducer] = None
    dead_letter_queue: Optional[Topic] = None
    if dead_letter_topic:
        dead_letter_queue = Topic(dead_letter_topic)

        dead_letter_producer = KafkaProducer(
            build_kafka_producer_configuration(StreamsTopic(dead_letter_topic))
        )

    configure_metrics(StreamMetricsAdapter(metrics))
    processor = StreamProcessor(
        consumer,
        topic,
        MultistorageConsumerProcessingStrategyFactory(
            [*storages.values()],
            max_batch_size,
            max_batch_time_ms / 1000.0,
            parallel_collect=parallel_collect,
            processes=processes,
            input_block_size=input_block_size,
            output_block_size=output_block_size,
            metrics=metrics,
            producer=dead_letter_producer,
            topic=dead_letter_queue,
        ),
    )

    def handler(signum: int, frame: Any) -> None:
        processor.signal_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    if dead_letter_producer:
        with closing(dead_letter_producer):
            processor.run()
    else:
        processor.run()
