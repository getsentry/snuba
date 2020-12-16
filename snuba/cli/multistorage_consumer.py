import logging
import signal
from typing import Any, Optional, Sequence

from confluent_kafka import Producer as ConfluentKafkaProducer

import click
from snuba import environment, settings
from snuba.consumer import MultistorageConsumerProcessingStrategyFactory
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import WRITABLE_STORAGES, get_writable_storage
from snuba.environment import setup_logging, setup_sentry
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.backends.kafka import (
    KafkaConsumer,
    KafkaConsumerWithCommitLog,
    build_kafka_consumer_configuration,
    build_kafka_producer_configuration,
)
from snuba.utils.streams.processing import StreamProcessor
from snuba.utils.streams.types import Topic


logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--storage",
    "storage_names",
    default=["events"],
    type=click.Choice([storage_key.value for storage_key in WRITABLE_STORAGES.keys()]),
    multiple=True,
)
@click.option(
    "--consumer-group", default="snuba-consumers",
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
@click.option("--log-level")
def multistorage_consumer(
    storage_names: Sequence[str],
    consumer_group: str,
    max_batch_size: int,
    max_batch_time_ms: int,
    log_level: Optional[str] = None,
) -> None:

    setup_logging(log_level)
    setup_sentry()

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
        raise NotImplementedError("only one topic is supported")

    # XXX: The ``CommitLogConsumer`` also only supports a single topic at this
    # time. (It is less easily modified.) This also assumes the commit log
    # topic is on the same Kafka cluster as the input topic.
    commit_log_topics = {
        spec.topic_name
        for spec in (
            storage.get_table_writer().get_stream_loader().get_commit_log_topic_spec()
            for storage in storages.values()
        )
        if spec is not None
    }

    commit_log_topic: Optional[Topic]
    if commit_log_topics:
        commit_log_topic = Topic(commit_log_topics.pop())
    else:
        commit_log_topic = None

    if commit_log_topics:
        raise NotImplementedError("only one commit log topic is supported")

    # XXX: This requires that all storages are associated with the same Kafka
    # cluster so that they can be consumed by the same consumer instance.
    # Unfortunately, we don't have the concept of independently configurable
    # Kafka clusters in settings, only consumer configurations that are
    # associated with storages and/or global default configurations. To avoid
    # implementing yet another method of configuring Kafka clusters, this just
    # piggybacks on the existing configuration method(s), with the assumption
    # that most deployments are going to be using the default configuration.
    storage_keys = [*storages.keys()]
    # TODO: Patch in ither configurations from command line arguments.
    consumer_configuration = build_kafka_consumer_configuration(
        storage_keys[0], consumer_group,
    )
    for storage_key in storage_keys[1:]:
        pass  # TODO: actually check configuration equality

    if commit_log_topic is None:
        consumer = KafkaConsumer(consumer_configuration)
    else:
        producer = ConfluentKafkaProducer(
            build_kafka_producer_configuration(storage_keys[0])
        )
        consumer = KafkaConsumerWithCommitLog(
            consumer_configuration,
            producer=producer,
            commit_log_topic=commit_log_topic,
        )

    metrics = MetricsWrapper(environment.metrics, "consumer")
    processor = StreamProcessor(
        consumer,
        topic,
        MultistorageConsumerProcessingStrategyFactory(
            [*storages.values()], max_batch_size, max_batch_time_ms / 1000.0, metrics,
        ),
        metrics=metrics,
    )

    def handler(signum: int, frame: Any) -> None:
        processor.signal_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    processor.run()
