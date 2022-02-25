import logging
import signal
from typing import Any, Optional, Sequence

import click
from arroyo import Topic, configure_metrics
from arroyo.backends.kafka import KafkaConsumer
from arroyo.processing import StreamProcessor

from snuba import environment, settings
from snuba.consumers.dead_letter_consumer import (
    DeadLetterConsumerProcessingStrategyFactory,
)
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import WRITABLE_STORAGES, get_writable_storage
from snuba.environment import setup_logging, setup_sentry
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.configuration_builder import build_kafka_consumer_configuration
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
@click.option(
    "--auto-offset-reset",
    default="error",
    type=click.Choice(["error", "earliest", "latest"]),
    help="Kafka consumer auto offset reset.",
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
    "--dead-letter-topic", help="Dead letter topic to consume failed insert messages."
)
@click.option("--log-level")
def dead_letter_consumer(
    storage_names: Sequence[str],
    consumer_group: str,
    max_batch_size: int,
    max_batch_time_ms: int,
    auto_offset_reset: str,
    queued_max_messages_kbytes: int,
    queued_min_messages: int,
    dead_letter_topic: str,
    log_level: Optional[str] = None,
) -> None:

    setup_logging(log_level)
    setup_sentry()

    storages = {
        key: get_writable_storage(key)
        for key in (getattr(StorageKey, name.upper()) for name in storage_names)
    }

    topic = StreamsTopic(dead_letter_topic)

    consumer_configuration = build_kafka_consumer_configuration(
        topic,
        consumer_group,
        auto_offset_reset=auto_offset_reset,
        queued_max_messages_kbytes=queued_max_messages_kbytes,
        queued_min_messages=queued_min_messages,
    )

    consumer = KafkaConsumer(consumer_configuration)

    metrics = MetricsWrapper(
        environment.metrics, "dead-letter-consumer", tags={"group": consumer_group},
    )

    configure_metrics(StreamMetricsAdapter(metrics))
    processor = StreamProcessor(
        consumer,
        Topic(dead_letter_topic),
        DeadLetterConsumerProcessingStrategyFactory(
            [*storages.values()],
            max_batch_size,
            max_batch_time_ms / 1000.0,
            metrics=metrics,
        ),
    )

    def handler(signum: int, frame: Any) -> None:
        processor.signal_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    processor.run()
