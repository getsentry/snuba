import logging
import signal
from typing import Any, Optional, Sequence

import click
import sentry_sdk
from arroyo import configure_metrics
from arroyo.backends.kafka import KafkaPayload
from arroyo.processing import StreamProcessor

from snuba import environment, settings
from snuba.consumers.consumer_builder import (
    ConsumerBuilder,
    KafkaParameters,
    ProcessingParameters,
)
from snuba.consumers.consumer_config import resolve_consumer_config
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.environment import setup_logging, setup_sentry
from snuba.lw_deletions.formatters import STORAGE_FORMATTER
from snuba.lw_deletions.strategy import ConsumerStrategyFactory
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.metrics_adapter import StreamMetricsAdapter
from snuba.web.bulk_delete_query import STORAGE_TOPIC

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--consumer-group",
    help="Consumer group use for consuming the deletion topic.",
    default="lw-deletions-consumer",
    required=True,
)
@click.option(
    "--bootstrap-server",
    multiple=True,
    help="Kafka bootstrap server to use for consuming.",
)
@click.option("--storage-name", help="Storage name to consume from", required=True)
@click.option(
    "--auto-offset-reset",
    default="earliest",
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
    help="Minimum number of messages per topic+partition the local consumer queue should contain before messages are sent to kafka.",
)
@click.option("--log-level", help="Logging level to use.")
def lw_deletions_consumer(
    *,
    consumer_group: str,
    bootstrap_server: Sequence[str],
    storage_name: str,
    max_batch_size: int,
    max_batch_time_ms: int,
    auto_offset_reset: str,
    no_strict_offset_reset: bool,
    queued_max_messages_kbytes: int,
    queued_min_messages: int,
    log_level: str,
) -> None:
    setup_logging(log_level)
    setup_sentry()

    logger.info("Consumer Starting")

    sentry_sdk.set_tag("storage", storage_name)
    shutdown_requested = False
    consumer: Optional[StreamProcessor[KafkaPayload]] = None

    def handler(signum: int, frame: Any) -> None:
        nonlocal shutdown_requested
        shutdown_requested = True

        if consumer is not None:
            consumer.signal_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    topic = STORAGE_TOPIC[storage_name]

    while not shutdown_requested:
        metrics_tags = {
            "consumer_group": consumer_group,
            "storage": storage_name,
        }
        metrics = MetricsWrapper(
            environment.metrics, "lw_deletions_consumer", tags=metrics_tags
        )
        configure_metrics(StreamMetricsAdapter(metrics), force=True)
        consumer_config = resolve_consumer_config(
            storage_names=[storage_name],
            raw_topic=topic.value,
            commit_log_topic=None,
            replacements_topic=None,
            bootstrap_servers=bootstrap_server,
            commit_log_bootstrap_servers=[],
            replacement_bootstrap_servers=[],
            slice_id=None,
            max_batch_size=max_batch_size,
            max_batch_time_ms=max_batch_time_ms,
            group_instance_id=consumer_group,
        )

        consumer_builder = ConsumerBuilder(
            consumer_config=consumer_config,
            kafka_params=KafkaParameters(
                group_id=consumer_group,
                auto_offset_reset=auto_offset_reset,
                strict_offset_reset=not no_strict_offset_reset,
                queued_max_messages_kbytes=queued_max_messages_kbytes,
                queued_min_messages=queued_min_messages,
            ),
            processing_params=ProcessingParameters(None, None, None),
            max_batch_size=max_batch_size,
            max_batch_time_ms=max_batch_time_ms,
            max_insert_batch_size=0,
            max_insert_batch_time_ms=0,
            metrics=metrics,
            slice_id=None,
            join_timeout=None,
            enforce_schema=False,
            metrics_tags=metrics_tags,
        )

        storage = get_writable_storage(StorageKey(storage_name))
        formatter = STORAGE_FORMATTER[storage_name]()
        strategy_factory = ConsumerStrategyFactory(
            max_batch_size=max_batch_size,
            max_batch_time_ms=max_batch_time_ms,
            storage=storage,
            formatter=formatter,
        )

        consumer = consumer_builder.build_lw_deletions_consumer(strategy_factory)

        consumer.run()
        consumer_builder.flush()
