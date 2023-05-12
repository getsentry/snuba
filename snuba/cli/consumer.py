import logging
import signal
from typing import Any, Optional, Sequence

import click
import rapidjson
import sentry_sdk
from arroyo import configure_metrics

from snuba import environment, settings
from snuba.consumers.consumer_builder import (
    ConsumerBuilder,
    KafkaParameters,
    ProcessingParameters,
)
from snuba.datasets.storages.factory import get_writable_storage_keys
from snuba.datasets.storages.storage_key import StorageKey
from snuba.environment import setup_logging, setup_sentry
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.metrics_adapter import StreamMetricsAdapter

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--storage",
    "storage_name",
    type=click.Choice(
        [storage_key.value for storage_key in get_writable_storage_keys()]
    ),
    help="The storage to target",
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
    help="Consumer group use for consuming the raw events topic.",
)
@click.option(
    "--bootstrap-server",
    multiple=True,
    help="Kafka bootstrap server to use for consuming.",
)
@click.option(
    "--commit-log-bootstrap-server",
    multiple=True,
    help="Kafka bootstrap server to use to produce the commit log.",
)
@click.option(
    "--replacement-bootstrap-server",
    multiple=True,
    help="Kafka bootstrap server to use to produce replacements.",
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
@click.option("--log-level", help="Logging level to use.")
@click.option(
    "--processes",
    type=int,
)
@click.option(
    "--input-block-size",
    type=int,
)
@click.option(
    "--output-block-size",
    type=int,
)
@click.option(
    "--profile-path", type=click.Path(dir_okay=True, file_okay=False, exists=True)
)
@click.option(
    "--max-poll-interval-ms",
    type=int,
)
def consumer(
    *,
    storage_name: str,
    raw_events_topic: Optional[str],
    replacements_topic: Optional[str],
    commit_log_topic: Optional[str],
    consumer_group: str,
    bootstrap_server: Sequence[str],
    commit_log_bootstrap_server: Sequence[str],
    replacement_bootstrap_server: Sequence[str],
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
    max_poll_interval_ms: Optional[int] = None,
) -> None:

    setup_logging(log_level)
    setup_sentry()
    logger.info("Consumer Starting")

    storage_key = StorageKey(storage_name)
    sentry_sdk.set_tag("storage", storage_name)

    metrics_tags = {
        "consumer_group": consumer_group,
        "storage": storage_key.value,
    }

    if slice_id:
        metrics_tags["slice_id"] = str(slice_id)

    metrics = MetricsWrapper(environment.metrics, "consumer", tags=metrics_tags)
    configure_metrics(StreamMetricsAdapter(metrics))

    def stats_callback(stats_json: str) -> None:
        stats = rapidjson.loads(stats_json)
        metrics.gauge("librdkafka.total_queue_size", stats.get("replyq", 0))

    consumer_builder = ConsumerBuilder(
        storage_key=storage_key,
        kafka_params=KafkaParameters(
            raw_topic=raw_events_topic,
            replacements_topic=replacements_topic,
            bootstrap_servers=bootstrap_server,
            commit_log_bootstrap_servers=commit_log_bootstrap_server,
            replacements_bootstrap_servers=replacement_bootstrap_server,
            group_id=consumer_group,
            commit_log_topic=commit_log_topic,
            auto_offset_reset=auto_offset_reset,
            strict_offset_reset=not no_strict_offset_reset,
            queued_max_messages_kbytes=queued_max_messages_kbytes,
            queued_min_messages=queued_min_messages,
        ),
        processing_params=ProcessingParameters(
            processes=processes,
            input_block_size=input_block_size,
            output_block_size=output_block_size,
        ),
        max_batch_size=max_batch_size,
        max_batch_time_ms=max_batch_time_ms,
        metrics=metrics,
        profile_path=profile_path,
        stats_callback=stats_callback,
        slice_id=slice_id,
        max_poll_interval_ms=max_poll_interval_ms,
    )

    consumer = consumer_builder.build_base_consumer()

    def handler(signum: int, frame: Any) -> None:
        consumer.signal_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    consumer.run()
    consumer_builder.flush()
