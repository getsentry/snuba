import signal
from typing import Any, Optional

import click
from arroyo import configure_metrics

from snuba import environment, settings
from snuba.consumers.consumer_builder import (
    ConsumerBuilder,
    KafkaParameters,
    MockParameters,
    ProcessingParameters,
)
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import WRITABLE_STORAGES
from snuba.environment import setup_logging, setup_sentry
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.metrics_adapter import StreamMetricsAdapter


@click.command()
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
    "--storage",
    "storage_name",
    type=click.Choice([storage_key.value for storage_key in WRITABLE_STORAGES.keys()]),
    help="The storage to target",
    required=True,
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
@click.option("--log-level", help="Logging level to use.")
@click.option(
    "--processes", type=int,
)
@click.option(
    "--input-block-size", type=int,
)
@click.option(
    "--output-block-size", type=int,
)
@click.option(
    "--profile-path", type=click.Path(dir_okay=True, file_okay=False, exists=True)
)
@click.option(
    "--avg-latency-ms",
    default=100,
    type=int,
    help="Average write latency the mock writer will apply",
)
@click.option(
    "--latency-std-deviation-ms",
    default=10,
    type=int,
    help="Std deviation to be applied to the latency",
)
def test_consumer(
    *,
    commit_log_topic: Optional[str],
    consumer_group: str,
    storage_name: str,
    max_batch_size: int,
    max_batch_time_ms: int,
    auto_offset_reset: str,
    queued_max_messages_kbytes: int,
    queued_min_messages: int,
    processes: Optional[int],
    input_block_size: Optional[int],
    output_block_size: Optional[int],
    avg_latency_ms: int,
    latency_std_deviation_ms: int,
    log_level: Optional[str] = None,
    profile_path: Optional[str] = None,
) -> None:

    setup_logging(log_level)
    setup_sentry()

    storage_key = StorageKey(storage_name)

    metrics = MetricsWrapper(
        environment.metrics,
        "test_consumer",
        tags={"group": consumer_group, "storage": storage_key.value},
    )
    configure_metrics(StreamMetricsAdapter(metrics))

    consumer_builder = ConsumerBuilder(
        storage_key=storage_key,
        kafka_params=KafkaParameters(
            raw_topic=None,
            replacements_topic=None,
            bootstrap_servers=None,
            group_id=consumer_group,
            commit_log_topic=commit_log_topic,
            auto_offset_reset=auto_offset_reset,
            queued_max_messages_kbytes=queued_max_messages_kbytes,
            queued_min_messages=queued_min_messages,
            stats_collection_frequency_ms=None,
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
        mock_parameters=MockParameters(
            avg_write_latency=avg_latency_ms, std_deviation=latency_std_deviation_ms,
        ),
    )

    consumer = consumer_builder.build_base_consumer()

    def handler(signum: int, frame: Any) -> None:
        consumer.signal_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    consumer.run()
