import signal
from contextlib import contextmanager
from typing import Any, Iterator, Optional, Sequence

import click
import structlog
from arroyo import configure_metrics
from arroyo.backends.kafka import KafkaProducer

from snuba import environment, state
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_enabled_dataset_names
from snuba.environment import setup_logging, setup_sentry
from snuba.migrations.connect import check_clickhouse_connections
from snuba.subscriptions.combined_scheduler_executor import (
    build_scheduler_executor_consumer,
)
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.metrics_adapter import StreamMetricsAdapter


@click.command()
@click.option(
    "--dataset",
    "dataset_name",
    required=True,
    type=click.Choice(get_enabled_dataset_names()),
    help="The dataset to target.",
)
@click.option(
    "--entity",
    "entity_names",
    required=True,
    multiple=True,
    type=click.Choice([entity_key.value for entity_key in EntityKey]),
    help="The entity to target.",
)
@click.option(
    "--consumer-group",
    default="snuba-subscription-executor",
    help="Consumer group used for consuming the scheduled subscription topic/s.",
)
@click.option(
    "--followed-consumer-group",
    required=True,
    help="Name of the consumer group to follow",
)
@click.option(
    "--total-concurrent-queries",
    default=64,
    type=int,
    help="Total max number of concurrent queries for all replicas. Used to calculate max_concurrent_queries.",
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
@click.option("--schedule-ttl", type=int, default=60 * 5)
@click.option(
    "--delay-seconds", type=int, help="Deprecated. Will be removed in a follow up"
)
@click.option(
    "--stale-threshold-seconds",
    type=int,
    help="Skip scheduling if timestamp is beyond this threshold compared to the system time",
)
@click.option(
    "--health-check-file",
    default=None,
    type=str,
    help="Arroyo will touch this file at intervals to indicate health. If not provided, no health check is performed.",
)
@click.option("--log-level", help="Logging level to use.")
def subscriptions_scheduler_executor(
    *,
    dataset_name: str,
    entity_names: Sequence[str],
    consumer_group: str,
    followed_consumer_group: str,
    total_concurrent_queries: int,
    auto_offset_reset: str,
    no_strict_offset_reset: bool,
    schedule_ttl: int,
    delay_seconds: Optional[int],
    stale_threshold_seconds: Optional[int],
    health_check_file: Optional[str],
    log_level: Optional[str],
) -> None:
    """
    Combined subscriptions scheduler and executor. Alternative to the separate scheduler and executor processes.
    """
    setup_logging(log_level)
    setup_sentry()

    logger = structlog.get_logger().bind(module=__name__)

    metrics = MetricsWrapper(
        environment.metrics,
        "subscriptions.scheduler_executor",
        tags={"dataset": dataset_name},
    )

    configure_metrics(StreamMetricsAdapter(metrics))

    # Just get the result topic configuration from the first entity. Later we
    # check they all have the same result topic anyway before building the consumer.
    entity_key = EntityKey(entity_names[0])

    storage = get_entity(entity_key).get_writable_storage()
    assert storage is not None
    stream_loader = storage.get_table_writer().get_stream_loader()
    result_topic_spec = stream_loader.get_subscription_scheduled_topic_spec()
    assert result_topic_spec is not None

    producer = KafkaProducer(
        build_kafka_producer_configuration(
            result_topic_spec.topic,
            override_params={"partitioner": "consistent"},
        )
    )

    logger.info("Checking Clickhouse connections")
    cluster = storage.get_cluster()
    check_clickhouse_connections([cluster])

    processor = build_scheduler_executor_consumer(
        dataset_name,
        entity_names,
        consumer_group,
        followed_consumer_group,
        producer,
        auto_offset_reset,
        not no_strict_offset_reset,
        schedule_ttl,
        stale_threshold_seconds,
        total_concurrent_queries,
        metrics,
        health_check_file,
    )

    def handler(signum: int, frame: Any) -> None:
        processor.signal_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    with closing(producer), flush_querylog():
        processor.run()


@contextmanager
def closing(producer: KafkaProducer) -> Iterator[Optional[KafkaProducer]]:
    try:
        yield producer
    finally:
        producer.close().result()


@contextmanager
def flush_querylog() -> Iterator[None]:
    try:
        yield
    finally:
        state.flush_producer()
