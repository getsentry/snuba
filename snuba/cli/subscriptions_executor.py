import signal
from typing import Any, Optional

import click
from arroyo import configure_metrics

from snuba import environment
from snuba.environment import setup_logging, setup_sentry
from snuba.subscriptions.executor_consumer import build_executor_consumer
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.metrics_adapter import StreamMetricsAdapter


@click.command()
@click.option(
    "--dataset",
    "dataset_name",
    required=True,
    type=click.Choice(["events", "transactions", "sessions"]),
    help="The dataset to target.",
)
@click.option(
    "--entity",
    "entity_name",
    required=True,
    type=click.Choice(["events", "transactions", "sessions"]),
    help="The entity to target.",
)
@click.option(
    "--consumer-group",
    default="snuba-subscription-executor",
    help="Consumer group used for consuming the scheduled subscription topic/s.",
)
@click.option(
    "--max-concurrent-queries",
    default=20,
    type=int,
    help="Max concurrent ClickHouse queries",
)
@click.option(
    "--auto-offset-reset",
    default="error",
    type=click.Choice(["error", "earliest", "latest"]),
    help="Kafka consumer auto offset reset.",
)
@click.option("--log-level", help="Logging level to use.")
def subscriptions_executor(
    *,
    dataset_name: str,
    entity_name: str,
    consumer_group: str,
    max_concurrent_queries: int,
    auto_offset_reset: str,
    log_level: Optional[str]
) -> None:
    """
    The subscription's executor consumes scheduled subscriptions from the scheduled
    subscription topic for that entity, executes the queries on ClickHouse and publishes
    results on the results topic.

    It currently only supports executing subscriptions on a single dataset/entity.
    """
    setup_logging(log_level)
    setup_sentry()

    metrics = MetricsWrapper(
        environment.metrics, "subscriptions.executor", tags={"entity": entity_name}
    )

    configure_metrics(StreamMetricsAdapter(metrics))

    processor = build_executor_consumer(
        dataset_name,
        entity_name,
        consumer_group,
        max_concurrent_queries,
        auto_offset_reset,
        metrics,
    )

    def handler(signum: int, frame: Any) -> None:
        processor.signal_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    processor.run()
