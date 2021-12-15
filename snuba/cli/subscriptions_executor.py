import signal
from typing import Any, Optional, Sequence

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
    "entity_names",
    required=True,
    multiple=True,
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
# This option allows us to reroute the produced subscription results to a different
# topic temporarily while we are testing and running the old and new subscription
# pipeline concurrently. Should be removed once we are done testing.
@click.option(
    "--override-result-topic", type=str, help="Override the result topic for testing"
)
def subscriptions_executor(
    *,
    dataset_name: str,
    entity_names: Sequence[str],
    consumer_group: str,
    max_concurrent_queries: int,
    auto_offset_reset: str,
    log_level: Optional[str],
    override_result_topic: Optional[str],
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
        environment.metrics,
        "subscriptions.executor",
        tags={"entity": ",".join(entity_names)},
    )

    configure_metrics(StreamMetricsAdapter(metrics))

    processor = build_executor_consumer(
        dataset_name,
        entity_names,
        consumer_group,
        max_concurrent_queries,
        auto_offset_reset,
        metrics,
        override_result_topic,
    )

    def handler(signum: int, frame: Any) -> None:
        processor.signal_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    processor.run()
