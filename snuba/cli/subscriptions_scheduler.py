import logging
import signal
from typing import Any, Optional

import click
from arroyo import configure_metrics

from snuba import environment
from snuba.environment import setup_logging, setup_sentry
from snuba.subscriptions.scheduler_consumer import SchedulerBuilder
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.metrics_adapter import StreamMetricsAdapter

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--entity",
    "entity_name",
    required=True,
    type=click.Choice(["events", "transactions", "sessions"]),
    help="The entity to target",
)
@click.option(
    "--consumer-group",
    default="snuba-subscription-scheduler",
    help="Consumer group used for consuming the commit log topic.",
)
@click.option(
    "--followed-consumer-group",
    required=True,
    help="Name of the consumer group to follow",
)
@click.option(
    "--auto-offset-reset",
    default="error",
    type=click.Choice(["error", "earliest", "latest"]),
    help="Kafka consumer auto offset reset.",
)
@click.option("--schedule-ttl", type=int, default=60 * 5)
@click.option("--log-level", help="Logging level to use.")
@click.option("--delay-seconds", type=int)
@click.option(
    "--load-factor",
    type=int,
    default=1,
    help="Temporary option to simulate additional load. To be removed after testing.",
)
def subscriptions_scheduler(
    *,
    entity_name: str,
    consumer_group: str,
    followed_consumer_group: str,
    auto_offset_reset: str,
    schedule_ttl: int,
    log_level: Optional[str],
    delay_seconds: Optional[int],
    load_factor: int = 1
) -> None:
    """
    Currently the subscriptions scheduler just consumes the commit log and records
    the lag between the slowest and fastest partitions.
    """

    setup_logging(log_level)
    setup_sentry()

    metrics = MetricsWrapper(
        environment.metrics, "subscriptions.scheduler", tags={"entity": entity_name}
    )

    configure_metrics(StreamMetricsAdapter(metrics))

    builder = SchedulerBuilder(
        entity_name,
        consumer_group,
        followed_consumer_group,
        auto_offset_reset,
        delay_seconds,
        metrics,
        # TODO: Just for testing, should be removed before the scheduler is actually used
        load_factor,
    )

    processor = builder.build_consumer()

    def handler(signum: int, frame: Any) -> None:
        processor.signal_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    processor.run()
