import signal
from typing import Any

import click
from arroyo import configure_metrics

from snuba import environment
from snuba.environment import setup_logging, setup_sentry
from snuba.subscriptions.verifier import build_verifier
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.metrics_adapter import StreamMetricsAdapter


@click.command()
@click.option(
    "--consumer-group",
    default="snuba-subscription-verifier",
    help="Consumer group used for consuming the commit log topic.",
)
@click.option(
    "--orig-result-topic", required=True, type=str, help="Original result topic"
)
@click.option("--new-result-topic", required=True, type=str, help="New result topic")
@click.option(
    "--threshold-sec",
    default=30,
    type=int,
    help="Min seconds to wait for potentially delayed subscriptions",
)
@click.option(
    "--auto-offset-reset",
    default="error",
    type=click.Choice(["error", "earliest", "latest"]),
    help="Kafka consumer auto offset reset.",
)
def subscriptions_verifier(
    *,
    consumer_group: str,
    orig_result_topic: str,
    new_result_topic: str,
    threshold_sec: int,
    auto_offset_reset: str,
) -> None:
    """
    The subscriptions verifier compares two result topics and logs discrepancies to Sentry.
    It is intended as temporary code used to validate results while transitioning to the new
    subscriptions pipeline.
    """
    setup_logging()
    setup_sentry()
    metrics = MetricsWrapper(environment.metrics, "subscriptions.verifier",)

    configure_metrics(StreamMetricsAdapter(metrics))

    processor = build_verifier(
        orig_result_topic,
        new_result_topic,
        consumer_group,
        threshold_sec,
        auto_offset_reset,
        metrics,
    )

    def handler(signum: int, frame: Any) -> None:
        processor.signal_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    processor.run()
