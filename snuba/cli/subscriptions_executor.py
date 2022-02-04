import signal
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from typing import Any, Optional, Sequence

import click
from arroyo import configure_metrics
from arroyo.backends.kafka import KafkaProducer

from snuba import environment
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.environment import setup_logging, setup_sentry
from snuba.subscriptions.executor_consumer import build_executor_consumer
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.metrics_adapter import StreamMetricsAdapter


@click.command()
@click.option(
    "--dataset",
    "dataset_name",
    required=True,
    type=click.Choice(["events", "transactions", "sessions", "metrics"]),
    help="The dataset to target.",
)
@click.option(
    "--entity",
    "entity_names",
    required=True,
    multiple=True,
    type=click.Choice(
        ["events", "transactions", "sessions", "metrics_counters", "metrics_sets"]
    ),
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

    # TODO: Hardcoded for testing so we have no chance of accidentally writing to the real
    # result topic. This should eventually come from stream_loader.get_result_topic_spec()
    if override_result_topic is None:
        override_result_topic = "snuba-subscription-result-test"

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
            result_topic_spec.topic, override_params={"partitioner": "consistent"},
        )
    )

    executor = ThreadPoolExecutor(max_concurrent_queries)

    processor = build_executor_consumer(
        dataset_name,
        entity_names,
        consumer_group,
        producer,
        max_concurrent_queries,
        auto_offset_reset,
        metrics,
        executor,
        override_result_topic,
    )

    def handler(signum: int, frame: Any) -> None:
        processor.signal_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    with closing(producer), executor:
        processor.run()
