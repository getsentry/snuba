import signal
from typing import Any, Optional, Sequence

import click

from snuba import environment, settings
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.environment import setup_logging, setup_sentry
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.metrics_adapter import StreamMetricsAdapter


@click.command()
@click.option(
    "--replacements-topic",
    help="Topic to consume replacement messages from.",
)
@click.option(
    "--consumer-group",
    default="snuba-replacers",
    help="Consumer group use for consuming the replacements topic.",
)
@click.option(
    "--bootstrap-server",
    multiple=True,
    help="Kafka bootstrap server to use.",
)
@click.option(
    "--storage",
    "storage_name",
    type=click.Choice(["errors"]),
    help="The storage to consume/run replacements for (currently only events supported)",
    required=True,
)
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
    help="Minimum number of messages per topic+partition librdkafka tries to maintain in the local consumer queue.",
)
@click.option("--log-level", help="Logging level to use.")
def replacer(
    *,
    replacements_topic: Optional[str],
    consumer_group: str,
    bootstrap_server: Sequence[str],
    storage_name: str,
    auto_offset_reset: str,
    no_strict_offset_reset: bool,
    queued_max_messages_kbytes: int,
    queued_min_messages: int,
    log_level: Optional[str] = None,
) -> None:

    from arroyo import Topic, configure_metrics
    from arroyo.backends.kafka import KafkaConsumer
    from arroyo.commit import ONCE_PER_SECOND
    from arroyo.processing import StreamProcessor

    from snuba.replacer import ReplacerStrategyFactory, ReplacerWorker
    from snuba.utils.streams.configuration_builder import (
        build_kafka_consumer_configuration,
    )

    setup_logging(log_level)
    setup_sentry()

    storage_key = StorageKey(storage_name)
    storage = get_writable_storage(storage_key)
    metrics_tags = {"group": consumer_group, "storage": storage_name}

    stream_loader = storage.get_table_writer().get_stream_loader()
    default_replacement_topic_spec = stream_loader.get_replacement_topic_spec()
    assert (
        default_replacement_topic_spec is not None
    ), f"Storage {storage.get_storage_key().value} does not have a replacement topic."
    replacements_topic = replacements_topic or default_replacement_topic_spec.topic_name

    metrics = MetricsWrapper(environment.metrics, "replacer", tags=metrics_tags)

    configure_metrics(StreamMetricsAdapter(metrics))

    replacer = StreamProcessor(
        KafkaConsumer(
            build_kafka_consumer_configuration(
                default_replacement_topic_spec.topic,
                bootstrap_servers=bootstrap_server,
                group_id=consumer_group,
                auto_offset_reset=auto_offset_reset,
                strict_offset_reset=not no_strict_offset_reset,
                queued_max_messages_kbytes=queued_max_messages_kbytes,
                queued_min_messages=queued_min_messages,
            ),
        ),
        Topic(replacements_topic),
        ReplacerStrategyFactory(
            worker=ReplacerWorker(storage, consumer_group, metrics=metrics),
        ),
        ONCE_PER_SECOND,
    )

    def handler(signum: int, frame: Any) -> None:
        replacer.signal_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    replacer.run()
