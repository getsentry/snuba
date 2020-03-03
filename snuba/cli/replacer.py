import signal
from typing import Optional, Sequence

import click

from snuba import environment, settings
from snuba.datasets.factory import get_dataset
from snuba.datasets.storages.factory import get_writable_storage
from snuba.environment import setup_logging, setup_sentry
from snuba.utils.metrics.backends.wrapper import MetricsWrapper


@click.command()
@click.option(
    "--replacements-topic", help="Topic to consume replacement messages from.",
)
@click.option(
    "--consumer-group",
    default="snuba-replacers",
    help="Consumer group use for consuming the replacements topic.",
)
@click.option(
    "--bootstrap-server",
    default=settings.DEFAULT_BROKERS,
    multiple=True,
    help="Kafka bootstrap server to use.",
)
@click.option(
    "--dataset",
    "dataset_name",
    type=click.Choice(["events", "events_migration"]),
    help="The dataset to consume/run replacements for (currently only events supported)",
)
@click.option(
    "--storage",
    "storage_name",
    default="events",
    type=click.Choice(["events", "errors"]),
    help="The storage to consume/run replacements for (currently only events supported)",
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
    "--optimize",
    is_flag=True,
    default=False,
    help="Run optimize after each replacement.",
)
def replacer(
    *,
    replacements_topic: Optional[str],
    consumer_group: str,
    bootstrap_server: Sequence[str],
    dataset_name: Optional[str],
    storage_name: str,
    max_batch_size: int,
    max_batch_time_ms: int,
    auto_offset_reset: str,
    queued_max_messages_kbytes: int,
    queued_min_messages: int,
    log_level: Optional[str] = None,
    optimize: bool,
) -> None:

    from snuba.clickhouse.native import ClickhousePool
    from snuba.replacer import ReplacerWorker
    from snuba.utils.codecs import PassthroughCodec
    from snuba.utils.streams.batching import BatchingConsumer
    from snuba.utils.streams.kafka import (
        KafkaConsumer,
        KafkaPayload,
        TransportError,
        build_kafka_consumer_configuration,
    )
    from snuba.utils.streams.types import Topic

    setup_logging(log_level)
    setup_sentry()

    storage = get_writable_storage(storage_name)
    metrics_tags = {"group": consumer_group, "storage": storage_name}

    # If dataset_name is provided, use the writable storage from that dataset.
    # This can be removed once we are passing storage_name instead of
    # dataset_name everywhere
    if dataset_name:
        dataset = get_dataset(dataset_name)
        storage = dataset.get_writable_storage()
        metrics_tags = {"group": consumer_group, "dataset": dataset_name}

    stream_loader = storage.get_table_writer().get_stream_loader()
    default_replacement_topic_spec = stream_loader.get_replacement_topic_spec()
    assert (
        default_replacement_topic_spec is not None
    ), f"Storage {type(storage)} does not have a replacement topic."
    replacements_topic = replacements_topic or default_replacement_topic_spec.topic_name

    metrics = MetricsWrapper(
        environment.metrics,
        "replacer",
        tags=metrics_tags,
    )

    client_settings = {
        # Replacing existing rows requires reconstructing the entire tuple for each
        # event (via a SELECT), which is a Hard Thing (TM) for columnstores to do. With
        # the default settings it's common for ClickHouse to go over the default max_memory_usage
        # of 10GB per query. Lowering the max_block_size reduces memory usage, and increasing the
        # max_memory_usage gives the query more breathing room.
        "max_block_size": settings.REPLACER_MAX_BLOCK_SIZE,
        "max_memory_usage": settings.REPLACER_MAX_MEMORY_USAGE,
        # Don't use up production cache for the count() queries.
        "use_uncompressed_cache": 0,
    }

    clickhouse = ClickhousePool(
        settings.CLICKHOUSE_HOST,
        settings.CLICKHOUSE_PORT,
        client_settings=client_settings,
    )

    codec: PassthroughCodec[KafkaPayload] = PassthroughCodec()
    replacer = BatchingConsumer(
        KafkaConsumer(
            build_kafka_consumer_configuration(
                bootstrap_servers=bootstrap_server,
                group_id=consumer_group,
                auto_offset_reset=auto_offset_reset,
                queued_max_messages_kbytes=queued_max_messages_kbytes,
                queued_min_messages=queued_min_messages,
            ),
            codec=codec,
        ),
        Topic(replacements_topic),
        worker=ReplacerWorker(clickhouse, storage, metrics=metrics, optimize=optimize),
        max_batch_size=max_batch_size,
        max_batch_time=max_batch_time_ms,
        metrics=metrics,
        recoverable_errors=[TransportError],
    )

    def handler(signum, frame) -> None:
        replacer.signal_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    replacer.run()
