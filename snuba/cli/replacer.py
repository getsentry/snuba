import logging
import signal
from typing import Optional, Sequence

import click

from snuba import settings
from snuba.datasets.factory import enforce_table_writer, get_dataset


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
    "--clickhouse-host",
    default=settings.CLICKHOUSE_HOST,
    help="Clickhouse server to write to.",
)
@click.option(
    "--clickhouse-port",
    default=settings.CLICKHOUSE_PORT,
    type=int,
    help="Clickhouse native port to write to.",
)
@click.option(
    "--dataset",
    "dataset_name",
    default="events",
    type=click.Choice(["events"]),
    help="The dataset to consume/run replacements for (currently only events supported)",
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
@click.option("--log-level", default=settings.LOG_LEVEL, help="Logging level to use.")
@click.option(
    "--dogstatsd-host",
    default=settings.DOGSTATSD_HOST,
    help="Host to send DogStatsD metrics to.",
)
@click.option(
    "--dogstatsd-port",
    default=settings.DOGSTATSD_PORT,
    type=int,
    help="Port to send DogStatsD metrics to.",
)
def replacer(
    *,
    replacements_topic: Optional[str],
    consumer_group: str,
    bootstrap_server: Sequence[str],
    clickhouse_host: str,
    clickhouse_port: int,
    dataset_name: str,
    max_batch_size: int,
    max_batch_time_ms: int,
    auto_offset_reset: str,
    queued_max_messages_kbytes: int,
    queued_min_messages: int,
    log_level: str,
    dogstatsd_host: str,
    dogstatsd_port: int,
) -> None:

    import sentry_sdk
    from snuba import util
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

    sentry_sdk.init(dsn=settings.SENTRY_DSN)
    dataset = get_dataset(dataset_name)

    logging.basicConfig(
        level=getattr(logging, log_level.upper()), format="%(asctime)s %(message)s"
    )

    stream_loader = enforce_table_writer(dataset).get_stream_loader()
    default_replacement_topic_spec = stream_loader.get_replacement_topic_spec()
    assert (
        default_replacement_topic_spec is not None
    ), f"Dataset {dataset} does not have a replacement topic."
    replacements_topic = replacements_topic or default_replacement_topic_spec.topic_name

    metrics = util.create_metrics(
        dogstatsd_host, dogstatsd_port, "snuba.replacer", tags={"group": consumer_group}
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
        host=clickhouse_host, port=clickhouse_port, client_settings=client_settings,
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
        worker=ReplacerWorker(clickhouse, dataset, metrics=metrics),
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
