import signal
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from typing import Sequence

import click
import logging

from snuba import settings
from snuba.datasets.factory import DATASET_NAMES, enforce_table_writer, get_dataset
from snuba.subscriptions.consumer import TickConsumer
from snuba.subscriptions.executor import SubscriptionExecutor
from snuba.subscriptions.worker import SubscriptionWorker
from snuba.util import create_metrics
from snuba.utils.codecs import PassthroughCodec
from snuba.utils.streams.batching import BatchingConsumer
from snuba.utils.streams.kafka import (
    KafkaConsumer,
    KafkaProducer,
    build_kafka_consumer_configuration,
)
from snuba.utils.streams.types import Topic


@click.command()
@click.option(
    "--dataset",
    "dataset_name",
    default="events",
    type=click.Choice(DATASET_NAMES),
    help="The dataset to target",
)
@click.option(
    "--consumer-group",
    default="snuba-subscriptions",
    help="Consumer group use for consuming the dataset source topic.",
)
@click.option(
    "--bootstrap-server",
    "bootstrap_servers",
    multiple=True,
    help="Kafka bootstrap server(s) to use.",
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
@click.option("--log-level", default=settings.LOG_LEVEL, help="Logging level to use.")
def subscriptions(
    *,
    dataset_name: str,
    consumer_group: str,
    bootstrap_servers: Sequence[str],
    max_batch_size: int,
    max_batch_time_ms: int,
    log_level: str,
) -> None:
    logging.basicConfig(level=getattr(logging, log_level.upper()))

    dataset = get_dataset(dataset_name)
    codec = None  # TODO

    with closing(
        KafkaProducer({"bootstrap.servers": ",".join(bootstrap_servers)}, codec)
    ) as producer, ThreadPoolExecutor(
        settings.SUBSCRIPTIONS_MAX_CONCURRENT_QUERIES
    ) as executor:
        consumer = BatchingConsumer(
            TickConsumer(
                KafkaConsumer(
                    build_kafka_consumer_configuration(
                        bootstrap_servers, consumer_group
                    ),
                    PassthroughCodec(),
                ),
            ),
            Topic(
                enforce_table_writer(dataset)
                .get_stream_loader()
                .get_default_topic_spec()
                .topic_name
            ),
            SubscriptionWorker(SubscriptionExecutor(dataset, executor), producer),
            max_batch_size,
            max_batch_time_ms,
            create_metrics(
                "snuba.subscriptions",
                tags={"group": consumer_group, "dataset": dataset_name},
            ),
        )

        def handler(signum, frame) -> None:
            consumer.signal_shutdown()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

        consumer.run()
