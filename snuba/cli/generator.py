import itertools
from contextlib import closing
from random import Random
from typing import Optional, Sequence

import click

from snuba.datasets.events_generator import generate_insertion_event
from snuba.datasets.factory import DATASET_NAMES, enforce_table_writer, get_dataset
from snuba.utils.streams.producer import Producer
from snuba.utils.streams.kafka import KafkaPayload
from snuba.utils.streams.types import Topic

generators = {"events": generate_insertion_event}


def get_kafka_producer(
    dataset, bootstrap_servers: Sequence[str]
) -> Producer[KafkaPayload]:
    from snuba import settings
    from snuba.utils.streams.kafka import KafkaProducer

    if not bootstrap_servers:
        storage = dataset.get_writable_storage()
        storage_key = storage.get_storage_key().value
        bootstrap_servers = settings.DEFAULT_STORAGE_BROKERS.get(
            storage_key, settings.DEFAULT_BROKERS
        )

    return KafkaProducer(
        {
            "bootstrap.servers": ",".join(bootstrap_servers),
            "partitioner": "consistent",
            "message.max.bytes": 50000000,  # 50MB, default is 1MB
        },
    )


def get_file_producer(directory: str) -> Producer[KafkaPayload]:
    from snuba.utils.streams.file import FileProducer, kafka_payload_file_codec

    return FileProducer(directory, kafka_payload_file_codec)


@click.command()
@click.argument(
    "directory", type=click.Path(dir_okay=True, file_okay=False, exists=True)
)
@click.option(
    "--dataset",
    "dataset_name",
    default="events",
    type=click.Choice(DATASET_NAMES),
    help="The dataset to target",
)
# @click.option("--bootstrap-server", "bootstrap_servers", multiple=True)
@click.option("-c", "--count", type=int)
@click.option("-s", "--scale", type=int, default=1)
@click.option("--topic", "topic_name")
@click.option("--seed")
def generator(
    *,
    directory: str,
    dataset_name: str,
    count: Optional[int],
    scale: int,
    # bootstrap_servers: Sequence[str],
    topic_name: Optional[str],
    seed: Optional[str],
) -> None:
    dataset = get_dataset(dataset_name)

    generator = generators[dataset_name]
    random = Random(seed)

    topic = (
        Topic(topic_name)
        if topic_name is not None
        else Topic(
            enforce_table_writer(dataset)
            .get_stream_loader()
            .get_default_topic_spec()
            .topic_name
        )
    )

    producer = get_file_producer(directory)

    with closing(producer):
        iterator = itertools.count()
        if count:
            iterator = itertools.islice(iterator, count)

        for i in iterator:
            producer.produce(topic, generator(random, scale))
