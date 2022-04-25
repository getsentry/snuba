import os
from typing import MutableSequence, Sequence

import click
from arroyo import Message, Topic
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload

from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.factory import STORAGES_WITH_DLQ
from snuba.utils.streams.configuration_builder import build_kafka_consumer_configuration


@click.group()
def dlq_manager() -> None:
    """
    Tools for Snuba's Dead Letter Queues
    """
    pass


@dlq_manager.command()
@click.option(
    "--storage",
    "storage_set",
    type=click.Choice(
        [storage_set_key.value for storage_set_key in STORAGES_WITH_DLQ.keys()]
    ),
    help="The storage set to list dead letters for",
    required=True,
)
def list(storage_set: str) -> None:
    """
    List all messages found in dead-letter topic.
    """
    messages = consume_dead_letters(storage_set)
    line_break = "-" * 50
    if messages:
        click.echo(f"Messages found in {storage_set} dead-letter topic:")
        for message in messages:
            click.echo(line_break)
            click.echo(message.payload.value)
    else:
        click.echo(f"No messages found in {storage_set} dead-letter-topic!")
    click.echo(line_break)


def consume_dead_letters(storage_set: str) -> Sequence[Message[KafkaPayload]]:
    bootstrap_servers = [os.environ.get("BOOTSTRAP_SERVERS") or "localhost:9092"]

    dead_letter_topic = STORAGES_WITH_DLQ[StorageSetKey(storage_set)]

    consumer = KafkaConsumer(
        build_kafka_consumer_configuration(
            topic=dead_letter_topic,
            bootstrap_servers=bootstrap_servers,
            group_id="dlq-manager-consumer",
            auto_offset_reset="earliest",
        )
    )
    consumer.subscribe([Topic(dead_letter_topic.value)])
    messages: MutableSequence[Message[KafkaPayload]] = []
    while True:
        try:
            message = consumer.poll(10)
            if message is None:
                consumer.close()
                return messages
            messages.append(message)
        except Exception as e:
            click.echo(f"An error occured: {e}")
            consumer.close()
            return messages
