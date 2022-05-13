import os
from typing import MutableSequence, Optional, Sequence, Tuple

import click
from arroyo import Message, Partition, Topic
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload

from snuba.datasets.storages.factory import WRITABLE_STORAGES
from snuba.utils.streams.configuration_builder import build_kafka_consumer_configuration

__STORAGE_TO_DLQ_TOPIC = {
    storage: storage.get_table_writer()
    .get_stream_loader()
    .get_dead_letter_queue_topic()
    for storage in WRITABLE_STORAGES.values()
}

STORAGE_SETS_WITH_DLQ = {
    storage.get_storage_set_key().value: topic
    for storage, topic in __STORAGE_TO_DLQ_TOPIC.items()
    if topic is not None
}


@click.group()
def dlq_manager() -> None:
    """
    Tools for Snuba's Dead Letter Queues
    """
    pass


@dlq_manager.command()
@click.option(
    "--storage-set",
    "storage_set",
    type=click.Choice([storage_set for storage_set in STORAGE_SETS_WITH_DLQ]),
    help="The storage set to list dead letters for",
    required=True,
)
@click.option(
    "--offset",
    "offset",
    type=int,
    help="An offset on the dead-letter topic to start reading from, earliest by default",
    default=0,
)
@click.option(
    "--partition",
    "partition_index",
    type=int,
    help="The partition to read from, must be used with the --offset arg. See 'info' command.",
    default=0,
)
@click.option(
    "--limit",
    "limit",
    type=int,
    help="Max number of messages to display, 10 by default",
    default=10,
)
def list(storage_set: str, offset: int, partition_index: int, limit: int) -> None:
    """
    List all messages found in a dead-letter topic
    """
    click.echo("\nthis could take up to 10 seconds...\n")
    messages = _consume_dead_letters(storage_set, offset, partition_index, limit)
    line_break = "-" * 50
    if messages:
        click.echo(
            f"\nListing the first {limit} messages "
            f"{f'from offset {offset} on partition {partition_index}' if offset != 0 else ''}"
            f"in {storage_set} dead-letter topic:"
            f"\n(see --help for pagination)\n"
        )
        for message in messages:
            click.echo(line_break)
            click.echo(f"DLQ Offset: {message.offset}\n")
            click.echo(message.payload.value)
    else:
        click.echo(f"\nNo messages found in {storage_set} dead-letter topic!")
    click.echo(line_break)


def _consume_dead_letters(
    storage_set: str, offset: int, partition_index: int, limit: int
) -> Sequence[Message[KafkaPayload]]:
    consumer = _build_consumer(storage_set)
    message = consumer.poll(10)

    if message is None:
        return []

    messages: MutableSequence[Message[KafkaPayload]] = []

    if message.offset >= offset:
        messages.append(message)

    if offset != 0:
        try:
            offsets = consumer.tell()
            if offsets:
                part: Partition = [p for p in offsets][0]
                consumer.seek({Partition(part.topic, partition_index): offset})
        except Exception as e:
            click.echo(f"\nAn error occured: {e}")
            click.echo(
                "\nIf the partition is not assigned, see available partitions using:"
            )
            click.echo(f"snuba dlq-manager info --storage-set={storage_set}")
            consumer.close()
            return messages

    for _ in range(limit):
        try:
            message = consumer.poll(1)
            if message is None:
                consumer.close()
                return messages
            messages.append(message)
        except Exception as e:
            click.echo(f"\nAn error occured: {e}")
            click.echo(
                "\nIf the offset is out of range, see available min/max offset using:"
            )
            click.echo(f"snuba dlq-manager info --storage-set={storage_set}")
            consumer.close()
            return messages
    return messages


@dlq_manager.command()
@click.option(
    "--storage-set",
    "storage_set",
    type=click.Choice([storage_set for storage_set in STORAGE_SETS_WITH_DLQ]),
    help="The storage set to see info about dead-letter topic for",
    required=True,
)
def info(storage_set: str) -> None:
    """
    Display useful info for a dead-letter topic for a storage set
    """
    click.echo("\nthis could take up to 10 seconds...\n")
    consumer = _build_consumer(storage_set)
    earliest_offset, latest_offset = _get_offsets_info(consumer)
    dead_letter_topic_snuba = STORAGE_SETS_WITH_DLQ[storage_set]
    click.echo(f"\nDisplaying info for {storage_set} dead letter messages:\n")
    click.echo(f"Dead letter topic name: {dead_letter_topic_snuba.value}")
    if earliest_offset is None:
        click.echo(f"No messages on {dead_letter_topic_snuba.value}!")
    else:
        click.echo(f"Earliest offset: {earliest_offset}")
        click.echo(f"Latest offset: {latest_offset}\n")
        for partition, offset in consumer.tell().items():
            click.echo(
                f"Partition {partition.index}: Contains up to offset {offset - 1}"
            )
    consumer.close()


def _get_offsets_info(consumer: KafkaConsumer) -> Tuple[Optional[int], Optional[int]]:
    message = consumer.poll(10)
    earliest_offset = message.offset if message else None
    latest_offset = None
    while message is not None:
        latest_offset = message.offset
        message = consumer.poll(1)
    return earliest_offset, latest_offset


def _build_consumer(storage_set: str) -> KafkaConsumer:
    bootstrap_servers = [os.environ.get("BOOTSTRAP_SERVERS") or "localhost:9092"]
    dead_letter_topic_snuba = STORAGE_SETS_WITH_DLQ[storage_set]
    consumer = KafkaConsumer(
        build_kafka_consumer_configuration(
            topic=dead_letter_topic_snuba,
            bootstrap_servers=bootstrap_servers,
            group_id="dlq-manager-consumer",
            auto_offset_reset="earliest",
        )
    )
    dead_letter_topic_arroyo = Topic(dead_letter_topic_snuba.value)
    consumer.subscribe([dead_letter_topic_arroyo])
    return consumer
