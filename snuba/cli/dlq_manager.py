import os
from typing import MutableSequence, Sequence

import click
from arroyo import Message, Topic
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload

from snuba.utils.streams.configuration_builder import build_kafka_consumer_configuration
from snuba.utils.streams.topics import Topic as StreamTopics


@click.group()
def dlq_manager() -> None:
    """
    Tools for Snuba's Dead Letter Queues
    """
    pass


@dlq_manager.command()
def list() -> None:
    """
    List all messages found in dead-letter topic.
    """
    messages = consume_dead_letters()
    line_break = "-" * 50
    click.echo("Messages found in Snuba dead-letter topic:")
    for message in messages:
        click.echo(line_break)
        click.echo(f"DLQ recieved at {message.timestamp}")
        click.echo(message.payload.value)
    click.echo(line_break)


def consume_dead_letters() -> Sequence[Message[KafkaPayload]]:
    bootstrap_servers = [os.environ.get("BOOTSTRAP_SERVERS") or "localhost:9092"]

    consumer = KafkaConsumer(
        build_kafka_consumer_configuration(
            topic=StreamTopics.DEAD_LETTER_TOPIC,
            bootstrap_servers=bootstrap_servers,
            group_id="dlq-manager-consumer",
            auto_offset_reset="earliest",
        )
    )
    consumer.subscribe([Topic(StreamTopics.DEAD_LETTER_TOPIC.value)])
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
