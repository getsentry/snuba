import itertools
from contextlib import closing
from datetime import datetime
from typing import Iterator

from arroyo import Message, Partition, Topic
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload, KafkaProducer
from arroyo.backends.kafka.commit import CommitCodec
from arroyo.commit import Commit
from arroyo.types import Position

from snuba.utils.streams.configuration_builder import get_default_kafka_configuration
from snuba.utils.streams.kafka_consumer_with_commit_log import (
    KafkaConsumerWithCommitLog,
)
from tests.backends.confluent_kafka import FakeConfluentKafkaProducer


def get_payloads() -> Iterator[KafkaPayload]:
    for i in itertools.count():
        yield KafkaPayload(None, f"{i}".encode("utf8"), [])


def test_commit_log_consumer() -> None:
    # XXX: This would be better as an integration test (or at least a test
    # against an abstract Producer interface) instead of against a test against
    # a mock.
    commit_log_producer = FakeConfluentKafkaProducer()

    configuration = get_default_kafka_configuration()

    consumer: KafkaConsumer = KafkaConsumerWithCommitLog(
        {
            **configuration,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "enable.auto.offset.store": "false",
            "enable.partition.eof": "true",
            "group.id": "test",
            "session.timeout.ms": 10000,
        },
        producer=commit_log_producer,
        commit_log_topic=Topic("commit-log"),
    )

    producer = KafkaProducer(configuration)

    topic = Topic("topic")

    with closing(consumer) as consumer:
        with closing(producer) as producer:
            producer.produce(topic, next(get_payloads())).result(5.0)

        consumer.subscribe([topic])

        message = consumer.poll(10.0)  # XXX: getting the subscription is slow
        assert isinstance(message, Message)

        now = datetime.now()

        position = Position(message.next_offset, now)

        consumer.stage_positions({message.partition: position})

        assert consumer.commit_positions() == {Partition(topic, 0): position}

        assert len(commit_log_producer.messages) == 1
        commit_message = commit_log_producer.messages[0]
        assert commit_message.topic() == "commit-log"

        assert CommitCodec().decode(
            KafkaPayload(
                commit_message.key(),
                commit_message.value(),
                commit_message.headers(),
            )
        ) == Commit("test", Partition(topic, 0), message.next_offset, now)
