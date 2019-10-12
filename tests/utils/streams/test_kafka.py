import pytest
import uuid
import time
from typing import Iterator, Sequence

from confluent_kafka import Producer as ConfluentProducer
from confluent_kafka.admin import AdminClient, NewTopic
from snuba.utils.streams.abstract import EndOfStream, Message
from snuba.utils.streams.kafka import KafkaConsumer, TopicPartition


configuration = {"bootstrap.servers": "127.0.0.1"}


@pytest.yield_fixture
def topic() -> Iterator[str]:
    name = f"test-{uuid.uuid1().hex}"
    client = AdminClient(configuration)
    [[key, future]] = client.create_topics(
        [NewTopic(name, num_partitions=1, replication_factor=1)]
    ).items()
    assert key == name
    assert future.result() is None
    try:
        yield name
    finally:
        [[key, future]] = client.delete_topics([name]).items()
        assert key == name
        assert future.result() is None


def test_consumer(topic: str) -> None:
    consumer = KafkaConsumer(
        {
            **configuration,
            "auto.offset.reset": "latest",
            "enable.auto.commit": "false",
            "enable.auto.offset.store": "true",
            "enable.partition.eof": "true",
            "group.id": "test",
            "session.timeout.ms": 10000,
        }
    )

    def assignment_callback(streams: Sequence[TopicPartition]):
        assignment_callback.called = True
        assert streams == [TopicPartition(topic, 0)]
        assert consumer.tell() == {
            TopicPartition(topic, 0): 0,
        }

    def revocation_callback(streams: Sequence[TopicPartition]):
        revocation_callback.called = True
        assert streams == [TopicPartition(topic, 0)]  # wut
        assert consumer.tell() == {
            TopicPartition(topic, 0): 1,
        }

    assignment_callback.called = False
    revocation_callback.called = False

    # TODO: It'd be much nicer if ``subscribe`` returned a future that we could
    # use to wait for assignment, but we'd need to be very careful to avoid
    # edge cases here. It's probably not worth the complexity for now.
    consumer.subscribe([topic], on_assign=assignment_callback, on_revoke=revocation_callback)

    try:
        consumer.poll(10.0)  # XXX: getting the subcription is slow
    except EndOfStream as error:
        assert error.stream == TopicPartition(topic, 0)
    else:
        raise AssertionError('expected EndOfStream error')

    assert assignment_callback.called is True

    assert consumer.tell() == {
        TopicPartition(topic, 0): 0,
    }

    producer = ConfluentProducer(configuration)
    value = uuid.uuid1().hex.encode("utf-8")
    producer.produce(topic, value=value)
    assert producer.flush(5.0) is 0

    message = consumer.poll(1.0)
    assert isinstance(message, Message)
    assert message.stream == TopicPartition(topic, 0)
    assert message.offset == 0
    assert message.value == value

    assert consumer.tell() == {
        TopicPartition(topic, 0): 1,
    }

    start = time.time()
    assert consumer.poll(0.0) is None
    assert time.time() - start < 0.001  # consumer should not block

    assert consumer.commit_offsets() == {TopicPartition(topic, 0): message.offset + 1}

    try:
        consumer.poll(1.0)
    except EndOfStream as error:
        assert error.stream == TopicPartition(topic, 0)
    else:
        raise AssertionError('expected EndOfStream error')

    assert consumer.seek({
        TopicPartition(topic, 0): 0,
    }) == consumer.tell()

    message = consumer.poll(1.0)
    assert isinstance(message, Message)
    assert message.stream == TopicPartition(topic, 0)
    assert message.offset == 0
    assert message.value == value

    consumer.unsubscribe()

    assert consumer.poll(1.0) is None

    assert revocation_callback.called is True

    assert consumer.tell() == {}

    consumer.close()

    with pytest.raises(RuntimeError):
        consumer.subscribe([topic])

    with pytest.raises(RuntimeError):
        consumer.poll()

    with pytest.raises(RuntimeError):
        consumer.commit_offsets()

    with pytest.raises(RuntimeError):
        consumer.close()
