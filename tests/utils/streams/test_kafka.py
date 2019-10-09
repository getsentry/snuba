import pytest
import uuid
import time
from unittest import mock
from typing import Iterator

from confluent_kafka import Producer as ConfluentProducer
from confluent_kafka.admin import AdminClient, NewTopic
from snuba.utils.streams.abstract import Message
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
            "enable.partition.eof": "false",
            "group.id": "test",
            "session.timeout.ms": 10000,
        }
    )

    # TODO: It'd be much nicer if ``subscribe`` returned a future that we could
    # use to wait for assignment, but we'd need to be very careful to avoid
    # edge cases here. It's probably not worth the complexity for now.
    # XXX: There has got to be a better way to do this...
    assignment_callback = mock.MagicMock()
    consumer.subscribe([topic], on_assign=assignment_callback)

    message = consumer.poll(10.0)  # XXX: getting the subcription is slow
    assert message is None
    assert assignment_callback.call_args_list == [mock.call([TopicPartition(topic, 0)])]

    producer = ConfluentProducer(configuration)
    value = uuid.uuid1().hex.encode("utf-8")
    producer.produce(topic, value=value)
    assert producer.flush(5.0) is 0

    message = consumer.poll(1.0)
    assert isinstance(message, Message)
    assert message.stream == TopicPartition(topic, 0)
    assert message.value == value

    start = time.time()
    assert consumer.poll(0.0) is None
    assert time.time() - start < 0.001  # consumer should not block

    assert consumer.commit() == {TopicPartition(topic, 0): message.offset + 1}

    consumer.close()

    with pytest.raises(RuntimeError):
        consumer.subscribe([topic])

    with pytest.raises(RuntimeError):
        consumer.poll()

    with pytest.raises(RuntimeError):
        consumer.commit()

    with pytest.raises(RuntimeError):
        consumer.close()
