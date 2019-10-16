import pytest
import uuid
from unittest import mock
from typing import Iterator

from confluent_kafka import Producer as ConfluentProducer
from confluent_kafka.admin import AdminClient, NewTopic
from snuba.utils.streams.abstract import ConsumerError, EndOfStream, Message
from snuba.utils.streams.kafka import KafkaConsumer, KafkaConsumerWithCommitLog, TopicPartition
from tests.backends.confluent_kafka import FakeConfluentKafkaProducer


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

    def build_consumer() -> KafkaConsumer:
        return KafkaConsumer(
            {
                **configuration,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
                "enable.auto.offset.store": "true",
                "enable.partition.eof": "true",
                "group.id": "test",
                "session.timeout.ms": 10000,
            }
        )

    producer = ConfluentProducer(configuration)
    value = uuid.uuid1().hex.encode("utf-8")
    for i in range(2):
        producer.produce(topic, value=value)
    assert producer.flush(5.0) is 0

    consumer = build_consumer()

    # TODO: It'd be much nicer if ``subscribe`` returned a future that we could
    # use to wait for assignment, but we'd need to be very careful to avoid
    # edge cases here. It's probably not worth the complexity for now.
    # XXX: There has got to be a better way to do this...
    assignment_callback = mock.MagicMock()
    revocation_callback = mock.MagicMock()
    consumer.subscribe([topic], on_assign=assignment_callback, on_revoke=revocation_callback)

    message = consumer.poll(10.0)  # XXX: getting the subcription is slow

    assert assignment_callback.call_args_list == [mock.call([TopicPartition(topic, 0)])]

    assert isinstance(message, Message)
    assert message.stream == TopicPartition(topic, 0)
    assert message.offset == 0
    assert message.value == value

    assert consumer.commit() == {TopicPartition(topic, 0): message.offset + 1}

    consumer.unsubscribe()

    assert consumer.poll(1.0) is None

    assert revocation_callback.call_args_list == [mock.call([TopicPartition(topic, 0)])]

    consumer.close()

    with pytest.raises(RuntimeError):
        consumer.subscribe([topic])

    with pytest.raises(RuntimeError):
        consumer.unsubscribe()

    with pytest.raises(RuntimeError):
        consumer.poll()

    with pytest.raises(RuntimeError):
        consumer.commit()

    consumer.close()

    consumer = build_consumer()

    consumer.subscribe([topic])

    message = consumer.poll(10.0)  # XXX: getting the subscription is slow
    assert isinstance(message, Message)
    assert message.stream == TopicPartition(topic, 0)
    assert message.offset == 1
    assert message.value == value

    try:
        assert consumer.poll(1.0) is None
    except EndOfStream as error:
        assert error.stream == TopicPartition(topic, 0)
        assert error.offset == 2
    else:
        raise AssertionError('expected EndOfStream error')

    consumer.close()


def test_auto_offset_reset_earliest(topic: str) -> None:
    producer = ConfluentProducer(configuration)
    value = uuid.uuid1().hex.encode("utf-8")
    producer.produce(topic, value=value)
    assert producer.flush(5.0) is 0

    consumer = KafkaConsumer(
        {
            **configuration,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "enable.auto.offset.store": "true",
            "enable.partition.eof": "true",
            "group.id": "test-earliest",
        }
    )

    consumer.subscribe([topic])

    message = consumer.poll(10.0)
    assert isinstance(message, Message)
    assert message.offset == 0

    consumer.close()


def test_auto_offset_reset_latest(topic: str) -> None:
    producer = ConfluentProducer(configuration)
    value = uuid.uuid1().hex.encode("utf-8")
    producer.produce(topic, value=value)
    assert producer.flush(5.0) is 0

    consumer = KafkaConsumer(
        {
            **configuration,
            "auto.offset.reset": "latest",
            "enable.auto.commit": "false",
            "enable.auto.offset.store": "true",
            "enable.partition.eof": "true",
            "group.id": "test-latest",
        }
    )

    consumer.subscribe([topic])

    try:
        consumer.poll(10.0)  # XXX: getting the subcription is slow
    except EndOfStream as error:
        assert error.stream == TopicPartition(topic, 0)
        assert error.offset == 1
    else:
        raise AssertionError('expected EndOfStream error')

    consumer.close()


def test_auto_offset_reset_error(topic: str) -> None:
    producer = ConfluentProducer(configuration)
    value = uuid.uuid1().hex.encode("utf-8")
    producer.produce(topic, value=value)
    assert producer.flush(5.0) is 0

    consumer = KafkaConsumer(
        {
            **configuration,
            "auto.offset.reset": "error",
            "enable.auto.commit": "false",
            "enable.auto.offset.store": "true",
            "enable.partition.eof": "true",
            "group.id": "test-error",
        }
    )

    consumer.subscribe([topic])

    with pytest.raises(ConsumerError):
        consumer.poll(10.0)  # XXX: getting the subcription is slow

    consumer.close()


def test_commit_log_consumer(topic: str) -> None:
    # XXX: This would be better as an integration test (or at least a test
    # against an abstract Producer interface) instead of against a test against
    # a mock.
    commit_log_producer = FakeConfluentKafkaProducer()

    consumer = KafkaConsumerWithCommitLog(
        {
            **configuration,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "enable.auto.offset.store": "true",
            "enable.partition.eof": "true",
            "group.id": "test",
            "session.timeout.ms": 10000,
        },
        commit_log_producer,
        'commit-log',
    )

    consumer.subscribe([topic])

    producer = ConfluentProducer(configuration)
    producer.produce(topic)
    assert producer.flush(5.0) is 0

    message = consumer.poll(10.0)  # XXX: getting the subscription is slow
    assert isinstance(message, Message)

    assert consumer.commit() == {TopicPartition(topic, 0): message.offset + 1}

    assert len(commit_log_producer.messages) == 1
    commit_message = commit_log_producer.messages[0]
    assert commit_message.topic() == 'commit-log'
    assert commit_message.key() == '{}:{}:{}'.format(topic, 0, 'test').encode('utf-8')
    assert commit_message.value() == '{}'.format(message.offset + 1).encode('utf-8')  # offsets are last processed message offset + 1
