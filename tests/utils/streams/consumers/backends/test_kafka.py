import pytest
import uuid
from typing import Iterator, Sequence

from confluent_kafka import Producer as ConfluentProducer
from confluent_kafka.admin import AdminClient, NewTopic
from snuba.utils.streams.consumers.types import ConsumerError, EndOfStream, Message
from snuba.utils.streams.consumers.backends.kafka import KafkaConsumerBackend, KafkaConsumerBackendWithCommitLog, TopicPartition
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


def test_consumer_backend(topic: str) -> None:

    def build_backend() -> KafkaConsumerBackend:
        return KafkaConsumerBackend(
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

    backend = build_backend()

    def assignment_callback(streams: Sequence[TopicPartition]):
        assignment_callback.called = True
        assert streams == [TopicPartition(topic, 0)]
        assert backend.tell() == {TopicPartition(topic, 0): 0}

        backend.seek({TopicPartition(topic, 0): 1})

        with pytest.raises(ConsumerError):
            backend.seek({TopicPartition(topic, 1): 0})

    def revocation_callback(streams: Sequence[TopicPartition]):
        revocation_callback.called = True
        assert streams == [TopicPartition(topic, 0)]
        assert backend.tell() == {TopicPartition(topic, 0): 1}

        # Not sure why you'd want to do this, but it shouldn't error.
        backend.seek({TopicPartition(topic, 0): 0})

    # TODO: It'd be much nicer if ``subscribe`` returned a future that we could
    # use to wait for assignment, but we'd need to be very careful to avoid
    # edge cases here. It's probably not worth the complexity for now.
    backend.subscribe([topic], on_assign=assignment_callback, on_revoke=revocation_callback)

    message = backend.poll(10.0)  # XXX: getting the subcription is slow
    assert isinstance(message, Message)
    assert message.stream == TopicPartition(topic, 0)
    assert message.offset == 1
    assert message.value == value

    assert backend.tell() == {TopicPartition(topic, 0): 2}
    assert getattr(assignment_callback, 'called', False)

    backend.seek({TopicPartition(topic, 0): 0})
    assert backend.tell() == {TopicPartition(topic, 0): 0}

    with pytest.raises(ConsumerError):
        backend.seek({TopicPartition(topic, 1): 0})

    backend.pause([TopicPartition(topic, 0)])

    backend.resume([TopicPartition(topic, 0)])

    message = backend.poll(1.0)
    assert isinstance(message, Message)
    assert message.stream == TopicPartition(topic, 0)
    assert message.offset == 0
    assert message.value == value

    assert backend.commit() == {TopicPartition(topic, 0): message.get_next_offset()}

    backend.unsubscribe()

    assert backend.poll(1.0) is None

    assert backend.tell() == {}

    with pytest.raises(ConsumerError):
        backend.seek({TopicPartition(topic, 0): 0})

    backend.close()

    with pytest.raises(RuntimeError):
        backend.subscribe([topic])

    with pytest.raises(RuntimeError):
        backend.unsubscribe()

    with pytest.raises(RuntimeError):
        backend.poll()

    with pytest.raises(RuntimeError):
        backend.tell()

    with pytest.raises(RuntimeError):
        backend.seek({TopicPartition(topic, 0): 0})

    with pytest.raises(RuntimeError):
        backend.pause([TopicPartition(topic, 0)])

    with pytest.raises(RuntimeError):
        backend.resume([TopicPartition(topic, 0)])

    with pytest.raises(RuntimeError):
        backend.commit()

    backend.close()

    backend = build_backend()

    backend.subscribe([topic])

    message = backend.poll(10.0)  # XXX: getting the subscription is slow
    assert isinstance(message, Message)
    assert message.stream == TopicPartition(topic, 0)
    assert message.offset == 1
    assert message.value == value

    try:
        assert backend.poll(1.0) is None
    except EndOfStream as error:
        assert error.stream == TopicPartition(topic, 0)
        assert error.offset == 2
    else:
        raise AssertionError('expected EndOfStream error')

    backend.close()


def test_auto_offset_reset_earliest(topic: str) -> None:
    producer = ConfluentProducer(configuration)
    value = uuid.uuid1().hex.encode("utf-8")
    producer.produce(topic, value=value)
    assert producer.flush(5.0) is 0

    backend = KafkaConsumerBackend(
        {
            **configuration,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "enable.auto.offset.store": "true",
            "enable.partition.eof": "true",
            "group.id": "test-earliest",
        }
    )

    backend.subscribe([topic])

    message = backend.poll(10.0)
    assert isinstance(message, Message)
    assert message.offset == 0

    backend.close()


def test_auto_offset_reset_latest(topic: str) -> None:
    producer = ConfluentProducer(configuration)
    value = uuid.uuid1().hex.encode("utf-8")
    producer.produce(topic, value=value)
    assert producer.flush(5.0) is 0

    backend = KafkaConsumerBackend(
        {
            **configuration,
            "auto.offset.reset": "latest",
            "enable.auto.commit": "false",
            "enable.auto.offset.store": "true",
            "enable.partition.eof": "true",
            "group.id": "test-latest",
        }
    )

    backend.subscribe([topic])

    try:
        backend.poll(10.0)  # XXX: getting the subcription is slow
    except EndOfStream as error:
        assert error.stream == TopicPartition(topic, 0)
        assert error.offset == 1
    else:
        raise AssertionError('expected EndOfStream error')

    backend.close()


def test_auto_offset_reset_error(topic: str) -> None:
    producer = ConfluentProducer(configuration)
    value = uuid.uuid1().hex.encode("utf-8")
    producer.produce(topic, value=value)
    assert producer.flush(5.0) is 0

    backend = KafkaConsumerBackend(
        {
            **configuration,
            "auto.offset.reset": "error",
            "enable.auto.commit": "false",
            "enable.auto.offset.store": "true",
            "enable.partition.eof": "true",
            "group.id": "test-error",
        }
    )

    backend.subscribe([topic])

    with pytest.raises(ConsumerError):
        backend.poll(10.0)  # XXX: getting the subcription is slow

    backend.close()


def test_commit_log_consumer_backend(topic: str) -> None:
    # XXX: This would be better as an integration test (or at least a test
    # against an abstract Producer interface) instead of against a test against
    # a mock.
    commit_log_producer = FakeConfluentKafkaProducer()

    backend = KafkaConsumerBackendWithCommitLog(
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

    backend.subscribe([topic])

    producer = ConfluentProducer(configuration)
    producer.produce(topic)
    assert producer.flush(5.0) is 0

    message = backend.poll(10.0)  # XXX: getting the subscription is slow
    assert isinstance(message, Message)

    assert backend.commit() == {TopicPartition(topic, 0): message.get_next_offset()}

    assert len(commit_log_producer.messages) == 1
    commit_message = commit_log_producer.messages[0]
    assert commit_message.topic() == 'commit-log'
    assert commit_message.key() == '{}:{}:{}'.format(topic, 0, 'test').encode('utf-8')
    assert commit_message.value() == '{}'.format(message.get_next_offset()).encode('utf-8')
