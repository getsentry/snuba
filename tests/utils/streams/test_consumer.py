import pytest
import uuid
from typing import Iterator, Mapping, Sequence

from confluent_kafka import Producer as ConfluentProducer
from confluent_kafka.admin import AdminClient, NewTopic
from snuba.utils.streams.consumer import (
    KafkaConsumer,
    KafkaConsumerWithCommitLog,
)
from snuba.utils.streams.types import (
    ConsumerError,
    EndOfPartition,
    Message,
    Partition,
    Topic,
)
from tests.backends.confluent_kafka import FakeConfluentKafkaProducer


configuration = {"bootstrap.servers": "127.0.0.1"}


@pytest.yield_fixture
def topic() -> Iterator[Topic]:
    name = f"test-{uuid.uuid1().hex}"
    client = AdminClient(configuration)
    [[key, future]] = client.create_topics(
        [NewTopic(name, num_partitions=1, replication_factor=1)]
    ).items()
    assert key == name
    assert future.result() is None
    try:
        yield Topic(name)
    finally:
        [[key, future]] = client.delete_topics([name]).items()
        assert key == name
        assert future.result() is None


def test_data_types() -> None:
    assert Partition(Topic("topic"), 0) in Topic("topic")
    assert Partition(Topic("topic"), 0) not in Topic("other-topic")
    assert Partition(Topic("other-topic"), 0) not in Topic("topic")


def test_consumer_backend(topic: Topic) -> None:
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
        producer.produce(topic.name, value=value)
    assert producer.flush(5.0) == 0

    consumer = build_consumer()

    def assignment_callback(partitions: Mapping[Partition, int]):
        assignment_callback.called = True
        assert partitions == {Partition(topic, 0): 0}

        consumer.seek({Partition(topic, 0): 1})

        with pytest.raises(ConsumerError):
            consumer.seek({Partition(topic, 1): 0})

    def revocation_callback(partitions: Sequence[Partition]):
        revocation_callback.called = True
        assert partitions == [Partition(topic, 0)]
        assert consumer.tell() == {Partition(topic, 0): 1}

        # Not sure why you'd want to do this, but it shouldn't error.
        consumer.seek({Partition(topic, 0): 0})

    # TODO: It'd be much nicer if ``subscribe`` returned a future that we could
    # use to wait for assignment, but we'd need to be very careful to avoid
    # edge cases here. It's probably not worth the complexity for now.
    consumer.subscribe(
        [topic], on_assign=assignment_callback, on_revoke=revocation_callback
    )

    message = consumer.poll(10.0)  # XXX: getting the subcription is slow
    assert isinstance(message, Message)
    assert message.partition == Partition(topic, 0)
    assert message.offset == 1
    assert message.value == value

    assert consumer.tell() == {Partition(topic, 0): 2}
    assert getattr(assignment_callback, "called", False)

    consumer.seek({Partition(topic, 0): 0})
    assert consumer.tell() == {Partition(topic, 0): 0}

    with pytest.raises(ConsumerError):
        consumer.seek({Partition(topic, 1): 0})

    consumer.pause([Partition(topic, 0)])

    consumer.resume([Partition(topic, 0)])

    message = consumer.poll(1.0)
    assert isinstance(message, Message)
    assert message.partition == Partition(topic, 0)
    assert message.offset == 0
    assert message.value == value

    assert consumer.commit() == {Partition(topic, 0): message.get_next_offset()}

    consumer.unsubscribe()

    assert consumer.poll(1.0) is None

    assert consumer.tell() == {}

    with pytest.raises(ConsumerError):
        consumer.seek({Partition(topic, 0): 0})

    consumer.close()

    with pytest.raises(RuntimeError):
        consumer.subscribe([topic])

    with pytest.raises(RuntimeError):
        consumer.unsubscribe()

    with pytest.raises(RuntimeError):
        consumer.poll()

    with pytest.raises(RuntimeError):
        consumer.tell()

    with pytest.raises(RuntimeError):
        consumer.seek({Partition(topic, 0): 0})

    with pytest.raises(RuntimeError):
        consumer.pause([Partition(topic, 0)])

    with pytest.raises(RuntimeError):
        consumer.resume([Partition(topic, 0)])

    with pytest.raises(RuntimeError):
        consumer.commit()

    consumer.close()

    consumer = build_consumer()

    consumer.subscribe([topic])

    message = consumer.poll(10.0)  # XXX: getting the subscription is slow
    assert isinstance(message, Message)
    assert message.partition == Partition(topic, 0)
    assert message.offset == 1
    assert message.value == value

    try:
        assert consumer.poll(1.0) is None
    except EndOfPartition as error:
        assert error.partition == Partition(topic, 0)
        assert error.offset == 2
    else:
        raise AssertionError("expected EndOfPartition error")

    consumer.close()


def test_auto_offset_reset_earliest(topic: Topic) -> None:
    producer = ConfluentProducer(configuration)
    value = uuid.uuid1().hex.encode("utf-8")
    producer.produce(topic.name, value=value)
    assert producer.flush(5.0) == 0

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


def test_auto_offset_reset_latest(topic: Topic) -> None:
    producer = ConfluentProducer(configuration)
    value = uuid.uuid1().hex.encode("utf-8")
    producer.produce(topic.name, value=value)
    assert producer.flush(5.0) == 0

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
    except EndOfPartition as error:
        assert error.partition == Partition(topic, 0)
        assert error.offset == 1
    else:
        raise AssertionError("expected EndOfPartition error")

    consumer.close()


def test_auto_offset_reset_error(topic: Topic) -> None:
    producer = ConfluentProducer(configuration)
    value = uuid.uuid1().hex.encode("utf-8")
    producer.produce(topic.name, value=value)
    assert producer.flush(5.0) == 0

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


def test_commit_log_consumer(topic: Topic) -> None:
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
        producer=commit_log_producer,
        commit_log_topic=Topic("commit-log"),
    )

    consumer.subscribe([topic])

    producer = ConfluentProducer(configuration)
    producer.produce(topic.name)
    assert producer.flush(5.0) == 0

    message = consumer.poll(10.0)  # XXX: getting the subscription is slow
    assert isinstance(message, Message)

    assert consumer.commit() == {Partition(topic, 0): message.get_next_offset()}

    assert len(commit_log_producer.messages) == 1
    commit_message = commit_log_producer.messages[0]
    assert commit_message.topic() == "commit-log"
    assert commit_message.key() == "{}:{}:{}".format(topic.name, 0, "test").encode(
        "utf-8"
    )
    assert commit_message.value() == "{}".format(message.get_next_offset()).encode(
        "utf-8"
    )
