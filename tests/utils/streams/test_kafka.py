import uuid
from typing import Iterator, Mapping, Sequence

import pytest
from concurrent.futures import wait
from confluent_kafka.admin import AdminClient, NewTopic
from contextlib import closing

from snuba.utils.codecs import PassthroughCodec
from snuba.utils.streams.consumer import Consumer, ConsumerError, EndOfPartition
from snuba.utils.streams.kafka import (
    Commit,
    CommitCodec,
    KafkaConsumer,
    KafkaConsumerWithCommitLog,
    KafkaPayload,
    KafkaProducer,
    as_kafka_configuration_bool,
)
from snuba.utils.streams.types import (
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


def build_producer() -> KafkaProducer[KafkaPayload]:
    codec: PassthroughCodec[KafkaPayload] = PassthroughCodec()
    return KafkaProducer(configuration, codec=codec)


def test_consumer_backend(topic: Topic) -> None:
    def build_consumer() -> Consumer[KafkaPayload]:
        codec: PassthroughCodec[KafkaPayload] = PassthroughCodec()
        return KafkaConsumer(
            {
                **configuration,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
                "enable.auto.offset.store": "false",
                "enable.partition.eof": "true",
                "group.id": "test",
                "session.timeout.ms": 10000,
            },
            codec=codec,
        )

    with closing(build_producer()) as producer:
        value = uuid.uuid1().hex.encode("utf-8")
        assert (
            wait(
                [producer.produce(topic, KafkaPayload(None, value)) for i in range(2)],
                timeout=5.0,
            ).not_done
            == set()
        )

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
    assert message.payload == KafkaPayload(None, value)

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
    assert message.payload == KafkaPayload(None, value)

    assert consumer.commit_offsets() == {}

    consumer.stage_offsets({message.partition: message.get_next_offset()})

    with pytest.raises(ConsumerError):
        consumer.stage_offsets({Partition(Topic("invalid"), 0): 0})

    assert consumer.commit_offsets() == {Partition(topic, 0): message.get_next_offset()}

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
        consumer.stage_offsets({})

    with pytest.raises(RuntimeError):
        consumer.commit_offsets()

    consumer.close()

    consumer = build_consumer()

    consumer.subscribe([topic])

    message = consumer.poll(10.0)  # XXX: getting the subscription is slow
    assert isinstance(message, Message)
    assert message.partition == Partition(topic, 0)
    assert message.offset == 1
    assert message.payload == KafkaPayload(None, value)

    try:
        assert consumer.poll(1.0) is None
    except EndOfPartition as error:
        assert error.partition == Partition(topic, 0)
        assert error.offset == 2
    else:
        raise AssertionError("expected EndOfPartition error")

    consumer.close()


def test_auto_offset_reset_earliest(topic: Topic) -> None:
    with closing(build_producer()) as producer:
        value = uuid.uuid1().hex.encode("utf-8")
        producer.produce(topic, KafkaPayload(None, value)).result(5.0)

    codec: PassthroughCodec[KafkaPayload] = PassthroughCodec()
    consumer: Consumer[KafkaPayload] = KafkaConsumer(
        {
            **configuration,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "enable.auto.offset.store": "false",
            "enable.partition.eof": "true",
            "group.id": "test-earliest",
        },
        codec=codec,
    )

    consumer.subscribe([topic])

    message = consumer.poll(10.0)
    assert isinstance(message, Message)
    assert message.offset == 0

    consumer.close()


def test_auto_offset_reset_latest(topic: Topic) -> None:
    with closing(build_producer()) as producer:
        value = uuid.uuid1().hex.encode("utf-8")
        producer.produce(topic, KafkaPayload(None, value)).result(5.0)

    codec: PassthroughCodec[KafkaPayload] = PassthroughCodec()
    consumer: Consumer[KafkaPayload] = KafkaConsumer(
        {
            **configuration,
            "auto.offset.reset": "latest",
            "enable.auto.commit": "false",
            "enable.auto.offset.store": "false",
            "enable.partition.eof": "true",
            "group.id": "test-latest",
        },
        codec=codec,
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
    with closing(build_producer()) as producer:
        value = uuid.uuid1().hex.encode("utf-8")
        producer.produce(topic, KafkaPayload(None, value)).result(5.0)

    codec: PassthroughCodec[KafkaPayload] = PassthroughCodec()
    consumer: Consumer[KafkaPayload] = KafkaConsumer(
        {
            **configuration,
            "auto.offset.reset": "error",
            "enable.auto.commit": "false",
            "enable.auto.offset.store": "false",
            "enable.partition.eof": "true",
            "group.id": "test-error",
        },
        codec=codec,
    )

    consumer.subscribe([topic])

    with pytest.raises(ConsumerError):
        consumer.poll(10.0)  # XXX: getting the subcription is slow

    consumer.close()


def test_commit_codec() -> None:
    codec = CommitCodec()
    commit = Commit("group", Partition(Topic("topic"), 0), 0)
    assert codec.decode(codec.encode(commit)) == commit


def test_commit_log_consumer(topic: Topic) -> None:
    # XXX: This would be better as an integration test (or at least a test
    # against an abstract Producer interface) instead of against a test against
    # a mock.
    commit_log_producer = FakeConfluentKafkaProducer()

    codec: PassthroughCodec[KafkaPayload] = PassthroughCodec()
    consumer: Consumer[KafkaPayload] = KafkaConsumerWithCommitLog(
        {
            **configuration,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "enable.auto.offset.store": "false",
            "enable.partition.eof": "true",
            "group.id": "test",
            "session.timeout.ms": 10000,
        },
        codec=codec,
        producer=commit_log_producer,
        commit_log_topic=Topic("commit-log"),
    )

    consumer.subscribe([topic])

    with closing(build_producer()) as producer:
        producer.produce(topic, KafkaPayload(None, b"")).result(5.0)

    message = consumer.poll(10.0)  # XXX: getting the subscription is slow
    assert isinstance(message, Message)

    consumer.stage_offsets({message.partition: message.get_next_offset()})

    assert consumer.commit_offsets() == {Partition(topic, 0): message.get_next_offset()}

    assert len(commit_log_producer.messages) == 1
    commit_message = commit_log_producer.messages[0]
    assert commit_message.topic() == "commit-log"

    assert CommitCodec().decode(
        KafkaPayload(commit_message.key(), commit_message.value())
    ) == Commit("test", Partition(topic, 0), message.get_next_offset())


def test_as_kafka_configuration_bool():
    assert as_kafka_configuration_bool(False) == False
    assert as_kafka_configuration_bool("false") == False
    assert as_kafka_configuration_bool("FALSE") == False
    assert as_kafka_configuration_bool("0") == False
    assert as_kafka_configuration_bool("f") == False
    assert as_kafka_configuration_bool(0) == False

    assert as_kafka_configuration_bool(True) == True
    assert as_kafka_configuration_bool("true") == True
    assert as_kafka_configuration_bool("TRUE") == True
    assert as_kafka_configuration_bool("1") == True
    assert as_kafka_configuration_bool("t") == True
    assert as_kafka_configuration_bool(1) == True

    with pytest.raises(TypeError):
        assert as_kafka_configuration_bool(None)

    with pytest.raises(ValueError):
        assert as_kafka_configuration_bool("")

    with pytest.raises(ValueError):
        assert as_kafka_configuration_bool("tru")

    with pytest.raises(ValueError):
        assert as_kafka_configuration_bool("flase")

    with pytest.raises(ValueError):
        assert as_kafka_configuration_bool(2)

    with pytest.raises(TypeError):
        assert as_kafka_configuration_bool(0.0)
