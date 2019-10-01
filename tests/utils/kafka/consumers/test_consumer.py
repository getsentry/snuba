import pytest
import uuid
from unittest import mock
from typing import Iterator

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from snuba.utils.kafka.consumers import abstract, confluent
from snuba.utils.kafka.consumers.abstract import TopicPartitionKey


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


@pytest.mark.parametrize(
    "consumer,producer",
    [
        (
            confluent.Consumer(
                {
                    **configuration,
                    "auto.offset.reset": "latest",
                    "enable.auto.commit": "false",
                    "enable.auto.offset.store": "true",
                    "enable.partition.eof": "false",
                    "group.id": "test",
                    "session.timeout.ms": 1000,
                }
            ),
            Producer({**configuration}),
        )
    ],
)
def test_consumer(topic: str, consumer: abstract.Consumer, producer: Producer) -> None:
    assert isinstance(consumer, abstract.Consumer)

    # TODO: It'd be much nicer if ``subscribe`` returned a future that we could
    # use to wait for assignment, but we'd need to be very careful to avoid
    # edge cases here. It's probably not worth the complexity for now.
    # XXX: There has got to be a better way to do this...
    callback = mock.MagicMock()
    consumer.subscribe([topic], on_assign=callback)

    message = consumer.poll(10.0)  # XXX: getting the subcription is slow
    assert message is None
    assert callback.call_args_list == [mock.call([TopicPartitionKey(topic, 0)])]

    value = uuid.uuid1().hex.encode("utf-8")
    producer.produce(topic, value=value)
    assert producer.flush(5.0) is 0

    message = consumer.poll(1.0)
    assert message is not None

    # TODO: This specific API may/will change in the future.
    assert message.topic() == topic
    assert message.partition() == 0
    assert message.value() == value

    assert consumer.commit() == {
        TopicPartitionKey(topic, 0): message.value() + 1,
    }

    consumer.close()

    with pytest.raises(RuntimeError):
        consumer.subscribe([topic])

    with pytest.raises(RuntimeError):
        consumer.poll()

    with pytest.raises(RuntimeError):
        consumer.commit()

    with pytest.raises(RuntimeError):
        consumer.close()
