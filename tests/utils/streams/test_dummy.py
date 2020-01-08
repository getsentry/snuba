import pytest

from snuba.utils.streams.consumer import Consumer, ConsumerError
from snuba.utils.streams.dummy import DummyConsumer
from snuba.utils.streams.types import Message, Partition, Topic


def test_working_offsets() -> None:
    topic = Topic("example")
    partition = Partition(topic, 0)

    consumer: Consumer[int] = DummyConsumer({partition: [0]})
    consumer.subscribe([topic])

    # NOTE: This will eventually need to be controlled by a generalized
    # consumer auto offset reset setting.
    assert consumer.tell() == {partition: 0}

    message = consumer.poll()
    assert isinstance(message, Message)
    assert message.offset == 0
    assert consumer.tell() == {partition: 1}

    # It should be safe to try to read the first missing offset (index) in the
    # partition.
    assert consumer.poll() is None

    consumer.seek({partition: 0})
    assert consumer.tell() == {partition: 0}

    message = consumer.poll()
    assert isinstance(message, Message)
    assert message.offset == 0
    assert consumer.tell() == {partition: 1}

    # Seeking beyond the first missing index should work, but subsequent reads
    # should error.
    consumer.seek({partition: 2})
    assert consumer.tell() == {partition: 2}

    with pytest.raises(ConsumerError):
        consumer.poll()

    # Offsets should not be advanced after a failed poll.
    assert consumer.tell() == {partition: 2}

    # Trying to seek on an unassigned partition should error.
    with pytest.raises(ConsumerError):
        consumer.seek({partition: 0, Partition(topic, -1): 0})

    # ``seek`` should be atomic -- either all updates are applied or none are.
    assert consumer.tell() == {partition: 2}
