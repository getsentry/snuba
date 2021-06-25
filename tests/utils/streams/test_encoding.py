from arroyo import Message, Topic
from arroyo.backends.local.backend import LocalBroker as Broker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.utils.clock import TestingClock

from snuba.utils.codecs import Encoder
from snuba.utils.streams.encoding import ProducerEncodingWrapper


def test_encoding_producer() -> None:
    broker: Broker[str] = Broker(MemoryMessageStorage(), TestingClock())

    topic = Topic("test")
    broker.create_topic(topic, 1)

    class ReverseEncoder(Encoder[str, str]):
        def encode(self, value: str) -> str:
            return "".join(value[::-1])

    producer = ProducerEncodingWrapper(broker.get_producer(), ReverseEncoder())
    decoded_message = producer.produce(topic, "hello").result()
    assert decoded_message.payload == "hello"

    consumer = broker.get_consumer("group")
    consumer.subscribe([topic])

    encoded_message = consumer.poll()
    assert encoded_message is not None

    # The payload returned by the consumer should not be decoded.
    assert encoded_message.payload == "olleh"

    # All other attributes should be the same.
    for attribute in set(Message.__slots__) - {"payload"}:
        assert getattr(encoded_message, attribute) == getattr(
            decoded_message, attribute
        )
