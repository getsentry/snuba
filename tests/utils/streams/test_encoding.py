from snuba.utils.codecs import Encoder
from snuba.utils.streams.backends.dummy import DummyBroker as Broker
from snuba.utils.streams.encoding import ProducerEncodingWrapper
from snuba.utils.streams.types import Message, Topic


def test_encoding_producer(broker: Broker[str]) -> None:
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
