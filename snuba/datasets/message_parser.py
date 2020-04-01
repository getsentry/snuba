import rapidjson
import simplejson as json

from abc import ABC, abstractmethod
from typing import Generic, Optional

from snuba.processor import Message as ParsedMessage
from snuba.utils.streams.kafka import KafkaPayload
from snuba.utils.streams.types import Message, TPayload


class StreamMessageParser(ABC, Generic[TPayload]):
    """
    Parses a message coming from the stream to be consumed and turns it into
    a message ready for the message processor.
    """

    @abstractmethod
    def parse_message(self, message: Message[TPayload]) -> Optional[ParsedMessage]:
        raise NotImplementedError


class KafkaJsonMessageParser(StreamMessageParser[KafkaPayload]):
    """
    The basic Kafka message parser that expects Json in input and either uses
    rapidjson or simplejson to parse the message.
    """

    def __init__(self, use_rapid_json: bool) -> None:
        self.__use_rapid_json = use_rapid_json

    def parse_message(self, message: Message[KafkaPayload]) -> Optional[ParsedMessage]:
        if self.__use_rapid_json:
            value = rapidjson.loads(message.payload.value)
        else:
            value = json.loads(message.payload.value)

        return ParsedMessage(value)
