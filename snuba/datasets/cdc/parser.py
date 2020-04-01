from typing import Optional

from snuba.datasets.message_parser import KafkaJsonMessageParser, StreamMessageParser
from snuba.processor import Message as ParsedMessage
from snuba.utils.streams.kafka import KafkaPayload
from snuba.utils.streams.types import Message

KAFKA_ONLY_PARTITION = (
    0  # CDC only works with single partition topics. So partition must be 0
)


class CdcKafkaMessageParser(StreamMessageParser[KafkaPayload]):
    """
    Decorates a standard KafkaJsonMessageParser filtering messages that provide the
    table name in the Kafka header.
    """

    def __init__(self, use_rapid_json: bool, postgres_table: str) -> None:
        self.__wrapped_parser = KafkaJsonMessageParser(use_rapid_json)
        self.__postgres_table = postgres_table

    def parse_message(self, message: Message[KafkaPayload]) -> Optional[ParsedMessage]:
        assert (
            message.partition.index == KAFKA_ONLY_PARTITION
        ), "CDC can only work with single partition topics for consistency"

        table_header = [
            header for header in message.payload.headers if header[0] == "table"
        ]
        if table_header:
            table_name = table_header[0][1].decode("utf-8")
            if table_name != self.__postgres_table:
                return None

        return self.__wrapped_parser.parse_message(message)
