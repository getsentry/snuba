from streaming_kafka_consumer import Message
from streaming_kafka_consumer.backends.kafka import KafkaPayload

from snuba.datasets.message_filters import StreamMessageFilter

KAFKA_ONLY_PARTITION = (
    0  # CDC only works with single partition topics. So partition must be 0
)


class CdcTableNameMessageFilter(StreamMessageFilter[KafkaPayload]):
    """
    Removes all messages from the stream that are not change events (insert,
    update, delete) on the specified table. (This also removes transactional
    events, such as begin and commit events, as they are not directed at a
    specific table.) This filtering utilizes the table header and does not
    require parsing the payload value.
    """

    def __init__(self, postgres_table: str) -> None:
        self.__postgres_table = postgres_table

    def should_drop(self, message: Message[KafkaPayload]) -> bool:
        assert (
            message.partition.index == KAFKA_ONLY_PARTITION
        ), "CDC can only work with single partition topics for consistency"

        table_name = next(
            (value for key, value in message.payload.headers if key == "table"), None
        )

        return not table_name or table_name.decode("utf-8") != self.__postgres_table
