from snuba import environment
from snuba.datasets.message_filters import StreamMessageFilter
from snuba.utils.metrics.backends.wrapper import MetricsWrapper
from snuba.utils.streams.kafka import KafkaPayload
from snuba.utils.streams.types import Message

metrics = MetricsWrapper(environment.metrics, "cdc.consumer")

KAFKA_ONLY_PARTITION = (
    0  # CDC only works with single partition topics. So partition must be 0
)


class CdcTableNameMessageFilter(StreamMessageFilter[KafkaPayload]):
    def __init__(self, postgres_table: str) -> None:
        self.__postgres_table = postgres_table

    def should_drop(self, message: Message[KafkaPayload]) -> bool:
        assert (
            message.partition.index == KAFKA_ONLY_PARTITION
        ), "CDC can only work with single partition topics for consistency"

        table_header = [
            header for header in message.payload.headers if header[0] == "table"
        ]
        if table_header:
            table_name = table_header[0][1].decode("utf-8")
            if table_name != self.__postgres_table:
                metrics.increment("CDC Message Dropped", tags={"table": table_name})
                return True

        return False
