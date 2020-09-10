from snuba import environment
from snuba.datasets.message_filters import StreamMessageFilter
from snuba.utils.metrics.backends.wrapper import MetricsWrapper
from snuba.utils.streams import Message
from snuba.utils.streams.backends.kafka import KafkaPayload

metrics = MetricsWrapper(environment.metrics, "cdc.consumer")

KAFKA_ONLY_PARTITION = (
    0  # CDC only works with single partition topics. So partition must be 0
)


class CdcTableNameMessageFilter(StreamMessageFilter[KafkaPayload]):
    """
    Filters CDC messages not related with the table this filter is interested into.
    This filtering happens based on the table header when present and without
    parsing the message payload.
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

        if table_name:
            table_name = table_name.decode("utf-8")
            if table_name != self.__postgres_table:
                metrics.increment("cdc_message_dropped", tags={"table": table_name})
                return True

        return False
