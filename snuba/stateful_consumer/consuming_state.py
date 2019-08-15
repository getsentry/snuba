from snuba.stateful_consumer.consumer_context import State, StateType

from batching_kafka_consumer import BatchingKafkaConsumer
from typing import Any


class ConsumingState(State):
    """
    This is the normal operation state where the consumer
    reads from the main topic (cdc in this case) and sends
    messages to the processor.
    It can transition to paused state when a snapshot process
    starts.
    """

    def __init__(self, consumer: BatchingKafkaConsumer) -> None:
        super(ConsumingState, self).__init__()
        self.__consumer = consumer

    def set_shutdown(self) -> None:
        super().set_shutdown()
        self.__consumer.signal_shutdown()

    def _handle_impl(self, input: Any) -> (StateType, Any):
        self.__consumer.run()
        return (StateType.FINISHED, None)
