from snuba.stateful_consumer import StateOutput
from snuba.stateful_consumer.state_context import State

from batching_kafka_consumer import BatchingKafkaConsumer
from typing import Any, Tuple


class ConsumingState(State[StateOutput]):
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

    def handle(self, input: Any) -> Tuple[StateOutput, Any]:
        self.__consumer.run()
        return (StateOutput.FINISH, None)
