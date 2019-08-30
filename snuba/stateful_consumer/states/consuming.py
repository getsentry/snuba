from snuba.stateful_consumer import ConsumerStateData, ConsumerStateCompletionEvent
from snuba.utils.state_machine import State

from batching_kafka_consumer import BatchingKafkaConsumer
from typing import Tuple


class ConsumingState(State[ConsumerStateCompletionEvent, ConsumerStateData]):
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

    def signal_shutdown(self) -> None:
        self.__consumer.signal_shutdown()

    def handle(self, state_data: ConsumerStateData) -> Tuple[ConsumerStateCompletionEvent, ConsumerStateData]:
        self.__consumer.run()
        return (
            ConsumerStateCompletionEvent.CONSUMPTION_COMPLETED,
            ConsumerStateData.no_snapshot_state(),
        )
