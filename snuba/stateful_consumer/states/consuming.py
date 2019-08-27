from typing import Tuple

from snuba.consumers.consumer_builder import ConsumerBuilder
from snuba.stateful_consumer import StateData, StateCompletionEvent
from snuba.stateful_consumer.state_context import State


class ConsumingState(State[StateCompletionEvent, StateData]):
    """
    This is the normal operation state where the consumer
    reads from the main topic (cdc in this case) and sends
    messages to the processor.
    It can transition to paused state when a snapshot process
    starts.
    """

    def __init__(
        self,
        consumer_builder: ConsumerBuilder,
    ) -> None:
        super(ConsumingState, self).__init__()

        self.__consumer = consumer_builder.build_base_consumer()

    def signal_shutdown(self) -> None:
        self.__consumer.signal_shutdown()

    def handle(self, state_data: StateData) -> Tuple[StateCompletionEvent, StateData]:
        self.__consumer.run()
        return (
            StateCompletionEvent.CONSUMPTION_COMPLETED,
            StateData.no_snapshot_state(),
        )
