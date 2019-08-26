from typing import Tuple

from snuba.consumer_initializer import ConsumerBuiler
from snuba.stateful_consumer import StateData, StateOutput
from snuba.stateful_consumer.state_context import State


class ConsumingState(State[StateOutput, StateData]):
    """
    This is the normal operation state where the consumer
    reads from the main topic (cdc in this case) and sends
    messages to the processor.
    It can transition to paused state when a snapshot process
    starts.
    """

    def __init__(
        self,
        consumer_builder: ConsumerBuiler,
    ) -> None:
        super(ConsumingState, self).__init__()

        self.__consumer = consumer_builder.build_base_worker()

    def set_shutdown(self) -> None:
        super().set_shutdown()
        self.__consumer.signal_shutdown()

    def handle(self, state_data: StateData) -> Tuple[StateOutput, StateData]:
        self.__consumer.run()
        return (
            StateOutput.FINISH,
            StateData.no_snapshot_state(),
        )
