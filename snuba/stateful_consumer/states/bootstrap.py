from snuba.stateful_consumer import ConsumerStateData, ConsumerStateCompletionEvent
from snuba.utils.state_machine import State

from typing import Tuple


class BootstrapState(State[ConsumerStateCompletionEvent, ConsumerStateData]):
    """
    This is the state the consumer starts into.
    Its job is to either transition to normal operation or
    to recover a previously running snapshot if the conumer
    was restarted while the process was on going.
    The recovery process is done by consuming the whole
    control topic.
    """

    def signal_shutdown(self) -> None:
        pass

    def handle(self, state_data: ConsumerStateData) -> Tuple[ConsumerStateCompletionEvent, ConsumerStateData]:
        # TODO: Actually do the snapshot bootstrap
        return (
            ConsumerStateCompletionEvent.NO_SNAPSHOT,
            ConsumerStateData.no_snapshot_state(),
        )
