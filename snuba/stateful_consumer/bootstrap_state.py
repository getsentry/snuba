from snuba.stateful_consumer import StateData, StateOutput
from snuba.stateful_consumer.state_context import State

from typing import Tuple


class BootstrapState(State[StateOutput, StateData]):
    """
    This is the state the consumer starts into.
    Its job is to either transition to normal operation or
    to recover a previously running snapshot if the conumer
    was restarted while the process was on going.
    The recovery process is done by consuming the whole
    control topic.
    """

    def handle(self, state_data: StateData) -> Tuple[StateOutput, StateData]:
        # TODO: Actually do the snapshot bootstrap
        return (
            StateOutput.NO_SNAPSHOT,
            StateData.no_snapshot_state(),
        )
