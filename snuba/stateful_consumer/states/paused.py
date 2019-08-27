from snuba.stateful_consumer import StateData, StateCompletionEvent
from snuba.stateful_consumer.state_context import State

from typing import Tuple


class PausedState(State[StateCompletionEvent, StateData]):
    """
    In this state the consumer is waiting for the snapshto to be
    ready and loaded. It consumes the control topic waiting for
    the singal the snapshot is ready and the xid coordinates of the
    snapshot.
    """

    def signal_shutdown(self) -> None:
        pass

    def handle(self, input: StateData) -> Tuple[StateCompletionEvent, StateData]:
        # TODO: Actually wait on the control topic for instructions
        return (
            StateCompletionEvent.CONSUMPTION_COMPLETED,
            StateData.no_snapshot_state(),
        )
