from snuba.stateful_consumer import StateOutput
from snuba.stateful_consumer.state_context import State

from typing import Any, Tuple


class PausedState(State[StateOutput]):
    """
    In this state the consumer is waiting for the snapshto to be
    ready and loaded. It consumes the control topic waiting for
    the singal the snapshot is ready and the xid coordinates of the
    snapshot.
    """

    def handle(self, input: Any) -> Tuple[StateOutput, Any]:
        # TODO: Actually wait on the control topic for instructions
        return (StateOutput.FINISH, None)
