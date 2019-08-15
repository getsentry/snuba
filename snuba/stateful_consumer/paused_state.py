from snuba.stateful_consumer.consumer_context import State, StateType

from typing import Any


class PausedState(State):
    """
    In this state the consumer is waiting for the snapshto to be
    ready and loaded. It consumes the control topic waiting for
    the singal the snapshot is ready and the xid coordinates of the
    snapshot.
    """

    def _handle_impl(self, input: Any) -> (StateType, Any):
        # TODO: Actually wait on the control topic for instructions
        return (StateType.FINISHED, None)
