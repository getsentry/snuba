from snuba.stateful_consumer import ConsumerStateData, ConsumerStateCompletionEvent
from snuba.utils.state_machine import State

from typing import Optional, Tuple


class PausedState(State[ConsumerStateCompletionEvent, Optional[ConsumerStateData]]):
    """
    In this state the consumer is waiting for the snapshot to be
    ready and loaded. It consumes the control topic waiting for
    the singal the snapshot is ready and the xid coordinates of the
    snapshot.
    """

    def signal_shutdown(self) -> None:
        pass

    def handle(
        self, input: Optional[ConsumerStateData],
    ) -> Tuple[ConsumerStateCompletionEvent, Optional[ConsumerStateData]]:
        # TODO: Actually wait on the control topic for instructions
        return (
            ConsumerStateCompletionEvent.CONSUMPTION_COMPLETED,
            None,
        )
