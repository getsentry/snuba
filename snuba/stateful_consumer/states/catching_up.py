from snuba.stateful_consumer import StateData, StateCompletionEvent
from snuba.stateful_consumer.state_context import State

from typing import Tuple


class CatchingUpState(State[StateCompletionEvent, StateData]):
    """
    In this state the consumer consumes the main topic but
    it discards the transacitons that were present in the
    snapshot (xid < xmax and not in xip_list).
    Once this phase is done the consumer goes back to normal
    consumption.
    """

    def signal_shutdown(self) -> None:
        pass

    def handle(self, state_data: StateData) -> Tuple[StateCompletionEvent, StateData]:
        # TODO: Actually consume cdc topic while discarding xids that were
        # already in the dump
        return (
            StateCompletionEvent.SNAPSHOT_CATCHUP_COMPLETED,
            StateData.no_snapshot_state(),
        )
