from snuba.stateful_consumer import ConsumerStateData, ConsumerStateCompletionEvent
from snuba.utils.state_machine import State

from typing import Tuple


class CatchingUpState(State[ConsumerStateCompletionEvent, ConsumerStateData]):
    """
    In this state the consumer consumes the main topic but
    it discards the transacitons that were present in the
    snapshot (xid < xmax and not in xip_list).
    Once this phase is done the consumer goes back to normal
    consumption.
    """

    def signal_shutdown(self) -> None:
        pass

    def handle(self, state_data: ConsumerStateData) -> Tuple[ConsumerStateCompletionEvent, ConsumerStateData]:
        # TODO: Actually consume cdc topic while discarding xids that were
        # already in the dump
        return (
            ConsumerStateCompletionEvent.SNAPSHOT_CATCHUP_COMPLETED,
            ConsumerStateData.no_snapshot_state(),
        )
