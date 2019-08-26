from snuba.stateful_consumer import StateData, StateOutput
from snuba.stateful_consumer.state_context import State

from typing import Tuple


class CatchingUpState(State[StateOutput, StateData]):
    """
    In this state the consumer consumes the main topic but
    it discards the transacitons that were present in the
    snapshot (xid < xmax and not in xip_list).
    Once this phase is done the consumer goes back to normal
    consumption.
    """

    def handle(self, state_data: StateData) -> Tuple[StateOutput, StateData]:
        # TODO: Actually consume cdc topic while discarding xids that were
        # already in the dump
        return (StateOutput.SNAPSHOT_CATCHUP_COMPLETED, None)
