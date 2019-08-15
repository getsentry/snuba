from snuba.stateful_consumer.consumer_context import State, StateType

from typing import Any


class CatchingUpState(State):
    """
    In this state the consumer consumes the main topic but
    it discards the transacitons that were present in the
    snapshot (xid < xmax and not in xip_list).
    Once this phase is done the consumer goes back to normal
    consumption.
    """

    def _handle_impl(self, input: Any) -> (StateType, Any):
        # TODO: Actually consume cdc topic while discarding xids that were
        # already in the dump
        return (StateType.CONSUMING, None)
