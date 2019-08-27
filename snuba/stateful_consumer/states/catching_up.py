import logging
from typing import Tuple

from snuba.consumers.consumer_builder import ConsumerBuiler
from snuba.stateful_consumer import StateData, StateCompletionEvent
from snuba.stateful_consumer.state_context import State

logger = logging.getLogger('snuba.snapshot-catchup')


class CatchingUpState(State[StateCompletionEvent, StateData]):
    """
    In this state the consumer consumes the main topic but
    it discards the transacitons that were present in the
    snapshot (xid < xmax and not in xip_list).
    Once this phase is done the consumer goes back to normal
    consumption.
    """

    def __init__(
        self,
        consumer_builder: ConsumerBuiler
    ) -> None:
        super(CatchingUpState, self).__init__()
        self.__consumer_builder = consumer_builder
        self.__consumer = None

    def signal_shutdown(self) -> None:
        if self.__consumer:
            self.__consumer.signal_shutdown()

    def handle(self, state_data: StateData) -> Tuple[StateCompletionEvent, StateData]:
        assert state_data.snapshot_id is not None
        assert state_data.transaction_data is not None

        self.__consumer = self.__consumer_builder.build_snapshot_aware_consumer(
            snapshot_id=state_data.snapshot_id,
            transaction_data=state_data.transaction_data,
        )

        self.__consumer.run()
        return (
            StateCompletionEvent.CONSUMPTION_COMPLETED,
            StateData.no_snapshot_state(),
        )
