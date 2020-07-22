import logging
from typing import Optional, Tuple

from snuba.consumers.consumer_builder import ConsumerBuilder
from snuba.stateful_consumer import ConsumerStateData, ConsumerStateCompletionEvent
from snuba.utils.streams.kafka import KafkaPayload
from snuba.utils.streams.processing import StreamProcessor
from snuba.utils.state_machine import State

logger = logging.getLogger("snuba.snapshot-catchup")


class CatchingUpState(State[ConsumerStateCompletionEvent, Optional[ConsumerStateData]]):
    """
    In this state the consumer consumes the main topic but
    it discards the transacitons that were present in the
    snapshot (xid < xmax and not in xip_list).
    Once this phase is done the consumer goes back to normal
    consumption.
    """

    def __init__(self, consumer_builder: ConsumerBuilder) -> None:
        super().__init__()
        self.__consumer_builder = consumer_builder
        self.__consumer: Optional[StreamProcessor[KafkaPayload]] = None

    def signal_shutdown(self) -> None:
        if self.__consumer:
            self.__consumer.signal_shutdown()

    def handle(
        self, state_data: Optional[ConsumerStateData]
    ) -> Tuple[ConsumerStateCompletionEvent, Optional[ConsumerStateData]]:
        assert state_data is not None

        consumer = self.__consumer_builder.build_snapshot_aware_consumer(
            snapshot_id=state_data.snapshot_id,
            transaction_data=state_data.transaction_data,
        )
        self.__consumer = consumer

        consumer.run()
        return (
            ConsumerStateCompletionEvent.CONSUMPTION_COMPLETED,
            None,
        )
