import logging

from confluent_kafka import Consumer, Producer

from snuba.consumer import ConsumerWorker
from snuba.consumers.snapshot_worker import SnapshotAwareWorker
from snuba.datasets import Dataset
from snuba.stateful_consumer import StateData, StateOutput
from snuba.stateful_consumer.state_context import State

from typing import Any, Callable, Optional, Tuple

logger = logging.getLogger('snuba.snapshot-catchup')


class CatchingUpState(State[StateOutput, StateData]):
    """
    In this state the consumer consumes the main topic but
    it discards the transacitons that were present in the
    snapshot (xid < xmax and not in xip_list).
    Once this phase is done the consumer goes back to normal
    consumption.
    """

    def __init__(
        self,
        consumer_builder: Callable[
            [Callable[
                [Dataset, Producer, Optional[str], Optional[Any]],
                ConsumerWorker]
            ],
            Consumer],
    ) -> None:
        super(CatchingUpState, self).__init__()
        self.__consumer_builder = consumer_builder
        self.__consumer = None

    def set_shutdown(self) -> None:
        super().set_shutdown()
        if self.__consumer:
            self.__consumer.signal_shutdown()

    def handle(self, state_data: StateData) -> Tuple[StateOutput, StateData]:
        assert isinstance(input, dict)
        snapshot_id = input["snapshot_id"]
        transaction_data = input["transaction_data"]

        def build_worker(
            dataset: Dataset,
            producer: Producer,
            replacements_topic: Optional[str],
            metrics: Optional[Any],
        ) -> ConsumerWorker:
            return SnapshotAwareWorker(
                dataset=dataset,
                producer=producer,
                snapshot_id=snapshot_id,
                transaction_data=transaction_data,
                replacements_topic=replacements_topic,
                metrics=metrics,
            )

        self.__consumer = self.__consumer_builder(
            worker_builder=build_worker,
        )

        self.__consumer.run()
        return (StateOutput.FINISH, None)
