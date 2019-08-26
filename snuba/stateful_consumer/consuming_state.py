from confluent_kafka import Producer, Consumer

from snuba.consumer import ConsumerWorker
from snuba.datasets import Dataset
from snuba.stateful_consumer import StateOutput
from snuba.stateful_consumer.state_context import State

from typing import Any, Callable, Optional, Tuple


class ConsumingState(State[StateOutput, StateData]):
    """
    This is the normal operation state where the consumer
    reads from the main topic (cdc in this case) and sends
    messages to the processor.
    It can transition to paused state when a snapshot process
    starts.
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
        super(ConsumingState, self).__init__()

        def build_worker(
            dataset: Dataset,
            producer: Producer,
            replacements_topic: Optional[str],
            metrics: Optional[Any],
        ) -> ConsumerWorker:
            return ConsumerWorker(
                dataset,
                producer=producer,
                replacements_topic=replacements_topic,
                metrics=metrics,
            )
        self.__consumer = consumer_builder(
            worker_builder=build_worker,
        )

    def set_shutdown(self) -> None:
        super().set_shutdown()
        self.__consumer.signal_shutdown()

    def handle(self, state_data: StateData) -> Tuple[StateOutput, StateData]:
        self.__consumer.run()
        return (StateOutput.FINISH, None)
