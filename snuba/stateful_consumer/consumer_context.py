from batching_kafka_consumer import BatchingKafkaConsumer

from snuba.stateful_consumer import StateData, StateType, StateCompletionEvent
from snuba.stateful_consumer.state_context import StateContext
from snuba.stateful_consumer.states.bootstrap import BootstrapState
from snuba.stateful_consumer.states.consuming import ConsumingState
from snuba.stateful_consumer.states.paused import PausedState
from snuba.stateful_consumer.states.catching_up import CatchingUpState

<< << << < HEAD
from typing import Mapping, Sequence
== == == =
>>>>>> > feat / statefulConsumer


class ConsumerContext(StateContext[StateType, StateCompletionEvent, StateData]):
    """
    Context class for the stateful consumer. The states defined here
    regulate when the consumer is consuming from the main topic and when
    it is consuming from the control topic.
    """

    def __init__(
        self,
        main_consumer: BatchingKafkaConsumer,
        topic: str,
        bootstrap_servers: Sequence[str],
        group_id: str,
    ) -> None:
        bootstrap_state = BootstrapState(
            topic,
            bootstrap_servers,
            group_id,
        )
        super(ConsumerContext, self).__init__(
            definition={
                StateType.BOOTSTRAP: (bootstrap_state, {
                    StateCompletionEvent.NO_SNAPSHOT: StateType.CONSUMING,
                    StateCompletionEvent.SNAPSHOT_INIT_RECEIVED: StateType.SNAPSHOT_PAUSED,
                    StateCompletionEvent.SNAPSHOT_READY_RECEIVED: StateType.CATCHING_UP,
                }),
                StateType.CONSUMING: (ConsumingState(main_consumer), {
                    StateCompletionEvent.CONSUMPTION_COMPLETED: None,
                    StateCompletionEvent.SNAPSHOT_INIT_RECEIVED: StateType.SNAPSHOT_PAUSED,
                }),
                StateType.SNAPSHOT_PAUSED: (PausedState(), {
                    StateCompletionEvent.CONSUMPTION_COMPLETED: None,
                    StateCompletionEvent.SNAPSHOT_READY_RECEIVED: StateType.CATCHING_UP,
                }),
                StateType.CATCHING_UP: (CatchingUpState(), {
                    StateCompletionEvent.CONSUMPTION_COMPLETED: None,
                    StateCompletionEvent.SNAPSHOT_CATCHUP_COMPLETED: StateType.CONSUMING,
                }),
            },
            start_state=StateType.BOOTSTRAP,
        )
