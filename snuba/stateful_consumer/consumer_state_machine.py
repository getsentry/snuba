from batching_kafka_consumer import BatchingKafkaConsumer

from snuba.stateful_consumer import ConsumerStateData, ConsumerStateCompletionEvent
from snuba.utils.state_machine import State, StateType, StateMachine
from snuba.stateful_consumer.states.bootstrap import BootstrapState
from snuba.stateful_consumer.states.consuming import ConsumingState
from snuba.stateful_consumer.states.paused import PausedState
from snuba.stateful_consumer.states.catching_up import CatchingUpState


class ConsumerStateMachine(StateMachine[ConsumerStateCompletionEvent, ConsumerStateData]):
    """
    Context class for the stateful consumer. The states defined here
    regulate when the consumer is consuming from the main topic and when
    it is consuming from the control topic.
    """

    def __init__(
        self,
        main_consumer: BatchingKafkaConsumer,
    ) -> None:
        self.__main_consumer = main_consumer
        super(ConsumerStateMachine, self).__init__(
            definition={
                BootstrapState: {
                    ConsumerStateCompletionEvent.NO_SNAPSHOT: ConsumingState,
                    ConsumerStateCompletionEvent.SNAPSHOT_INIT_RECEIVED: PausedState,
                    ConsumerStateCompletionEvent.SNAPSHOT_READY_RECEIVED: CatchingUpState,
                },
                ConsumingState: {
                    ConsumerStateCompletionEvent.CONSUMPTION_COMPLETED: None,
                    ConsumerStateCompletionEvent.SNAPSHOT_INIT_RECEIVED: PausedState,
                },
                PausedState: {
                    ConsumerStateCompletionEvent.CONSUMPTION_COMPLETED: None,
                    ConsumerStateCompletionEvent.SNAPSHOT_READY_RECEIVED: CatchingUpState,
                },
                CatchingUpState: {
                    ConsumerStateCompletionEvent.CONSUMPTION_COMPLETED: None,
                    ConsumerStateCompletionEvent.SNAPSHOT_CATCHUP_COMPLETED: ConsumingState,
                },
            },
            start_state=BootstrapState,
        )

    def _build_state(
        self,
        state_class: StateType,
    ) -> State[ConsumerStateCompletionEvent, ConsumerStateData]:
        if state_class == ConsumingState:
            return ConsumingState(self.__main_consumer)
        else:
            return state_class()
