from batching_kafka_consumer import BatchingKafkaConsumer

from snuba.stateful_consumer import ConsumerStateData, ConsumerStateType, ConsumerStateCompletionEvent
from snuba.utils.state_machine import StateContext
from snuba.stateful_consumer.states.bootstrap import BootstrapState
from snuba.stateful_consumer.states.consuming import ConsumingState
from snuba.stateful_consumer.states.paused import PausedState
from snuba.stateful_consumer.states.catching_up import CatchingUpState


class ConsumerContext(StateContext[ConsumerStateType, ConsumerStateCompletionEvent, ConsumerStateData]):
    """
    Context class for the stateful consumer. The states defined here
    regulate when the consumer is consuming from the main topic and when
    it is consuming from the control topic.
    """

    def __init__(
        self,
        main_consumer: BatchingKafkaConsumer,
    ) -> None:
        super(ConsumerContext, self).__init__(
            definition={
                ConsumerStateType.BOOTSTRAP: (BootstrapState(), {
                    ConsumerStateCompletionEvent.NO_SNAPSHOT: ConsumerStateType.CONSUMING,
                    ConsumerStateCompletionEvent.SNAPSHOT_INIT_RECEIVED: ConsumerStateType.SNAPSHOT_PAUSED,
                    ConsumerStateCompletionEvent.SNAPSHOT_READY_RECEIVED: ConsumerStateType.CATCHING_UP,
                }),
                ConsumerStateType.CONSUMING: (ConsumingState(main_consumer), {
                    ConsumerStateCompletionEvent.CONSUMPTION_COMPLETED: None,
                    ConsumerStateCompletionEvent.SNAPSHOT_INIT_RECEIVED: ConsumerStateType.SNAPSHOT_PAUSED,
                }),
                ConsumerStateType.SNAPSHOT_PAUSED: (PausedState(), {
                    ConsumerStateCompletionEvent.CONSUMPTION_COMPLETED: None,
                    ConsumerStateCompletionEvent.SNAPSHOT_READY_RECEIVED: ConsumerStateType.CATCHING_UP,
                }),
                ConsumerStateType.CATCHING_UP: (CatchingUpState(), {
                    ConsumerStateCompletionEvent.CONSUMPTION_COMPLETED: None,
                    ConsumerStateCompletionEvent.SNAPSHOT_CATCHUP_COMPLETED: ConsumerStateType.CONSUMING,
                }),
            },
            start_state=ConsumerStateType.BOOTSTRAP,
        )
