from batching_kafka_consumer import BatchingKafkaConsumer

from snuba.stateful_consumer import StateType
from snuba.stateful_consumer.state_context import StateContext
from snuba.stateful_consumer.bootstrap_state import BootstrapState
from snuba.stateful_consumer.consuming_state import ConsumingState
from snuba.stateful_consumer.paused_state import PausedState
from snuba.stateful_consumer.catching_up_state import CatchingUpState


class ConsumerContext(StateContext[StateType]):
    """
    Context class for the stateful consumer. The states defined here
    regulate when the consumer is consuming from the main topic and when
    it is consuming from the control topic.
    """

    def __init__(
        self,
        main_consumer: BatchingKafkaConsumer,
    ) -> None:
        states = {
            StateType.BOOTSTRAP: BootstrapState(),
            StateType.CONSUMING: ConsumingState(main_consumer),
            StateType.SNAPSHOT_PAUSED: PausedState(),
            StateType.CATCHING_UP: CatchingUpState(),
        }
        start_state = StateType.BOOTSTRAP
        terminal_state = StateType.FINISHED
        super(ConsumerContext, self).__init__(
            states=states,
            start_state=start_state,
            terminal_state=terminal_state
        )
