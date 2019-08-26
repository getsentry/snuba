from snuba.consumer_initializer import ConsumerBuiler
from snuba.stateful_consumer import StateData, StateType, StateOutput
from snuba.stateful_consumer.state_context import StateContext
from snuba.stateful_consumer.bootstrap_state import BootstrapState
from snuba.stateful_consumer.consuming_state import ConsumingState
from snuba.stateful_consumer.paused_state import PausedState
from snuba.stateful_consumer.catching_up_state import CatchingUpState

from typing import Mapping, Sequence


class ConsumerContext(StateContext[StateType, StateOutput, StateData]):
    """
    Context class for the stateful consumer. The states defined here
    regulate when the consumer is consuming from the main topic and when
    it is consuming from the control topic.
    """

    def __init__(
        self,
        consumer_builder: ConsumerBuiler,
        group_id: str,
        bootstrap_servers: Sequence[str],
        control_topic: str,
    ) -> None:
        states = {
            StateType.BOOTSTRAP: BootstrapState(
                control_topic,
                bootstrap_servers,
                group_id,
            ),
            StateType.CONSUMING: ConsumingState(consumer_builder),
            StateType.SNAPSHOT_PAUSED: PausedState(),
            StateType.CATCHING_UP: CatchingUpState(consumer_builder),
        }
        start_state = StateType.BOOTSTRAP
        terminal_state = StateType.FINISHED
        super(ConsumerContext, self).__init__(
            states=states,
            start_state=start_state,
            terminal_state=terminal_state
        )

    def _get_state_transitions(self) -> Mapping[StateType, Mapping[StateOutput, StateType]]:
        return {
            StateType.BOOTSTRAP: {
                StateOutput.NO_SNAPSHOT: StateType.CONSUMING,
                StateOutput.SNAPSHOT_INIT_RECEIVED: StateType.SNAPSHOT_PAUSED,
                StateOutput.SNAPSHOT_READY_RECEIVED: StateType.CATCHING_UP,
            },
            StateType.CONSUMING: {
                StateOutput.FINISH: StateType.FINISHED,
                StateOutput.SNAPSHOT_INIT_RECEIVED: StateType.SNAPSHOT_PAUSED,
            },
            StateType.SNAPSHOT_PAUSED: {
                StateOutput.FINISH: StateType.FINISHED,
                StateOutput.SNAPSHOT_READY_RECEIVED: StateType.CATCHING_UP,
            },
            StateType.CATCHING_UP: {
                StateOutput.FINISH: StateType.FINISHED,
                StateOutput.SNAPSHOT_CATCHUP_COMPLETED: StateType.CONSUMING,
            },
        }
