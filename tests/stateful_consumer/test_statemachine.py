from typing import Mapping, Set, Tuple

from snuba.stateful_consumer import StateData, StateOutput, StateType
from snuba.stateful_consumer.state_context import StateContext, State


class State1(State[StateOutput, StateData]):
    def __init__(self, processed_states: Mapping[str, bool]) -> None:
        super(State1, self).__init__()
        self.__processed_state = processed_states

    def handle(self, state_data: StateData) -> Tuple[StateOutput, StateData]:
        assert state_data == "start"
        self.__processed_state[StateType.BOOTSTRAP] = True
        return (StateOutput.NO_SNAPSHOT, "consume")


class State2(State[StateOutput, StateData]):
    def __init__(self, processed_states: Mapping[str, bool]) -> None:
        super(State2, self).__init__()
        self.__processed_state = processed_states

    def handle(self, state_data: StateData) -> Tuple[StateOutput, StateData]:
        assert state_data == "consume"
        self.__processed_state[StateType.CONSUMING] = True
        return (StateOutput.FINISH, None)


class TestContext(StateContext[StateType, StateOutput, StateData]):
    def __init__(self, processed_states: Set[StateType]):
        super(TestContext, self).__init__(
            states={
                StateType.BOOTSTRAP: State1(processed_states),
                StateType.CONSUMING: State2(processed_states),
            },
            start_state=StateType.BOOTSTRAP,
            terminal_state=StateType.FINISHED,
        )

    def _get_state_transitions(self) -> Mapping[StateType, Mapping[StateOutput, StateType]]:
        return {
            StateType.BOOTSTRAP: {
                StateOutput.NO_SNAPSHOT: StateType.CONSUMING
            },
            StateType.CONSUMING: {
                StateOutput.FINISH: StateType.FINISHED
            },
        }


class TestStateMachine:
    def test_states(self) -> None:
        processed_states = {}
        context = TestContext(
            processed_states
        )

        context.run("start")
        assert processed_states == {
            StateType.BOOTSTRAP: True,
            StateType.CONSUMING: True,
        }
