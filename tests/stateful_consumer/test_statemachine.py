from typing import Mapping, Set, Tuple

from snuba.stateful_consumer import StateData, StateCompletionEvent, StateType
from snuba.stateful_consumer.state_context import StateContext, State


class State1(State[StateCompletionEvent, StateData]):
    def __init__(self, processed_states: Mapping[str, bool]) -> None:
        super(State1, self).__init__()
        self.__processed_state = processed_states

    def signal_shutdown(self) -> None:
        pass

    def handle(self, state_data: StateData) -> Tuple[StateCompletionEvent, StateData]:
        assert state_data == "start"
        self.__processed_state[StateType.BOOTSTRAP] = True
        return (StateCompletionEvent.NO_SNAPSHOT, "consume")


class State2(State[StateCompletionEvent, StateData]):
    def __init__(self, processed_states: Mapping[str, bool]) -> None:
        super(State2, self).__init__()
        self.__processed_state = processed_states

    def signal_shutdown(self) -> None:
        pass

    def handle(self, state_data: StateData) -> Tuple[StateCompletionEvent, StateData]:
        assert state_data == "consume"
        self.__processed_state[StateType.CONSUMING] = True
        return (StateCompletionEvent.CONSUMPTION_COMPLETED, None)


class TestContext(StateContext[StateType, StateCompletionEvent, StateData]):
    def __init__(self, processed_states: Set[StateType]):
        super(TestContext, self).__init__(
            definition={
                StateType.BOOTSTRAP: (State1(processed_states), {
                    StateCompletionEvent.NO_SNAPSHOT: StateType.CONSUMING,
                }),
                StateType.CONSUMING: (State2(processed_states), {
                    StateCompletionEvent.CONSUMPTION_COMPLETED: None,
                }),
            },
            start_state=StateType.BOOTSTRAP,
        )


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
