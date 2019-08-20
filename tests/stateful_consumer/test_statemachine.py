from typing import Any, Mapping, Set, Tuple

from snuba.stateful_consumer import StateType
from snuba.stateful_consumer.state_context import StateContext, State


class State1(State[StateType]):
    def __init__(self, processed_states: Mapping[str, bool]) -> None:
        super(State1, self).__init__()
        self.__processed_state = processed_states

    def handle(self, input: Any) -> Tuple[StateType, Any]:
        assert input == "start"
        self.__processed_state[StateType.BOOTSTRAP] = True
        return (StateType.CONSUMING, "consume")


class State2(State[StateType]):
    def __init__(self, processed_states: Mapping[str, bool]) -> None:
        super(State2, self).__init__()
        self.__processed_state = processed_states

    def handle(self, input: Any) -> Tuple[StateType, Any]:
        assert input == "consume"
        self.__processed_state[StateType.CONSUMING] = True
        return (StateType.FINISHED, None)


class TestContext(StateContext[StateType]):
    def __init__(self, processed_states: Set[StateType]):
        super(TestContext, self).__init__(
            states={
                StateType.BOOTSTRAP: State1(processed_states),
                StateType.CONSUMING: State2(processed_states),
            },
            start_state=StateType.BOOTSTRAP,
            terminal_state=StateType.FINISHED,
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
