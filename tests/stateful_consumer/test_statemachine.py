from typing import Any, Mapping

from snuba.stateful_consumer.consumer_context import ConsumerContext, State, StateType


class State1(State):
    def __init__(self, processed_states: Mapping[str, bool]) -> None:
        super(State1, self).__init__()
        self.__processed_state = processed_states

    def _handle_impl(self, input: Any) -> (StateType, Any):
        assert input == "start"
        self.__processed_state[StateType.BOOTSTRAP] = True
        return (StateType.CONSUMING, "consume")


class State2(State):
    def __init__(self, processed_states: Mapping[str, bool]) -> None:
        super(State2, self).__init__()
        self.__processed_state = processed_states

    def _handle_impl(self, input: Any) -> (StateType, Any):
        assert input == "consume"
        self.__processed_state[StateType.CONSUMING] = True
        return (StateType.FINISHED, None)


class TestStateMachine:
    def test_states(self) -> None:
        processed_states = {}
        context = ConsumerContext(
            states={
                StateType.BOOTSTRAP: State1(processed_states),
                StateType.CONSUMING: State2(processed_states),
            },
            start_state=StateType.BOOTSTRAP
        )

        context.run("start")
        assert processed_states == {
            StateType.BOOTSTRAP: True,
            StateType.CONSUMING: True,
        }
