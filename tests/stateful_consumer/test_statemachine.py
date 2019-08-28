from typing import Mapping, Set, Tuple

from snuba.stateful_consumer import ConsumerStateData, ConsumerStateCompletionEvent, ConsumerStateType
from snuba.utils.state_machine import StateContext, State


class State1(State[ConsumerStateCompletionEvent, ConsumerStateData]):
    def __init__(self, processed_states: Mapping[str, bool]) -> None:
        super(State1, self).__init__()
        self.__processed_state = processed_states

    def signal_shutdown(self) -> None:
        pass

    def handle(self, state_data: ConsumerStateData) -> Tuple[ConsumerStateCompletionEvent, ConsumerStateData]:
        assert state_data == "start"
        self.__processed_state[ConsumerStateType.BOOTSTRAP] = True
        return (ConsumerStateCompletionEvent.NO_SNAPSHOT, "consume")


class State2(State[ConsumerStateCompletionEvent, ConsumerStateData]):
    def __init__(self, processed_states: Mapping[str, bool]) -> None:
        super(State2, self).__init__()
        self.__processed_state = processed_states

    def signal_shutdown(self) -> None:
        pass

    def handle(self, state_data: ConsumerStateData) -> Tuple[ConsumerStateCompletionEvent, ConsumerStateData]:
        assert state_data == "consume"
        self.__processed_state[ConsumerStateType.CONSUMING] = True
        return (ConsumerStateCompletionEvent.CONSUMPTION_COMPLETED, None)


class TestContext(StateContext[ConsumerStateType, ConsumerStateCompletionEvent, ConsumerStateData]):
    def __init__(self, processed_states: Set[ConsumerStateType]):
        super(TestContext, self).__init__(
            definition={
                ConsumerStateType.BOOTSTRAP: (State1(processed_states), {
                    ConsumerStateCompletionEvent.NO_SNAPSHOT: ConsumerStateType.CONSUMING,
                }),
                ConsumerStateType.CONSUMING: (State2(processed_states), {
                    ConsumerStateCompletionEvent.CONSUMPTION_COMPLETED: None,
                }),
            },
            start_state=ConsumerStateType.BOOTSTRAP,
        )


class TestStateMachine:
    def test_states(self) -> None:
        processed_states = {}
        context = TestContext(
            processed_states
        )

        context.run("start")
        assert processed_states == {
            ConsumerStateType.BOOTSTRAP: True,
            ConsumerStateType.CONSUMING: True,
        }
