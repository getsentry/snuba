from typing import Mapping, Set, Tuple

from snuba.stateful_consumer import ConsumerStateData, ConsumerStateCompletionEvent, ConsumerStateType
from snuba.utils.state_machine import StateMachine, State, StateType


class State1(State[ConsumerStateCompletionEvent, ConsumerStateData]):
    def __init__(self, processed_states: Mapping[str, bool]) -> None:
        super(State1, self).__init__()
        self.__processed_state = processed_states

    def signal_shutdown(self) -> None:
        pass

    def handle(self, state_data: ConsumerStateData) -> Tuple[ConsumerStateCompletionEvent, ConsumerStateData]:
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


class TestContext(StateMachine[ConsumerStateType, ConsumerStateCompletionEvent, ConsumerStateData]):
    def __init__(self, processed_states: Set[ConsumerStateType]):
        self.__processed_state = processed_states
        super(TestContext, self).__init__(
            definition={
                State1: {
                    ConsumerStateCompletionEvent.NO_SNAPSHOT: State2,
                },
                State2: {
                    ConsumerStateCompletionEvent.CONSUMPTION_COMPLETED: None,
                },
            },
            start_state=State1,
        )

    def _build_state(
        self,
        state_class: StateType,
    ) -> State[ConsumerStateCompletionEvent, ConsumerStateData]:
        return state_class(self.__processed_state)


class TestStateMachine:
    def test_states(self) -> None:
        processed_states = {}
        context = TestContext(
            processed_states
        )

        context.run()
        assert processed_states == {
            ConsumerStateType.BOOTSTRAP: True,
            ConsumerStateType.CONSUMING: True,
        }
