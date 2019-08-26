from abc import ABC, abstractmethod
from typing import Generic, Mapping, TypeVar, Tuple

import logging


logger = logging.getLogger('snuba.state-machine')

TStateType = TypeVar('TStateType')
TStateResult = TypeVar('TStateResult')
TStateData = TypeVar('TStateData')


class State(Generic[TStateResult, TStateData]):
    """
    Encapsulates the state specific logic in the state pattern.
    Subclasses may implement their own handle method.
    """

    def __init__(self):
        self._shutdown = False

    def set_shutdown(self) -> None:
        """
        Communicate in a non preemptive way to the state that it is time
        to shut down. It is up to the state itself to decide what to do
        with this.
        """
        self._shutdown = True

    @abstractmethod
    def handle(self, state_data: TStateData) -> Tuple[TStateResult, TStateData]:
        """
        Implemented by each state. It runs its own state specific logic and
        returns a tuple that contains the next state type to go to and any context
        data that will be passed to that state.
        """
        raise NotImplementedError


class StateContext(Generic[TStateType], ABC):
    """
    State pattern implementation used to change the logic
    of the consumer depending on which phase it is serving.
    This class coordinates the state changes.
    """

    def __init__(
        self,
        states: Mapping[TStateType, State],
        start_state: TStateType,
        terminal_state: TStateType,
    ) -> None:
        self.__states = states
        self.__current_state = start_state
        self.__terminal_state = terminal_state
        self.__shutdown = False

    def run(self, initial_data: TStateData) -> None:
        logger.debug("Starting state machine")
        nest_state_data = initial_data
        while self.__current_state != self.__terminal_state:
            state_output, nest_state_data = self.__states[self.__current_state] \
                .handle(nest_state_data)
            next_state = self.__resolve_next_state(
                self.__current_state,
                state_output,
            )
            if self.__shutdown:
                next_state = self.__terminal_state
            logger.debug("Transitioning to state %r", next_state)
            self.__current_state = next_state
        logger.debug("Finishing state machine processing")

    def set_shutdown(self) -> None:
        logger.debug("Shutting down state machine")
        self.__states[self.__current_state].set_shutdown()
        self.__shutdown = True

    def __resolve_next_state(
        self,
        current_state: TStateType,
        output: TStateResult,
    ) -> TStateType:
        state_map = self._get_state_transitions()
        if current_state not in state_map:
            raise ValueError("ConsumerContext does not know about state %r" % current_state)

        current_state_map = state_map[current_state]
        if output not in current_state_map:
            raise ValueError("No valid transition from state %r with output %r." % (current_state, output))

        return state_map[current_state][output]

    @abstractmethod
    def _get_state_transitions(self) -> Mapping[TStateType, Mapping[TStateResult, TStateType]]:
        """
        Returns a map that represents the valid state transitions for the state machine.
        Every entry is represented by a current state and a map of output (from the current
        state) to the next states.
        """
        raise NotImplementedError
