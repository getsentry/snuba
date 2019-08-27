from abc import ABC, abstractmethod
from typing import Generic, Mapping, Optional, TypeVar, Tuple

import logging


logger = logging.getLogger('snuba.state-machine')

"""Enum that identifies the state type within the state machine. """
TStateType = TypeVar('TStateType')
"""
Enum that identifies the event a state raises upon termination.
This is used by the context to resolve the next state.
"""
TStateCompletionEvent = TypeVar('TStateCompletionEvent')
"""
Any context data produced by a state during its work that has to be made
available to the following states.
"""
TStateData = TypeVar('TStateData')


class State(Generic[TStateCompletionEvent, TStateData], ABC):
    """
    Encapsulates the state specific logic in the state pattern.
    Subclasses must implement their own handle method.
    """

    @abstractmethod
    def signal_shutdown(self) -> None:
        """
        Communicate in a non preemptive way to the state that it is time
        to shut down. It is up to the state itself to decide what to do
        with this.
        """
        raise NotImplementedError

    @abstractmethod
    def handle(self, state_data: TStateData) -> Tuple[TStateCompletionEvent, TStateData]:
        """
        Implemented by each state. It runs its own state specific logic and
        returns a tuple that contains the state result, which identifies the
        event this state wants to return upon termination, and some context
        information to make available to following states.
        """
        raise NotImplementedError


StateTransitions = Mapping[TStateCompletionEvent, Optional[TStateType]]

StateMachineDefinition = Mapping[
    TStateType, Tuple[State, StateTransitions]
]


class StateContext(Generic[TStateType, TStateCompletionEvent, TStateData], ABC):
    """
    State pattern implementation used to change the logic
    of the consumer depending on which phase it is serving.
    This class coordinates the state changes.
    """

    def __init__(
        self,
        definition: StateMachineDefinition,
        start_state: TStateType,
    ) -> None:
        self.__definition = definition
        self.__current_state = start_state
        self.__shutdown = False

    def run(self, initial_data: TStateData = None) -> None:
        logger.debug("Starting state machine")
        next_state_data = initial_data
        while self.__current_state:
            state_output, next_state_data = self.__get_current_state() \
                .handle(next_state_data)
            next_state = self.__resolve_next_state(
                state_output,
            )
            if self.__shutdown:
                next_state = None
            logger.debug("Transitioning to state %r", next_state)
            self.__current_state = next_state
        logger.debug("Finishing state machine processing")

    def set_shutdown(self) -> None:
        logger.debug("Shutting down state machine")
        self.__get_current_state().signal_shutdown()
        self.__shutdown = True

    def __get_current_state(self) -> Optional[State]:
        return self.__definition[self.__current_state][0]

    def __resolve_next_state(
        self,
        output: TStateCompletionEvent,
    ) -> TStateType:
        current_state_map = self.__definition[self.__current_state][1]
        if output not in current_state_map:
            raise ValueError(f"No valid transition from state {self.__current_state} with output {output}.")

        return current_state_map[output]
