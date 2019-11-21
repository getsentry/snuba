from abc import ABC, abstractmethod
from typing import Generic, Mapping, Optional, Type, TypeVar, Tuple, Union

import logging


logger = logging.getLogger("snuba.state-machine")


# Enum that identifies the event a state raises upon termination.
# This is used by the context to resolve the next state.
TStateCompletionEvent = TypeVar("TStateCompletionEvent")


# Any context data produced by a state during its work that has to be made
# available to the following states.
TStateData = TypeVar("TStateData")


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
    def handle(
        self, state_data: Union[TStateData, None]
    ) -> Tuple[TStateCompletionEvent, TStateData]:
        """
        Implemented by each state. It runs its own state specific logic and
        returns a tuple that contains the state result, which identifies the
        event this state wants to return upon termination, and some context
        information to make available to following states.
        """
        raise NotImplementedError


StateType = Type[State[TStateCompletionEvent, TStateData]]

StateTransitions = Mapping[
    TStateCompletionEvent, Union[StateType[TStateCompletionEvent, TStateData], None]
]

StateMachineDefinition = Mapping[
    StateType[TStateCompletionEvent, TStateData],
    StateTransitions[TStateCompletionEvent, TStateData],
]


class StateMachine(Generic[TStateCompletionEvent, TStateData], ABC):
    """
    State pattern implementation used to change the logic
    of a component depending the state the component is into.
    This class coordinates the state transitions.
    Subclasses are use case specific and provide the concrete
    types for TStateData and TStateCompletionEvent.
    """

    def __init__(
        self,
        definition: StateMachineDefinition[TStateCompletionEvent, TStateData],
        start_state: StateType[TStateCompletionEvent, TStateData],
    ) -> None:
        self.__definition = definition
        self.__current_state_type: Union[
            StateType[TStateCompletionEvent, TStateData], None
        ] = start_state
        self.__has_shutdown = False
        self.__current_state: Optional[State[TStateCompletionEvent, TStateData]] = None

    def run(self) -> None:
        """
        Execute the state machine starting from the start_state
        and does not stop until the state machine does not reach
        a final state (None).

        It processes the shutdown state at every state transition.
        No state processing is preempted via shutdown.

        TODO: support use cases where a stat ecan stop this processing
        waiting for new input without reaching the terminal state.
        """

        logger.debug("Starting state machine")
        state_data = None

        while self.__current_state_type is not None:
            current_state = self._build_state(self.__current_state_type,)
            self.__current_state = current_state
            event, state_data = current_state.handle(state_data)

            if self.__has_shutdown:
                next_state_type = None
                break

            current_state_map = self.__definition[self.__current_state_type]
            if event not in current_state_map:
                raise ValueError(
                    f"No valid transition from state {self.__current_state_type} with event {event}."
                )

            next_state_type = current_state_map[event]

            logger.debug("Transitioning to state %r", next_state_type)
            self.__current_state_type = next_state_type

        logger.debug("Finishing state machine processing")

    def signal_shutdown(self) -> None:
        """
        Communicate in a non preemptive way to the state machine that it
        is time to shut down. This status is checked at every state
        transition.
        """
        logger.debug("Shutting down state machine")
        if self.__current_state is not None:
            self.__current_state.signal_shutdown()
        self.__has_shutdown = True

    @abstractmethod
    def _build_state(
        self, state_class: StateType[TStateCompletionEvent, TStateData],
    ) -> State[TStateCompletionEvent, TStateData]:
        """
        Factory to provide implementations of the state given
        the type (which is the state class name).
        """
        raise NotImplementedError
