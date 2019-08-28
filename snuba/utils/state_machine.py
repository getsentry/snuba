from abc import ABC, abstractmethod
from typing import Generic, Mapping, TypeVar, Tuple, Union

import logging


logger = logging.getLogger('snuba.state-machine')

# Enum that identifies the state type within the state machine.
TStateType = TypeVar('TStateType')


# Enum that identifies the event a state raises upon termination.
# This is used by the context to resolve the next state.
TStateCompletionEvent = TypeVar('TStateCompletionEvent')


# Any context data produced by a state during its work that has to be made
# available to the following states.
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


StateTransitions = Mapping[TStateCompletionEvent, Union[TStateType, None]]

StateMachineDefinition = Mapping[
    TStateType,
    Tuple[
        State[TStateCompletionEvent, TStateData],
        StateTransitions,
    ]
]


class StateMachine(Generic[TStateType, TStateCompletionEvent, TStateData], ABC):
    """
    State pattern implementation used to change the logic
    of a component depending the state the component is into.
    This class coordinates the state transitions.
    Subclasses are use case specific and provide the concrete
    types for TStateType, TStateData and TStateCompletionEvent.
    """

    def __init__(
        self,
        definition: StateMachineDefinition,
        start_state: TStateType,
    ) -> None:
        self.__definition = definition
        self.__current_state_type: Union[TStateType, None] = start_state
        self.__has_shutdown = False

    def run(self) -> None:
        """
        Execute the state machine starting from the start_state
        and does not stop until the state machine does not reach
        a final state (None).

        It processes the shutdown state at every state transition.
        No state processing is preempted via shutdown.
        """

        logger.debug("Starting state machine")
        state_data = None

        while self.__current_state_type is not None:
            event, state_data = self.__get_current_state_object() \
                .handle(state_data)

            if self.__has_shutdown:
                next_state_type = None
                break

            current_state_map = self.__definition[self.__current_state_type][1]
            if event not in current_state_map:
                raise ValueError(f"No valid transition from state {self.__current_state_type} with event {event}.")

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
        self.__get_current_state_object().signal_shutdown()
        self.__has_shutdown = True

    def __get_current_state_object(self) -> State:
        return self.__definition[self.__current_state_type][0]
