from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Mapping

import logging


logger = logging.getLogger('snuba.state-machine')


class StateType(Enum):
    BOOTSTRAP = 0
    CONSUMING = 1
    SNAPSHOT_PAUSED = 2
    CATCHING_UP = 3
    FINISHED = 4


class State(ABC):
    """
    Encapsulates the logic of one state the consumer can be into
    during its work.
    There should be an implementation for every value in StateType.
    The state specific logic stays in the handle_impl method.
    That method returns the state to transition to at the end of the
    operation together with any context information the next state
    needs.
    """

    def __init__(self):
        self._shutdown = False

    def set_shutdown(self) -> None:
        self._shutdown = True

    def is_shutdown(self) -> bool:
        return self._shutdown

    def handle(self, input: Any) -> (StateType, Any):
        if self.is_shutdown():
            return (StateType.FINISHED, None)
        next_state, input = self._handle_impl(input)
        if self.is_shutdown():
            return (StateType.FINISHED, None)
        return (next_state, input)

    @abstractmethod
    def _handle_impl(self, input: Any) -> (StateType, Any):
        raise NotImplementedError


class ConsumerContext:
    """
    State pattern implementation used to change the logic
    of the consumer depending on which phase it is serving.
    This class coordinates the state changes.
    """

    def __init__(
        self,
        states: Mapping[StateType, State],
        start_state: StateType,
    ) -> None:
        self.__states = states
        self.__current_state = start_state

    def run(self, input: Any) -> None:
        logger.debug("Starting state machine")
        while self.__current_state != StateType.FINISHED:
            next_state, input = self.__states[self.__current_state].handle(input)
            logger.debug("Transitioning to state %r", next_state)
            self.__current_state = next_state
        logger.debug("Finishing state machine processing")

    def set_shutdown(self) -> None:
        logger.debug("Shutting down state machine")
        self.__states[self.__current_state].set_shutdown()
