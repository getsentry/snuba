import logging
from typing import Callable, Optional

from arroyo.processing.strategies.abstract import ProcessingStrategy as ProcessingStep
from arroyo.types import Message, TPayload

logger = logging.getLogger(__name__)


class FilterStep(ProcessingStep[TPayload]):
    """
    Determines if a message should be submitted to the next processing step.
    """

    def __init__(
        self,
        function: Callable[[Message[TPayload]], bool],
        next_step: ProcessingStep[TPayload],
    ):
        self.__test_function = function
        self.__next_step = next_step

        self.__closed = False

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[TPayload]) -> None:
        assert not self.__closed

        if self.__test_function(message):
            self.__next_step.submit(message)

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True

        logger.debug("Terminating %r...", self.__next_step)
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.close()
        self.__next_step.join(timeout)
