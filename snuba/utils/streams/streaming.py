from typing import Callable, Optional, TypeVar

from snuba.utils.streams.processing import ProcessingStrategy
from snuba.utils.streams.types import Message, TPayload


ProcessingStep = ProcessingStrategy


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

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[TPayload]) -> None:
        if self.__test_function(message):
            self.__next_step.submit(message)

    def close(self) -> None:
        pass

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.close()
        self.__next_step.join(timeout)


TTransformed = TypeVar("TTransformed")


class TransformStep(ProcessingStep[TPayload]):
    """
    Transforms a message and submits the transformed value to the next
    processing step.
    """

    def __init__(
        self,
        function: Callable[[Message[TPayload]], TTransformed],
        next_step: ProcessingStep[TTransformed],
    ) -> None:
        self.__transform_function = function
        self.__next_step = next_step

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[TPayload]) -> None:
        self.__next_step.submit(
            Message(
                message.partition,
                message.offset,
                self.__transform_function(message),
                message.timestamp,
            )
        )

    def close(self) -> None:
        pass

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.close()
        self.__next_step.join(timeout)
