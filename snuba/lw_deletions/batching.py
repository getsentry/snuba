from __future__ import annotations

import time
from typing import Callable, Generic, MutableSequence, Optional, TypeVar, Union, cast

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.processing.strategies.buffer import Buffer
from arroyo.types import BaseValue, FilteredPayload, Message, TStrategyPayload, Value

ValuesBatch = MutableSequence[BaseValue[TStrategyPayload]]


TPayload = TypeVar("TPayload")
TResult = TypeVar("TResult")


Accumulator = Callable[[TResult, BaseValue[TPayload]], TResult]


class ReduceRowsBuffer(Generic[TPayload, TResult]):
    def __init__(
        self,
        accumulator: Accumulator[TResult, TPayload],
        initial_value: Callable[[], TResult],
        max_batch_size: int,
        max_batch_time: float,
        increment_by: Optional[Callable[[BaseValue[TPayload]], int]] = None,
    ):
        self.accumulator = accumulator
        self.initial_value = initial_value
        self.max_batch_size = max_batch_size
        self.max_batch_time = max_batch_time
        self.increment_by = increment_by

        self._buffer = initial_value()
        self._buffer_size = 0
        self._buffer_until = time.time() + max_batch_time

    @property
    def buffer(self) -> TResult:
        return self._buffer

    @property
    def is_empty(self) -> bool:
        return self._buffer_size == 0

    @property
    def is_ready(self) -> bool:
        return self._buffer_size >= self.max_batch_size or time.time() >= self._buffer_until

    def append(self, message: BaseValue[TPayload]) -> None:
        """
        Instead of increasing the buffer size based on the number
        of messages, we use the `rows_to_delete` attribute in the
        message payload so we can batch by the number of rows we
        want to delete.
        """
        self._buffer = self.accumulator(self._buffer, message)
        if self.increment_by:
            buffer_increment = self.increment_by(message)
        else:
            buffer_increment = 1
        self._buffer_size += buffer_increment

    def new(self) -> "ReduceRowsBuffer[TPayload, TResult]":
        return ReduceRowsBuffer(
            accumulator=self.accumulator,
            initial_value=self.initial_value,
            max_batch_size=self.max_batch_size,
            max_batch_time=self.max_batch_time,
            increment_by=self.increment_by,
        )


class ReduceCustom(
    ProcessingStrategy[Union[FilteredPayload, TPayload]], Generic[TPayload, TResult]
):
    def __init__(
        self,
        max_batch_size: int,
        max_batch_time: float,
        accumulator: Accumulator[TResult, TPayload],
        initial_value: Callable[[], TResult],
        next_step: ProcessingStrategy[TResult],
        increment_by: Optional[Callable[[BaseValue[TPayload]], int]] = None,
    ) -> None:
        self.__buffer_step = Buffer(
            buffer=ReduceRowsBuffer(
                max_batch_size=max_batch_size,
                max_batch_time=max_batch_time,
                accumulator=accumulator,
                initial_value=initial_value,
                increment_by=increment_by,
            ),
            next_step=next_step,
        )

    def submit(self, message: Message[Union[FilteredPayload, TPayload]]) -> None:
        self.__buffer_step.submit(message)

    def poll(self) -> None:
        self.__buffer_step.poll()

    def close(self) -> None:
        self.__buffer_step.close()

    def terminate(self) -> None:
        self.__buffer_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__buffer_step.join(timeout)


class NoBatchStep(ProcessingStrategy[Union[FilteredPayload, TStrategyPayload]]):
    def __init__(
        self,
        next_step: ProcessingStrategy[ValuesBatch[TStrategyPayload]],
    ) -> None:
        self.__next_step = next_step

    def submit(self, message: Message[Union[FilteredPayload, TStrategyPayload]]) -> None:
        if isinstance(message.payload, FilteredPayload):
            return
        value = cast(BaseValue[TStrategyPayload], message.value)
        self.__next_step.submit(Message(Value([value], value.committable)))

    def poll(self) -> None:
        self.__next_step.poll()

    def close(self) -> None:
        self.__next_step.close()

    def terminate(self) -> None:
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.join(timeout)


class BatchStepCustom(ProcessingStrategy[Union[FilteredPayload, TStrategyPayload]]):
    def __init__(
        self,
        max_batch_size: int,
        max_batch_time: float,
        next_step: ProcessingStrategy[ValuesBatch[TStrategyPayload]],
        increment_by: Optional[Callable[[BaseValue[TStrategyPayload]], int]] = None,
    ) -> None:
        def accumulator(
            result: ValuesBatch[TStrategyPayload], value: BaseValue[TStrategyPayload]
        ) -> ValuesBatch[TStrategyPayload]:
            result.append(value)
            return result

        self.__reduce_step: ReduceCustom[TStrategyPayload, ValuesBatch[TStrategyPayload]] = (
            ReduceCustom(
                max_batch_size,
                max_batch_time,
                accumulator,
                lambda: [],
                next_step,
                increment_by,
            )
        )

    def submit(self, message: Message[Union[FilteredPayload, TStrategyPayload]]) -> None:
        self.__reduce_step.submit(message)

    def poll(self) -> None:
        self.__reduce_step.poll()

    def close(self) -> None:
        self.__reduce_step.close()

    def terminate(self) -> None:
        self.__reduce_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__reduce_step.join(timeout)
