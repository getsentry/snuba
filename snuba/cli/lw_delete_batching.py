from __future__ import annotations

import time
from typing import Callable, Generic, MutableSequence, Optional, TypeVar, Union

import rapidjson
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.processing.strategies.buffer import Buffer
from arroyo.types import BaseValue, FilteredPayload, Message, TStrategyPayload

ValuesBatch = MutableSequence[BaseValue[TStrategyPayload]]


TPayload = TypeVar("TPayload")
TResult = TypeVar("TResult")


Accumulator = Callable[[TResult, BaseValue[TPayload]], TResult]


class ReduceRowsBuffer(Generic[TPayload, TResult]):
    """Reduce strategy buffer class."""

    def __init__(
        self,
        accumulator: Accumulator[TResult, TPayload],
        initial_value: Callable[[], TResult],
        max_batch_size: int,
        max_batch_time: float,
    ):
        self.accumulator = accumulator
        self.initial_value = initial_value
        self.max_batch_size = max_batch_size
        self.max_batch_time = max_batch_time

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
        return (
            self._buffer_size >= self.max_batch_size
            or time.time() >= self._buffer_until
        )

    def append(self, message: BaseValue[TPayload]) -> None:
        self._buffer = self.accumulator(self._buffer, message)
        num_rows = rapidjson.loads(message.payload.value)["num_rows"]
        self._buffer_size += int(num_rows)

    def new(self) -> "ReduceRowsBuffer[TPayload, TResult]":
        return ReduceRowsBuffer(
            accumulator=self.accumulator,
            initial_value=self.initial_value,
            max_batch_size=self.max_batch_size,
            max_batch_time=self.max_batch_time,
        )


class ReduceCustom(
    ProcessingStrategy[Union[FilteredPayload, TPayload]], Generic[TPayload, TResult]
):
    """Copy of arroyo reduce strategy"""

    def __init__(
        self,
        max_batch_size: int,
        max_batch_time: float,
        accumulator: Accumulator[TResult, TPayload],
        initial_value: Callable[[], TResult],
        next_step: ProcessingStrategy[TResult],
    ) -> None:
        self.__buffer_step = Buffer(
            buffer=ReduceRowsBuffer(
                max_batch_size=max_batch_size,
                max_batch_time=max_batch_time,
                accumulator=accumulator,
                initial_value=initial_value,
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


class BatchStepCustom(ProcessingStrategy[Union[FilteredPayload, TStrategyPayload]]):
    """Copy of arroyo batch step strategy"""

    def __init__(
        self,
        max_batch_size: int,
        max_batch_time: float,
        next_step: ProcessingStrategy[ValuesBatch[TStrategyPayload]],
    ) -> None:
        def accumulator(
            result: ValuesBatch[TStrategyPayload], value: BaseValue[TStrategyPayload]
        ) -> ValuesBatch[TStrategyPayload]:
            result.append(value)
            return result

        self.__reduce_step: ReduceCustom[
            TStrategyPayload, ValuesBatch[TStrategyPayload]
        ] = ReduceCustom(
            max_batch_size,
            max_batch_time,
            accumulator,
            lambda: [],
            next_step,
        )

    def submit(
        self, message: Message[Union[FilteredPayload, TStrategyPayload]]
    ) -> None:
        self.__reduce_step.submit(message)

    def poll(self) -> None:
        self.__reduce_step.poll()

    def close(self) -> None:
        self.__reduce_step.close()

    def terminate(self) -> None:
        self.__reduce_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__reduce_step.join(timeout)
