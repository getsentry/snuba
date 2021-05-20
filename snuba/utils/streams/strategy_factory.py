from abc import abstractmethod
from typing import Callable, Mapping, Optional, Protocol, TypeVar

from snuba.utils.streams.backends.kafka import KafkaPayload
from snuba.utils.streams.metrics import DummyMetricsBackend, Metrics, MetricsWrapper
from snuba.utils.streams.processing.strategies import (
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from snuba.utils.streams.processing.strategies.streaming import (
    CollectStep,
    FilterStep,
    ParallelTransformStep,
    TransformStep,
)
from snuba.utils.streams.types import Message, Partition


class StreamMessageFilter(Protocol):
    """
    A filter over messages coming from a stream. Can be used to pre filter
    messages during consumption but potentially for other use cases as well.
    """

    @abstractmethod
    def should_drop(self, message: Message[KafkaPayload]) -> bool:
        raise NotImplementedError


TProcessed = TypeVar("TProcessed")


class StreamingConsumerStrategyFactory(ProcessingStrategyFactory[KafkaPayload]):
    def __init__(
        self,
        prefilter: Optional[StreamMessageFilter],
        process_message: Callable[[Message[KafkaPayload]], TProcessed],
        collector: Callable[[], ProcessingStrategy[TProcessed]],
        max_batch_size: int,
        max_batch_time: float,
        processes: Optional[int],
        input_block_size: Optional[int],
        output_block_size: Optional[int],
        metrics: Metrics = DummyMetricsBackend,
    ) -> None:
        self.__prefilter = prefilter
        self.__process_message = process_message
        self.__collector = collector

        self.__max_batch_size = max_batch_size
        self.__max_batch_time = max_batch_time

        if processes is not None:
            assert input_block_size is not None, "input block size required"
            assert output_block_size is not None, "output block size required"
        else:
            assert (
                input_block_size is None
            ), "input block size cannot be used without processes"
            assert (
                output_block_size is None
            ), "output block size cannot be used without processes"

        self.__processes = processes
        self.__input_block_size = input_block_size
        self.__output_block_size = output_block_size
        self.__metrics = metrics

    def __should_accept(self, message: Message[KafkaPayload]) -> bool:
        assert self.__prefilter is not None
        return not self.__prefilter.should_drop(message)

    def create(
        self, commit: Callable[[Mapping[Partition, int]], None]
    ) -> ProcessingStrategy[KafkaPayload]:
        collect = CollectStep(
            self.__collector, commit, self.__max_batch_size, self.__max_batch_time,
        )

        transform_function = self.__process_message

        strategy: ProcessingStrategy[KafkaPayload]
        if self.__processes is None:
            strategy = TransformStep(transform_function, collect)
        else:
            assert self.__input_block_size is not None
            assert self.__output_block_size is not None
            strategy = ParallelTransformStep(
                transform_function,
                collect,
                self.__processes,
                max_batch_size=self.__max_batch_size,
                max_batch_time=self.__max_batch_time,
                input_block_size=self.__input_block_size,
                output_block_size=self.__output_block_size,
                metrics=MetricsWrapper(self.__metrics, "process"),
            )

        if self.__prefilter is not None:
            strategy = FilterStep(self.__should_accept, strategy)

        return strategy
