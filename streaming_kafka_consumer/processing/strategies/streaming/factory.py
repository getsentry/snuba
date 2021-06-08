from abc import abstractmethod
from typing import Callable, Mapping, Optional, Protocol, TypeVar

from streaming_kafka_consumer.backends.kafka import KafkaPayload
from streaming_kafka_consumer.metrics import (
    DummyMetricsBackend,
    Metrics,
    MetricsWrapper,
)
from streaming_kafka_consumer.processing.strategies import (
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from streaming_kafka_consumer.processing.strategies.streaming.collect import CollectStep
from streaming_kafka_consumer.processing.strategies.streaming.filter import FilterStep
from streaming_kafka_consumer.processing.strategies.streaming.transform import (
    ParallelTransformStep,
    TransformStep,
)
from streaming_kafka_consumer.types import Message, Partition

TPayload = TypeVar("TPayload")
TProcessed = TypeVar("TProcessed")


class StreamMessageFilter(Protocol[TPayload]):
    """
    A filter over messages coming from a stream. Can be used to pre filter
    messages during consumption but potentially for other use cases as well.
    """

    @abstractmethod
    def should_drop(self, message: Message[TPayload]) -> bool:
        raise NotImplementedError


class ConsumerStrategyFactory(ProcessingStrategyFactory[TPayload]):
    """
    Builds a three step consumer strategy consisting of filter, transform
    and collect phases.

    The `prefilter` supports passing a test function to determine whether a
    message should proceed to the next processing steps or be dropped. If no
    `prefilter` is passed, all messages will proceed through processing.

    The `process_message` function should transform a message containing the
    raw payload into a processed one for the collector. If a value is passed
    for `processes` then this step will switch to the parallel transform
    strategy in order to use multiple processors.

    The `collector` function should return a strategy to be executed on
    batches of messages. Could be used to write messages to disk in batches.
    """

    def __init__(
        self,
        prefilter: Optional[StreamMessageFilter[TPayload]],
        process_message: Callable[[Message[TPayload]], TProcessed],
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

    def __should_accept(self, message: Message[TPayload]) -> bool:
        assert self.__prefilter is not None
        return not self.__prefilter.should_drop(message)

    def create(
        self, commit: Callable[[Mapping[Partition, int]], None]
    ) -> ProcessingStrategy[TPayload]:
        collect = CollectStep(
            self.__collector, commit, self.__max_batch_size, self.__max_batch_time,
        )

        transform_function = self.__process_message

        strategy: ProcessingStrategy[TPayload]
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


class KafkaConsumerStrategyFactory(ConsumerStrategyFactory[KafkaPayload]):
    pass
