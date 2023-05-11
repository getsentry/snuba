from abc import abstractmethod
from typing import Callable, Mapping, Optional, Protocol, Union

from arroyo.backends.kafka import KafkaPayload
from arroyo.commit import ONCE_PER_SECOND
from arroyo.processing.strategies import (
    FilterStep,
    ProcessingStrategy,
    ProcessingStrategyFactory,
    Reduce,
    RunTaskInThreads,
)
from arroyo.processing.strategies.commit import CommitOffsets
from arroyo.processing.strategies.transform import ParallelTransformStep, TransformStep
from arroyo.types import BaseValue, Commit, FilteredPayload, Message, Partition

from snuba.consumers.consumer import BytesInsertBatch, ProcessedMessageBatchWriter
from snuba.processor import ReplacementBatch

ProcessedMessage = Union[None, BytesInsertBatch, ReplacementBatch]


class StreamMessageFilter(Protocol):
    """
    A filter over messages coming from a stream. Can be used to pre filter
    messages during consumption but potentially for other use cases as well.
    """

    @abstractmethod
    def should_drop(self, message: Message[KafkaPayload]) -> bool:
        raise NotImplementedError


class KafkaConsumerStrategyFactory(ProcessingStrategyFactory[KafkaPayload]):
    """
    Do not use for new consumers.
    This is deprecated and will be removed in a future version.

    Builds a four step consumer strategy consisting of dead letter queue,
    filter, transform, and collect phases.

    The `dead_letter_queue_policy_creator` defines the policy for what to do
    when an bad message is encountered throughout the next processing step(s).
    A DLQ wraps the entire strategy, catching InvalidMessage exceptions and
    handling them as the policy dictates.

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
        prefilter: Optional[StreamMessageFilter],
        process_message: Callable[[Message[KafkaPayload]], ProcessedMessage],
        collector: Callable[[], ProcessedMessageBatchWriter],
        max_batch_size: int,
        max_batch_time: float,
        processes: Optional[int],
        input_block_size: Optional[int],
        output_block_size: Optional[int],
        initialize_parallel_transform: Optional[Callable[[], None]] = None,
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
        self.__initialize_parallel_transform = initialize_parallel_transform

    def __should_accept(self, message: Message[KafkaPayload]) -> bool:
        assert self.__prefilter is not None
        return not self.__prefilter.should_drop(message)

    def create_with_partitions(
        self,
        commit: Commit,
        partitions: Mapping[Partition, int],
    ) -> ProcessingStrategy[KafkaPayload]:
        def accumulator(
            batch_writer: ProcessedMessageBatchWriter,
            message: BaseValue[ProcessedMessage],
        ) -> ProcessedMessageBatchWriter:
            batch_writer.submit(Message(message))
            return batch_writer

        def flush_batch(
            message: Message[ProcessedMessageBatchWriter],
        ) -> Message[ProcessedMessageBatchWriter]:
            message.payload.close()
            message.payload.join()
            return message

        collect: Reduce[ProcessedMessage, ProcessedMessageBatchWriter] = Reduce(
            self.__max_batch_size,
            self.__max_batch_time,
            accumulator,
            self.__collector,
            RunTaskInThreads(
                flush_batch,
                # The threadpool has 1 worker since we want to ensure batches are processed
                # sequentially and passed to the next step in order.
                1,
                1,
                CommitOffsets(commit),
            ),
        )

        transform_function = self.__process_message

        strategy: ProcessingStrategy[Union[FilteredPayload, KafkaPayload]]
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
                initializer=self.__initialize_parallel_transform,
            )

        if self.__prefilter is not None:
            strategy = FilterStep(
                self.__should_accept, strategy, commit_policy=ONCE_PER_SECOND
            )

        return strategy
