from abc import abstractmethod
from typing import Callable, Mapping, Optional, Protocol, Union

from arroyo.backends.kafka import KafkaPayload
from arroyo.commit import ONCE_PER_SECOND
from arroyo.processing.strategies import (
    FilterStep,
    ProcessingStrategy,
    ProcessingStrategyFactory,
    Reduce,
    RunTask,
    RunTaskInThreads,
    RunTaskWithMultiprocessing,
)
from arroyo.processing.strategies.commit import CommitOffsets
from arroyo.processing.strategies.healthcheck import Healthcheck
from arroyo.processing.strategies.run_task_with_multiprocessing import (
    MultiprocessingPool,
)
from arroyo.types import BaseValue, Commit, FilteredPayload, Message, Partition

from snuba.consumers.consumer import BytesInsertBatch, ProcessedMessageBatchWriter
from snuba.consumers.dlq import ExitAfterNMessages
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
    Builds a four step consumer strategy consisting of dead letter queue,
    filter, transform, and collect phases.

    The `prefilter` supports passing a test function to determine whether a
    message should proceed to the next processing steps or be dropped. If no
    `prefilter` is passed, all messages will proceed through processing.

    The `process_message` function should transform a message containing the
    raw payload into a processed one for the collector. If a value is passed
    for `processes` then this step will switch to the parallel transform
    strategy in order to use multiple processors.

    The `collector` function should return a strategy to be executed on
    batches of messages. Could be used to write messages to disk in batches.

    `max_messages_to_process` is passed only by the DLQ consumer. It is used
    to stop the consumer after a fixed number of messages are processed.
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
        max_insert_batch_size: Optional[int],
        max_insert_batch_time: Optional[float],
        skip_write: bool = False,
        # Passed in the case of DLQ consumer which exits after a certain number of messages
        # is processed
        max_messages_to_process: Optional[int] = None,
        initialize_parallel_transform: Optional[Callable[[], None]] = None,
        health_check_file: Optional[str] = None,
    ) -> None:
        self.__prefilter = prefilter
        self.__process_message = process_message
        self.__collector = collector
        self.__max_messages_to_process = max_messages_to_process

        self.__skip_write = skip_write
        self.__max_batch_size = max_batch_size
        self.__max_batch_time = max_batch_time
        self.__max_insert_batch_size = max_insert_batch_size or max_batch_size
        self.__max_insert_batch_time = max_insert_batch_time or max_batch_time

        if processes is None:
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
        self.__health_check_file = health_check_file
        self.__pool = (
            MultiprocessingPool(self.__processes, self.__initialize_parallel_transform)
            if self.__processes
            else None
        )

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
            return message

        flush_and_commit: ProcessingStrategy[ProcessedMessageBatchWriter]

        if self.__skip_write:
            flush_and_commit = CommitOffsets(commit)
        else:
            flush_and_commit = RunTaskInThreads(
                flush_batch,
                # We process up to 2 insert batches in parallel
                2,
                3,
                CommitOffsets(commit),
            )

        collect = Reduce[ProcessedMessage, ProcessedMessageBatchWriter](
            self.__max_insert_batch_size,
            self.__max_insert_batch_time,
            accumulator,
            self.__collector,
            flush_and_commit,
        )

        transform_function = self.__process_message

        strategy: ProcessingStrategy[Union[FilteredPayload, KafkaPayload]]
        if self.__pool is None:
            strategy = RunTask(transform_function, collect)
        else:
            strategy = RunTaskWithMultiprocessing(
                transform_function,
                collect,
                max_batch_size=self.__max_batch_size,
                max_batch_time=self.__max_batch_time,
                pool=self.__pool,
                input_block_size=self.__input_block_size,
                output_block_size=self.__output_block_size,
            )

        if self.__prefilter is not None:
            strategy = FilterStep(
                self.__should_accept, strategy, commit_policy=ONCE_PER_SECOND
            )

        if self.__max_messages_to_process is not None:
            strategy = ExitAfterNMessages(
                strategy, self.__max_messages_to_process, 10.0
            )

        if self.__health_check_file is not None:
            strategy = Healthcheck(self.__health_check_file, strategy)

        return strategy

    def shutdown(self) -> None:
        if self.__pool:
            self.__pool.close()
