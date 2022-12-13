import time
from abc import abstractmethod
from typing import (
    Any,
    Callable,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
    Protocol,
    TypeVar,
)

from arroyo.backends.kafka import KafkaPayload
from arroyo.backends.kafka.commit import CommitCodec
from arroyo.commit import Commit as CommitLogCommit
from arroyo.processing.strategies import ProcessingStrategy, ProcessingStrategyFactory
from arroyo.processing.strategies.collect import CollectStep, ParallelCollectStep
from arroyo.processing.strategies.commit import CommitOffsets
from arroyo.processing.strategies.dead_letter_queue.dead_letter_queue import (
    DeadLetterQueue,
)
from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
)
from arroyo.processing.strategies.filter import FilterStep
from arroyo.processing.strategies.transform import ParallelTransformStep, TransformStep
from arroyo.types import Commit, Message, Partition, Position, Topic
from confluent_kafka import KafkaError
from confluent_kafka import Message as ConfluentMessage
from confluent_kafka import Producer as ConfluentProducer

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


PRODUCE_FREQUENCY_SEC = 1.0


class ProduceCommitLog(ProcessingStrategy[Any]):
    def __init__(
        self,
        producer: ConfluentProducer,
        commit_log_topic: Topic,
        group_id: str,
        commit: Commit,
    ) -> None:
        self.__producer = producer
        self.__commit_log_topic = commit_log_topic
        self.__group_id = group_id
        self.__commit_codec = CommitCodec()
        self.__commit = commit

        # Record offsets to be produced to the commit log
        self.__offsets_to_produce: MutableMapping[Partition, Position] = {}
        self.__last_flush_time = time.time()

    def poll(self) -> None:
        self.__flush()
        self.__producer.poll(0.0)

    def __commit_message_delivery_callback(
        self, error: Optional[KafkaError], message: ConfluentMessage
    ) -> None:
        if error is not None:
            raise Exception(error.str())

    def __flush(self, force: bool = False) -> None:
        if not force and time.time() - self.__last_flush_time < PRODUCE_FREQUENCY_SEC:
            return

        self.__commit(self.__offsets_to_produce)

        for partition, position in self.__offsets_to_produce.items():
            commit = CommitLogCommit(
                self.__group_id, partition, position.offset, position.timestamp
            )

            payload = self.__commit_codec.encode(commit)

            self.__producer.produce(
                self.__commit_log_topic.name,
                key=payload.key,
                value=payload.value,
                headers=payload.headers,
                on_delivery=self.__commit_message_delivery_callback,
            )

        self.__offsets_to_produce.clear()

    def submit(self, message: Message[Any]) -> None:
        self.__offsets_to_produce.update(message.committable)

    def close(self) -> None:
        pass

    def terminate(self) -> None:
        pass

    def join(self, timeout: Optional[float] = None) -> None:
        self.__flush(force=True)

        messages: int = self.__producer.flush(*[timeout] if timeout is not None else [])
        if messages > 0:
            raise TimeoutError(f"{messages} commit log messages pending delivery")


class CommitLogConfig(NamedTuple):
    producer: ConfluentProducer
    topic: Topic
    group_id: str


class ConsumerStrategyFactory(ProcessingStrategyFactory[TPayload]):
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
        prefilter: Optional[StreamMessageFilter[TPayload]],
        process_message: Callable[[Message[TPayload]], TProcessed],
        collector: Callable[[], ProcessingStrategy[TProcessed]],
        max_batch_size: int,
        max_batch_time: float,
        processes: Optional[int],
        input_block_size: Optional[int],
        output_block_size: Optional[int],
        initialize_parallel_transform: Optional[Callable[[], None]] = None,
        dead_letter_queue_policy_creator: Optional[
            Callable[[], DeadLetterQueuePolicy]
        ] = None,
        parallel_collect: bool = False,
        parallel_collect_timeout: float = 10.0,
        commit_log_config: Optional[CommitLogConfig] = None,
    ) -> None:
        self.__prefilter = prefilter
        self.__dead_letter_queue_policy_creator = dead_letter_queue_policy_creator
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
        self.__parallel_collect = parallel_collect
        self.__parallel_collect_timeout = parallel_collect_timeout
        self.__commit_log_config = commit_log_config

    def __should_accept(self, message: Message[TPayload]) -> bool:
        assert self.__prefilter is not None
        return not self.__prefilter.should_drop(message)

    def create_with_partitions(
        self,
        commit: Commit,
        partitions: Mapping[Partition, int],
    ) -> ProcessingStrategy[TPayload]:
        commit_strategy: ProcessingStrategy[Any]
        if self.__commit_log_config is not None:
            commit_strategy = ProduceCommitLog(
                self.__commit_log_config.producer,
                self.__commit_log_config.topic,
                self.__commit_log_config.group_id,
                commit,
            )
        else:
            commit_strategy = CommitOffsets(commit)

        collect = (
            ParallelCollectStep(
                self.__collector,
                commit_strategy,
                self.__max_batch_size,
                self.__max_batch_time,
                self.__parallel_collect_timeout,
            )
            if self.__parallel_collect
            else CollectStep(
                self.__collector,
                commit_strategy,
                self.__max_batch_size,
                self.__max_batch_time,
            )
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
                initializer=self.__initialize_parallel_transform,
            )

        if self.__prefilter is not None:
            strategy = FilterStep(self.__should_accept, strategy)

        if self.__dead_letter_queue_policy_creator is not None:
            # The DLQ Policy is instantiated here so it gets the correct
            # metrics singleton in the init function of the policy
            # It also ensures any producer/consumer is recreated on rebalance
            strategy = DeadLetterQueue(
                strategy, self.__dead_letter_queue_policy_creator()
            )

        return strategy


class KafkaConsumerStrategyFactory(ConsumerStrategyFactory[KafkaPayload]):
    """
    Do not use for new consumers.
    This is deprecated and will be removed in a future version.
    """

    pass
