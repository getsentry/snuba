from __future__ import annotations

import logging
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from functools import lru_cache
from typing import Callable, Deque, Mapping, Optional, Sequence, Tuple, cast
from zlib import crc32

from arroyo import Message, Partition, Topic
from arroyo.backends.abstract import Producer
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload
from arroyo.processing import StreamProcessor
from arroyo.processing.strategies import MessageRejected, ProcessingStrategy
from arroyo.processing.strategies.abstract import ProcessingStrategyFactory
from arroyo.types import Position

from snuba import state
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import ENTITY_NAME_LOOKUP, get_entity
from snuba.datasets.factory import get_dataset
from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.reader import Result
from snuba.request import Request
from snuba.subscriptions.codecs import (
    SubscriptionScheduledTaskEncoder,
    SubscriptionTaskResultEncoder,
)
from snuba.subscriptions.data import (
    ScheduledSubscriptionTask,
    SubscriptionTaskResult,
    SubscriptionTaskResultFuture,
)
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.gauge import Gauge, ThreadSafeGauge
from snuba.utils.metrics.timer import Timer
from snuba.utils.streams.configuration_builder import build_kafka_consumer_configuration
from snuba.web.query import parse_and_run_query

logger = logging.getLogger(__name__)


def build_executor_consumer(
    dataset_name: str,
    entity_names: Sequence[str],
    consumer_group: str,
    producer: Producer[KafkaPayload],
    max_concurrent_queries: int,
    auto_offset_reset: str,
    metrics: MetricsBackend,
    executor: ThreadPoolExecutor,
    # TODO: Should be removed once testing is done
    override_result_topic: str,
) -> StreamProcessor[KafkaPayload]:
    # Validate that a valid dataset/entity pair was passed in
    dataset = get_dataset(dataset_name)
    dataset_entity_names = [
        ENTITY_NAME_LOOKUP[e].value for e in dataset.get_all_entities()
    ]

    # Only entities in the same dataset with the same scheduled and result topics
    # may be run together

    def get_topics_for_entity(
        entity_name: str,
    ) -> Tuple[KafkaTopicSpec, KafkaTopicSpec]:
        assert (
            entity_name in dataset_entity_names
        ), f"Entity {entity_name} does not exist in dataset {dataset_name}"

        entity = get_entity(EntityKey(entity_name))
        storage = entity.get_writable_storage()

        assert (
            storage is not None
        ), f"Entity {entity_name} does not have a writable storage by default."

        stream_loader = storage.get_table_writer().get_stream_loader()

        scheduled_topic_spec = stream_loader.get_subscription_scheduled_topic_spec()
        assert scheduled_topic_spec is not None

        result_topic_spec = stream_loader.get_subscription_result_topic_spec()
        assert result_topic_spec is not None

        return scheduled_topic_spec, result_topic_spec

    scheduled_topic_spec, result_topic_spec = get_topics_for_entity(entity_names[0])

    for entity_name in entity_names[1:]:
        assert get_topics_for_entity(entity_name) == (
            scheduled_topic_spec,
            result_topic_spec,
        ), "All entities must have same scheduled and result topics"

    return StreamProcessor(
        KafkaConsumer(
            build_kafka_consumer_configuration(
                scheduled_topic_spec.topic,
                consumer_group,
                auto_offset_reset=auto_offset_reset,
            ),
        ),
        Topic(scheduled_topic_spec.topic_name),
        SubscriptionExecutorProcessingFactory(
            executor,
            max_concurrent_queries,
            dataset,
            entity_names,
            producer,
            metrics,
            override_result_topic,
        ),
    )


class SubscriptionExecutorProcessingFactory(ProcessingStrategyFactory[KafkaPayload]):
    def __init__(
        self,
        executor: ThreadPoolExecutor,
        max_concurrent_queries: int,
        dataset: Dataset,
        entity_names: Sequence[str],
        producer: Producer[KafkaPayload],
        metrics: MetricsBackend,
        result_topic: str,
    ) -> None:
        self.__executor = executor
        self.__max_concurrent_queries = max_concurrent_queries
        self.__dataset = dataset
        self.__entity_names = entity_names
        self.__producer = producer
        self.__metrics = metrics
        self.__result_topic = result_topic

    def create(
        self, commit: Callable[[Mapping[Partition, Position]], None]
    ) -> ProcessingStrategy[KafkaPayload]:
        return ExecuteQuery(
            self.__dataset,
            self.__entity_names,
            self.__executor,
            self.__max_concurrent_queries,
            self.__metrics,
            ProduceResult(self.__producer, self.__result_topic, commit),
            commit,
        )


@lru_cache(maxsize=50000)
def subscription_id_to_float(subscription_id: str) -> float:
    # Converts a subscription ID string to a float between 0 and 1
    # Used for sampling query execution by subscription ID during rollout
    return (crc32(subscription_id.encode("utf-8")) & 0xFFFFFFFF) / 2 ** 32


class ExecuteQuery(ProcessingStrategy[KafkaPayload]):
    """
    Decodes a scheduled subscription task from the Kafka payload, builds
    the request and executes the ClickHouse query.
    """

    def __init__(
        self,
        dataset: Dataset,
        entity_names: Sequence[str],
        executor: ThreadPoolExecutor,
        max_concurrent_queries: int,
        metrics: MetricsBackend,
        next_step: ProcessingStrategy[SubscriptionTaskResult],
        commit: Optional[Callable[[Mapping[Partition, Position]], None]] = None,
    ) -> None:
        self.__dataset = dataset
        self.__entity_names = set(entity_names)
        self.__executor = executor
        self.__max_concurrent_queries = max_concurrent_queries
        self.__metrics = metrics
        self.__next_step = next_step
        self.__commit = commit

        self.__encoder = SubscriptionScheduledTaskEncoder()

        self.__queue: Deque[
            Tuple[Message[KafkaPayload], SubscriptionTaskResultFuture]
        ] = deque()

        self.__closed = False

        self.__concurrent_gauge: Gauge = ThreadSafeGauge(
            self.__metrics, "executor.concurrent"
        )

        self.__concurrent_clickhouse_gauge: Gauge = ThreadSafeGauge(
            self.__metrics, "executor.concurrent.clickhouse"
        )

    def __execute_query(
        self, task: ScheduledSubscriptionTask, tick_upper_offset: int
    ) -> Tuple[Request, Result]:
        timer = Timer("query")

        with self.__concurrent_gauge:
            request = task.task.subscription.data.build_request(
                self.__dataset,
                task.timestamp,
                tick_upper_offset,
                timer,
                self.__metrics,
                "subscriptions_executor",
            )

            result = parse_and_run_query(
                self.__dataset,
                request,
                timer,
                robust=True,
                concurrent_queries_gauge=self.__concurrent_clickhouse_gauge,
            ).result

            return (request, result)

    def poll(self) -> None:
        while self.__queue:
            if not self.__queue[0][1].future.done():
                break

            message, result_future = self.__queue.popleft()

            self.__next_step.submit(
                Message(
                    message.partition,
                    message.offset,
                    SubscriptionTaskResult(
                        result_future.task, result_future.future.result()
                    ),
                    message.timestamp,
                    message.next_offset,
                )
            )

        self.__next_step.poll()

    def submit(self, message: Message[KafkaPayload]) -> None:
        assert not self.__closed

        # If there are max_concurrent_queries + 10 pending futures in the queue,
        # we will start raising MessageRejected to slow down the consumer as
        # it means our executor cannot keep up
        queue_size_factor = state.get_config("executor_queue_size_factor", 10)
        assert (
            queue_size_factor is not None
        ), "Invalid executor_queue_size_factor config"
        max_queue_size = self.__max_concurrent_queries * queue_size_factor

        # Tell the consumer to pause until we have removed some futures from
        # the queue
        if len(self.__queue) >= max_queue_size:
            raise MessageRejected

        task = self.__encoder.decode(message.payload)

        tick_upper_offset = task.task.tick_upper_offset

        # We need to sample queries to ClickHouse while this is being rolled out
        # as we don't want to duplicate every subscription query
        executor_sample_rate = cast(
            float, state.get_config("executor_sample_rate", 0.0)
        )

        # HACK: Just commit offsets and return if we haven't started rollout
        # yet. This is just a temporary workaround to prevent the lag continuously
        # growing and dwarfing others on op's Kafka dashboard
        if self.__commit is not None and executor_sample_rate == 0:
            self.__commit(
                {message.partition: Position(message.offset, message.timestamp)}
            )

            return

        subscription_id = str(task.task.subscription.identifier)
        should_execute = (
            subscription_id_to_float(subscription_id) < executor_sample_rate
        )

        entity_name = task.task.entity.value

        if should_execute and entity_name in self.__entity_names:
            self.__queue.append(
                (
                    message,
                    SubscriptionTaskResultFuture(
                        task,
                        self.__executor.submit(
                            self.__execute_query, task, tick_upper_offset
                        ),
                    ),
                )
            )
        else:
            self.__metrics.increment("skipped_execution", tags={"entity": entity_name})

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True

        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        start = time.time()

        while self.__queue:
            remaining = timeout - (time.time() - start) if timeout is not None else None

            if remaining is not None and remaining <= 0:
                logger.warning(f"Timed out with {len(self.__queue)} futures in queue")
                break

            message, result_future = self.__queue.popleft()

            subscription_task_result = SubscriptionTaskResult(
                result_future.task, result_future.future.result(remaining)
            )

            self.__next_step.submit(
                Message(
                    message.partition,
                    message.offset,
                    subscription_task_result,
                    message.timestamp,
                    message.next_offset,
                )
            )

        remaining = timeout - (time.time() - start) if timeout is not None else None

        self.__next_step.close()
        self.__next_step.join(remaining)


class ProduceResult(ProcessingStrategy[SubscriptionTaskResult]):
    """
    Gets a SubscriptionTaskResult from the ExecuteQuery processor, encodes and produces
    the results to the subscription result topic then commits offsets.
    """

    def __init__(
        self,
        producer: Producer[KafkaPayload],
        result_topic: str,
        commit: Callable[[Mapping[Partition, Position]], None],
    ):
        self.__producer = producer
        self.__result_topic = Topic(result_topic)
        self.__commit = commit

        self.__encoder = SubscriptionTaskResultEncoder()

        self.__queue: Deque[
            Tuple[Message[SubscriptionTaskResult], Future[Message[KafkaPayload]]]
        ] = deque()

        self.__max_buffer_size = 10000
        self.__closed = False

    def poll(self) -> None:
        while self.__queue:
            message, future = self.__queue[0]

            if not future.done():
                break

            self.__queue.popleft()
            self.__commit(
                {message.partition: Position(message.offset, message.timestamp)}
            )

    def submit(self, message: Message[SubscriptionTaskResult]) -> None:
        assert not self.__closed

        if len(self.__queue) >= self.__max_buffer_size:
            raise MessageRejected

        encoded = self.__encoder.encode(message.payload)
        self.__queue.append(
            (message, self.__producer.produce(self.__result_topic, encoded))
        )

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True

    def join(self, timeout: Optional[float] = None) -> None:
        start = time.time()
        while self.__queue:
            remaining = timeout - (time.time() - start) if timeout is not None else None
            if remaining is not None and remaining <= 0:
                logger.warning(f"Timed out with {len(self.__queue)} futures in queue")
                break

            message, future = self.__queue.popleft()

            future.result(remaining)

            self.__commit(
                {message.partition: Position(message.offset, message.timestamp)}
            )
