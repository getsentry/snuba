from __future__ import annotations

import logging
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from typing import Callable, Deque, Mapping, MutableMapping, Optional, Sequence, Tuple

import rapidjson
from arroyo import Message, Partition, Topic
from arroyo.backends.abstract import Producer
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload
from arroyo.processing import StreamProcessor
from arroyo.processing.strategies import MessageRejected, ProcessingStrategy
from arroyo.processing.strategies.abstract import ProcessingStrategyFactory
from arroyo.types import Position

from snuba import state
from snuba.consumers.utils import get_partition_count
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import ENTITY_NAME_LOOKUP, get_entity
from snuba.datasets.factory import get_dataset
from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.reader import Result
from snuba.request import Request
from snuba.state import get_config
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

COMMIT_FREQUENCY_SEC = 1


def build_executor_consumer(
    dataset_name: str,
    entity_names: Sequence[str],
    consumer_group: str,
    producer: Producer[KafkaPayload],
    max_concurrent_queries: int,
    total_concurrent_queries: int,
    auto_offset_reset: str,
    strict_offset_reset: Optional[bool],
    metrics: MetricsBackend,
    stale_threshold_seconds: Optional[int],
    cooperative_rebalancing: bool = False,
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

    consumer_configuration = build_kafka_consumer_configuration(
        scheduled_topic_spec.topic,
        consumer_group,
        auto_offset_reset=auto_offset_reset,
        strict_offset_reset=strict_offset_reset,
    )

    try:
        partition_count = get_partition_count(scheduled_topic_spec.topic)
    except Exception:
        logger.error("partition count unavailable..", exc_info=True)
        partition_count = 0

    # XXX: for now verify that the partition_counts are correct for
    # the executors.
    metrics.gauge(
        "executor.partition_count",
        partition_count,
        tags={"topic": scheduled_topic_spec.topic_name},
    )

    # Collect metrics from librdkafka if we have stats_collection_freq_ms set
    # for the consumer group, or use the default.
    stats_collection_frequency_ms = get_config(
        f"stats_collection_freq_ms_{consumer_group}",
        get_config("stats_collection_freq_ms", 0),
    )

    if stats_collection_frequency_ms and stats_collection_frequency_ms > 0:

        def stats_callback(stats_json: str) -> None:
            stats = rapidjson.loads(stats_json)
            metrics.gauge("librdkafka.total_queue_size", stats.get("replyq", 0))

        consumer_configuration.update(
            {
                "statistics.interval.ms": stats_collection_frequency_ms,
                "stats_cb": stats_callback,
            }
        )

    if cooperative_rebalancing is True:
        consumer_configuration["partition.assignment.strategy"] = "cooperative-sticky"

    return StreamProcessor(
        KafkaConsumer(consumer_configuration),
        Topic(scheduled_topic_spec.topic_name),
        SubscriptionExecutorProcessingFactory(
            max_concurrent_queries,
            total_concurrent_queries,
            dataset,
            entity_names,
            producer,
            metrics,
            stale_threshold_seconds,
            result_topic_spec.topic_name,
        ),
    )


class SubscriptionExecutorProcessingFactory(ProcessingStrategyFactory[KafkaPayload]):
    def __init__(
        self,
        max_concurrent_queries: int,
        total_concurrent_queries: int,
        dataset: Dataset,
        entity_names: Sequence[str],
        producer: Producer[KafkaPayload],
        metrics: MetricsBackend,
        stale_threshold_seconds: Optional[int],
        result_topic: str,
    ) -> None:
        self.__max_concurrent_queries = max_concurrent_queries
        self.__total_concurrent_queries = total_concurrent_queries
        self.__dataset = dataset
        self.__entity_names = entity_names
        self.__producer = producer
        self.__metrics = metrics
        self.__stale_threshold_seconds = stale_threshold_seconds
        self.__result_topic = result_topic

    def create_with_partitions(
        self,
        commit: Callable[[Mapping[Partition, Position]], None],
        partitions: Mapping[Partition, int],
    ) -> ProcessingStrategy[KafkaPayload]:
        return ExecuteQuery(
            self.__dataset,
            self.__entity_names,
            self.__max_concurrent_queries,
            self.__total_concurrent_queries,
            self.__stale_threshold_seconds,
            self.__metrics,
            ProduceResult(self.__producer, self.__result_topic, commit),
        )


class ExecuteQuery(ProcessingStrategy[KafkaPayload]):
    """
    Decodes a scheduled subscription task from the Kafka payload, builds
    the request and executes the ClickHouse query.
    """

    def __init__(
        self,
        dataset: Dataset,
        entity_names: Sequence[str],
        max_concurrent_queries: int,
        total_concurrent_queries: int,
        stale_threshold_seconds: Optional[int],
        metrics: MetricsBackend,
        next_step: ProcessingStrategy[SubscriptionTaskResult],
    ) -> None:
        self.__dataset = dataset
        self.__entity_names = set(entity_names)
        self.__max_concurrent_queries = max_concurrent_queries
        self.__total_concurrent_queries = total_concurrent_queries
        self.__executor = ThreadPoolExecutor(self.__max_concurrent_queries)
        self.__stale_threshold_seconds = stale_threshold_seconds
        self.__metrics = metrics
        self.__next_step = next_step

        self.__last_committed: Optional[float] = None

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
        # Measure the amount of time that took between the task's scheduled
        # time and it beginning to execute.
        self.__metrics.timing(
            "executor.latency", (time.time() - task.timestamp.timestamp()) * 1000
        )

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

        entity = task.task.entity
        entity_name = entity.value

        should_execute = entity_name in self.__entity_names

        # Don't execute stale subscriptions
        if (
            self.__stale_threshold_seconds is not None
            and time.time() - datetime.timestamp(task.timestamp)
            >= self.__stale_threshold_seconds
        ):
            should_execute = False

        if should_execute:
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
                )
            )

        remaining = timeout - (time.time() - start) if timeout is not None else None
        self.__executor.shutdown()

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
        self.__commit_data: MutableMapping[Partition, Position] = {}

        # Time we last called commit
        self.__last_committed: Optional[float] = None

        self.__encoder = SubscriptionTaskResultEncoder()

        self.__queue: Deque[
            Tuple[Message[SubscriptionTaskResult], Future[Message[KafkaPayload]]]
        ] = deque()

        self.__max_buffer_size = 10000
        self.__closed = False

    def __throttled_commit(self, force: bool = False) -> None:
        # Commits all offsets and resets self.__commit_data at most
        # every COMMIT_FREQUENCY_SEC. If force=True is passed, the
        # commit frequency is ignored and we immediately commit.

        now = time.time()

        if (
            self.__last_committed is None
            or now - self.__last_committed >= COMMIT_FREQUENCY_SEC
            or force is True
        ):
            if self.__commit_data:
                self.__commit(self.__commit_data)
                self.__last_committed = now
                self.__commit_data = {}

    def poll(self) -> None:
        while self.__queue:
            message, future = self.__queue[0]

            if not future.done():
                break

            self.__queue.popleft()

            self.__commit_data[message.partition] = Position(
                message.next_offset, message.timestamp
            )
        self.__throttled_commit()

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

        # Commit all pending offsets
        self.__throttled_commit(force=True)

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
