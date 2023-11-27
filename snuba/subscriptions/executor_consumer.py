from __future__ import annotations

import logging
import math
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Deque, Mapping, Optional, Sequence, Tuple

from arroyo import Message, Partition, Topic
from arroyo.backends.abstract import Producer
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload
from arroyo.commit import ONCE_PER_SECOND
from arroyo.processing import StreamProcessor
from arroyo.processing.strategies import MessageRejected, ProcessingStrategy
from arroyo.processing.strategies.abstract import ProcessingStrategyFactory
from arroyo.processing.strategies.commit import CommitOffsets
from arroyo.processing.strategies.healthcheck import Healthcheck
from arroyo.processing.strategies.produce import Produce
from arroyo.types import Commit

from snuba import state
from snuba.clickhouse.errors import ClickhouseError
from snuba.consumers.utils import get_partition_count
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity, get_entity_name
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
from snuba.utils.streams.topics import Topic as SnubaTopic
from snuba.web import QueryException
from snuba.web.constants import NON_RETRYABLE_CLICKHOUSE_ERROR_CODES
from snuba.web.query import parse_and_run_query

logger = logging.getLogger(__name__)


def calculate_max_concurrent_queries(
    assigned_partition_count: int,
    total_partition_count: int,
    total_concurrent_queries: int,
) -> int:
    """
    As consumers are scaled up or down, max_concurrent_queries should
    change accordingly per replica. Fewer replicas means each replica
    can have more max_concurrent_queries and vice versa.

    We use the total_partition_count and assigned_partition_count to estimate
    how many total replicas there are, which in turn lets us calc the
    max_concurrent_queries for the given replica.

    Round up since .5 of a replica doesnt really
    make sense. total_partition_count could be 0 though if something
    went wrong trying to fetch that from the kafka admin. In that case we
    fall back to 1 replica (meaning the max concurrent queries == the total)
    """
    replicas = total_partition_count / assigned_partition_count
    return math.ceil((total_concurrent_queries / replicas))


def build_executor_consumer(
    dataset_name: str,
    entity_names: Sequence[str],
    consumer_group: str,
    bootstrap_servers: Sequence[str],
    slice_id: Optional[int],
    producer: Producer[KafkaPayload],
    total_concurrent_queries: int,
    auto_offset_reset: str,
    strict_offset_reset: Optional[bool],
    metrics: MetricsBackend,
    stale_threshold_seconds: Optional[int],
    health_check_file: Optional[str] = None,
) -> StreamProcessor[KafkaPayload]:
    # Validate that a valid dataset/entity pair was passed in
    dataset = get_dataset(dataset_name)
    dataset_entity_names = [
        get_entity_name(e).value for e in dataset.get_all_entities()
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

    physical_scheduled_topic = scheduled_topic_spec.get_physical_topic_name(slice_id)

    consumer_configuration = build_kafka_consumer_configuration(
        SnubaTopic(physical_scheduled_topic),
        consumer_group,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset=auto_offset_reset,
        strict_offset_reset=strict_offset_reset,
    )

    total_partition_count = get_partition_count(SnubaTopic(physical_scheduled_topic))

    return StreamProcessor(
        KafkaConsumer(consumer_configuration),
        Topic(physical_scheduled_topic),
        SubscriptionExecutorProcessingFactory(
            total_concurrent_queries,
            total_partition_count,
            dataset,
            entity_names,
            producer,
            metrics,
            stale_threshold_seconds,
            result_topic_spec.topic_name,
            health_check_file,
        ),
        commit_policy=ONCE_PER_SECOND,
        join_timeout=5.0,
    )


class SubscriptionExecutorProcessingFactory(ProcessingStrategyFactory[KafkaPayload]):
    def __init__(
        self,
        total_concurrent_queries: int,
        total_partition_count: int,
        dataset: Dataset,
        entity_names: Sequence[str],
        producer: Producer[KafkaPayload],
        metrics: MetricsBackend,
        stale_threshold_seconds: Optional[int],
        result_topic: str,
        health_check_file: Optional[str] = None,
    ) -> None:
        self.__total_concurrent_queries = total_concurrent_queries
        self.__total_partition_count = total_partition_count
        self.__dataset = dataset
        self.__entity_names = entity_names
        self.__producer = producer
        self.__metrics = metrics
        self.__stale_threshold_seconds = stale_threshold_seconds
        self.__result_topic = result_topic
        self.__health_check_file = health_check_file

    def create_with_partitions(
        self,
        commit: Commit,
        partitions: Mapping[Partition, int],
    ) -> ProcessingStrategy[KafkaPayload]:
        calculated_max_concurrent_queries = calculate_max_concurrent_queries(
            len(partitions),
            self.__total_partition_count,
            self.__total_concurrent_queries,
        )
        self.__metrics.gauge(
            "calculated_max_concurrent_queries", calculated_max_concurrent_queries
        )

        strategy: ProcessingStrategy[KafkaPayload] = ExecuteQuery(
            self.__dataset,
            self.__entity_names,
            calculated_max_concurrent_queries,
            self.__stale_threshold_seconds,
            self.__metrics,
            Produce(self.__producer, Topic(self.__result_topic), CommitOffsets(commit)),
        )

        if self.__health_check_file:
            strategy = Healthcheck(self.__health_check_file, strategy)

        return strategy


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
        stale_threshold_seconds: Optional[int],
        metrics: MetricsBackend,
        next_step: ProcessingStrategy[KafkaPayload],
    ) -> None:
        self.__dataset = dataset
        self.__entity_names = set(entity_names)
        self.__max_concurrent_queries = max_concurrent_queries
        self.__executor = ThreadPoolExecutor(self.__max_concurrent_queries)
        self.__stale_threshold_seconds = stale_threshold_seconds
        self.__metrics = metrics
        self.__next_step = next_step

        self.__encoder = SubscriptionScheduledTaskEncoder()
        self.__result_encoder = SubscriptionTaskResultEncoder()

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

            transformed_message = message.replace(
                self.__result_encoder.encode(
                    SubscriptionTaskResult(
                        result_future.task, result_future.future.result()
                    )
                )
            )

            self.__next_step.submit(transformed_message)

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
            try:
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
            except QueryException as exc:
                cause = exc.__cause__
                if isinstance(cause, ClickhouseError):
                    if cause.code in NON_RETRYABLE_CLICKHOUSE_ERROR_CODES:
                        logger.exception("Error running subscription query %r", exc)
                    else:
                        raise exc
        else:
            self.__metrics.increment("skipped_execution", tags={"entity": entity_name})

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True

        self.__executor.shutdown()
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        start = time.time()

        while self.__queue:
            remaining = timeout - (time.time() - start) if timeout is not None else None

            if remaining is not None and remaining <= 0:
                logger.warning(f"Timed out with {len(self.__queue)} futures in queue")
                break

            message, result_future = self.__queue.popleft()

            transformed_message = self.__result_encoder.encode(
                SubscriptionTaskResult(
                    result_future.task, result_future.future.result(remaining)
                )
            )

            self.__next_step.submit(message.replace(transformed_message))

        remaining = timeout - (time.time() - start) if timeout is not None else None
        self.__executor.shutdown()

        self.__next_step.close()
        self.__next_step.join(remaining)
