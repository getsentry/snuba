from __future__ import annotations

import logging
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable, Deque, Mapping, NamedTuple, Optional, Tuple

from arroyo import Message, Partition, Topic
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload
from arroyo.processing import StreamProcessor
from arroyo.processing.strategies import MessageRejected, ProcessingStrategy
from arroyo.processing.strategies.abstract import ProcessingStrategyFactory
from arroyo.types import Position

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import ENTITY_NAME_LOOKUP, get_entity
from snuba.datasets.factory import get_dataset
from snuba.reader import Result
from snuba.request import Request
from snuba.subscriptions.codecs import SubscriptionScheduledTaskEncoder
from snuba.subscriptions.data import ScheduledSubscriptionTask
from snuba.subscriptions.utils import Tick
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.gauge import Gauge, ThreadSafeGauge
from snuba.utils.metrics.timer import Timer
from snuba.utils.streams.configuration_builder import build_kafka_consumer_configuration
from snuba.web.query import parse_and_run_query

logger = logging.getLogger(__name__)


def build_executor_consumer(
    dataset_name: str,
    entity_name: str,
    consumer_group: str,
    max_concurrent_queries: int,
    auto_offset_reset: str,
    metrics: MetricsBackend,
) -> StreamProcessor[KafkaPayload]:
    # Validate that a valid dataset/entity pair was passed in
    dataset = get_dataset(dataset_name)
    dataset_entity_names = [
        ENTITY_NAME_LOOKUP[e].value for e in dataset.get_all_entities()
    ]
    assert (
        entity_name in dataset_entity_names
    ), f"Entity {entity_name} does not exist in dataset {dataset_name}"

    entity_key = EntityKey(entity_name)
    entity = get_entity(entity_key)
    storage = entity.get_writable_storage()

    assert (
        storage is not None
    ), f"Entity {entity_name} does not have a writable storage by default."

    stream_loader = storage.get_table_writer().get_stream_loader()
    scheduled_topic_spec = stream_loader.get_subscription_scheduled_topic_spec()
    assert scheduled_topic_spec is not None

    return StreamProcessor(
        KafkaConsumer(
            build_kafka_consumer_configuration(
                scheduled_topic_spec.topic,
                consumer_group,
                auto_offset_reset=auto_offset_reset,
            ),
        ),
        Topic(scheduled_topic_spec.topic_name),
        SubscriptionExecutorProcessingFactory(),
    )


class Noop(ProcessingStrategy[KafkaPayload]):
    """
    Placeholder.
    """

    def __init__(self, commit: Callable[[Mapping[Partition, Position]], None]):
        self.__commit = commit

    def poll(self) -> None:
        pass

    def submit(self, message: Message[KafkaPayload]) -> None:
        self.__commit({message.partition: Position(message.offset, message.timestamp)})

    def close(self) -> None:
        pass

    def terminate(self) -> None:
        pass

    def join(self, timeout: Optional[float] = None) -> None:
        pass


class SubscriptionExecutorProcessingFactory(ProcessingStrategyFactory[KafkaPayload]):
    def create(
        self, commit: Callable[[Mapping[Partition, Position]], None]
    ) -> ProcessingStrategy[KafkaPayload]:
        return Noop(commit)


class SubscriptionResultFuture(NamedTuple):
    message: Message[KafkaPayload]
    future: Future[Tuple[Request, Result]]


class ExecuteQuery(ProcessingStrategy[KafkaPayload]):
    """
    Decodes a scheduled subscription task from the Kafka payload, builds
    the request and executes the ClickHouse query.
    """

    def __init__(
        self,
        dataset: Dataset,
        executor: ThreadPoolExecutor,
        max_concurrent_queries: int,
        metrics: MetricsBackend,
        next_step: ProcessingStrategy[KafkaPayload],
    ) -> None:
        self.__dataset = dataset
        self.__executor = executor
        self.__max_concurrent_queries = max_concurrent_queries
        self.__metrics = metrics
        self.__next_step = next_step

        self.__encoder = SubscriptionScheduledTaskEncoder()

        self.__queue: Deque[SubscriptionResultFuture] = deque()

        self.__closed = False

        self.__concurrent_gauge: Gauge = ThreadSafeGauge(
            self.__metrics, "executor.concurrent"
        )

        self.__concurrent_clickhouse_gauge: Gauge = ThreadSafeGauge(
            self.__metrics, "executor.concurrent.clickhouse"
        )

    def __execute_query(
        self, task: ScheduledSubscriptionTask, tick: Tick
    ) -> Tuple[Request, Result]:
        timer = Timer("query")

        with self.__concurrent_gauge:
            request = task.task.subscription.data.build_request(
                self.__dataset,
                task.timestamp,
                tick.offsets.upper,
                timer,
                self.__metrics,
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
            if not self.__queue[0].future.done():
                break

            result_future = self.__queue.popleft()

            message = result_future.message

            self.__next_step.submit(message)

        self.__next_step.poll()

    def submit(self, message: Message[KafkaPayload]) -> None:
        assert not self.__closed

        # Tell the consumer to pause until we have removed some futures from
        # the queue
        if len(self.__queue) >= self.__max_concurrent_queries:
            raise MessageRejected

        task = self.__encoder.decode(message.payload)

        tick = task.task.tick

        self.__queue.append(
            SubscriptionResultFuture(
                message, self.__executor.submit(self.__execute_query, task, tick),
            )
        )

    def close(self) -> None:
        self.__closed = True
        self.__next_step.close()

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

            result_future = self.__queue.popleft()

            result_future.future.result(remaining)

            message = result_future.message

            self.__next_step.submit(message)

        remaining = timeout - (time.time() - start) if timeout is not None else None
        self.__next_step.close()
        self.__next_step.join(remaining)
