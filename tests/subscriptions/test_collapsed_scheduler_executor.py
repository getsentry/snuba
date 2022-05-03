import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Callable, Mapping, Optional, Sequence, cast
from unittest import mock

from arroyo import Message, Partition, Topic
from arroyo.backends.abstract import Producer
from arroyo.backends.kafka import KafkaPayload, KafkaProducer
from arroyo.processing.strategies import ProcessingStrategy
from arroyo.types import Position

from snuba import state
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.factory import get_dataset
from snuba.redis import redis_client
from snuba.subscriptions.codecs import SubscriptionScheduledTaskEncoder
from snuba.subscriptions.data import PartitionId, SubscriptionData
from snuba.subscriptions.entity_subscription import EventsSubscription
from snuba.subscriptions.executor_consumer import SubscriptionExecutorProcessingFactory
from snuba.subscriptions.scheduler import SubscriptionScheduler
from snuba.subscriptions.scheduler_processing_strategy import TickBuffer
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.subscriptions.utils import SchedulingWatermarkMode, Tick
from snuba.utils.metrics import MetricsBackend
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic as SnubaTopic
from snuba.utils.types import Interval
from tests.backends.metrics import TestingMetricsBackend


class ForwardToExecutor(ProcessingStrategy[Tick]):
    def __init__(
        self,
        partitions: int,
        schedulers: Sequence[Mapping[int, SubscriptionScheduler]],
        metrics: MetricsBackend,
        next_step: ProcessingStrategy[KafkaPayload],
    ) -> None:
        self.__partitions = partitions
        self.__next_step = next_step
        self.__metrics = metrics

        self.__schedulers = schedulers
        self.__encoder = SubscriptionScheduledTaskEncoder()
        # Store the last message we received for each partition so know when
        # to commit offsets.
        # self.__latest_messages_by_partition: MutableMapping[
        #     int, Optional[Message[Tick]]
        # ] = {index: None for index in range(self.__partitions)}
        # self.__offset_low_watermark: Optional[int] = None
        # self.__offset_high_watermark: Optional[int] = None

        self.__closed = False

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[Tick]) -> None:

        # NOTE: do we want to just run the query right after the TickBuffer
        # if so, what assumptions are we making?
        # There will be never be more than one of these consumers
        # one per entity
        # NOTE: how does the committing propogate back to the kafka topic?

        # let's get the query to run
        tick = message.payload
        assert tick.partition is not None

        tasks = []
        for entity_scheduler in self.__schedulers:
            tasks.extend([task for task in entity_scheduler[tick.partition].find(tick)])

        encoded_tasks = [self.__encoder.encode(task) for task in tasks]

        for task in encoded_tasks:
            self.__next_step.submit(
                cast(Message[KafkaPayload], replace(message, payload=task))
            )

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True

    def join(self, timeout: Optional[float] = None) -> None:
        pass


from arroyo.processing.strategies.abstract import ProcessingStrategyFactory


class CollapsedSchedulerExecutorFactory(ProcessingStrategyFactory[Tick]):
    def __init__(
        self,
        executor: ThreadPoolExecutor,
        partitions: int,
        max_concurrent_queries: int,
        dataset: Dataset,
        entity_names: Sequence[str],
        producer: Producer[KafkaPayload],
        metrics: MetricsBackend,
        stale_threshold_seconds: Optional[int],
        result_topic: str,
        schedule_ttl: int,
    ) -> None:
        self.__executor = executor
        self.__partitions = partitions
        self.__max_concurrent_queries = max_concurrent_queries
        self.__dataset = dataset
        self.__entity_names = entity_names
        self.__producer = producer
        self.__metrics = metrics
        self.__stale_threshold_seconds = stale_threshold_seconds
        self.__result_topic = result_topic

        entity_keys = [EntityKey(entity_name) for entity_name in self.__entity_names]

        self.__schedulers = [
            {
                index: SubscriptionScheduler(
                    entity_key,
                    RedisSubscriptionDataStore(
                        redis_client, entity_key, PartitionId(index)
                    ),
                    partition_id=PartitionId(index),
                    cache_ttl=timedelta(seconds=schedule_ttl),
                    metrics=self.__metrics,
                )
                for index in range(self.__partitions)
            }
            for entity_key in entity_keys
        ]

    def create(
        self, commit: Callable[[Mapping[Partition, Position]], None]
    ) -> ProcessingStrategy[Tick]:
        execute_step = SubscriptionExecutorProcessingFactory(
            self.__executor,
            self.__max_concurrent_queries,
            self.__dataset,
            self.__entity_names,
            self.__producer,
            self.__metrics,
            self.__stale_threshold_seconds,
            self.__result_topic,
        ).create(commit)
        return TickBuffer(
            SchedulingWatermarkMode.PARTITION,
            self.__partitions,
            None,
            ForwardToExecutor(
                self.__partitions, self.__schedulers, self.__metrics, execute_step
            ),
        )


def do_setup() -> None:
    store = RedisSubscriptionDataStore(redis_client, EntityKey.EVENTS, PartitionId(0))
    store.create(
        uuid.uuid4(),
        SubscriptionData(
            project_id=1,
            time_window_sec=60,
            resolution_sec=60,
            query="MATCH (events) SELECT count()",
            entity_subscription=EventsSubscription(data_dict={}),
        ),
    )


def test_collapsed_scheduler() -> None:
    do_setup()
    state.set_config("subscription_mode_events", "new")
    epoch = datetime(1970, 1, 1)

    dataset = get_dataset("events")
    entity_names = ["events"]
    num_partitions = 2
    max_concurrent_queries = 2
    executor = ThreadPoolExecutor(max_concurrent_queries)
    metrics = TestingMetricsBackend()

    commit = mock.Mock()

    topic = Topic("snuba-commit-log")
    partition = Partition(topic, 0)
    stale_threshold_seconds = None
    result_topic = "idk"
    schedule_ttl = 60

    producer = KafkaProducer(
        build_kafka_producer_configuration(SnubaTopic.SUBSCRIPTION_RESULTS_EVENTS)
    )

    with closing(producer):
        factory = CollapsedSchedulerExecutorFactory(
            executor,
            num_partitions,
            max_concurrent_queries,
            dataset,
            entity_names,
            producer,
            metrics,
            stale_threshold_seconds,
            result_topic,
            schedule_ttl,
        )

        strategy = factory.create(commit)

        message = Message(
            partition,
            4,
            Tick(
                0,
                offsets=Interval(1, 3),
                timestamps=Interval(epoch, epoch + timedelta(seconds=60)),
            ),
            epoch,
            5,
        )
        # TODO: make polling the tick buffer work
        strategy.submit(message)
        import time

        time.sleep(1.0)
        strategy.poll()
        assert commit.call_count == 1
