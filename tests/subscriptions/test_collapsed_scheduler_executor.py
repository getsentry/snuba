import uuid
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Optional, Sequence
from unittest import mock

import pytest
from arroyo import Message, Partition, Topic
from arroyo.backends.kafka import KafkaPayload
from arroyo.backends.local.backend import LocalBroker as Broker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.types import Position
from arroyo.utils.clock import TestingClock

from snuba import state
from snuba.datasets.entities import EntityKey
from snuba.datasets.factory import get_dataset
from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.redis import redis_client
from snuba.subscriptions.codecs import SubscriptionScheduledTaskEncoder
from snuba.subscriptions.data import PartitionId, SubscriptionData
from snuba.subscriptions.entity_subscription import EventsSubscription
from snuba.subscriptions.scheduler import SubscriptionScheduler
from snuba.subscriptions.scheduler_processing_strategy import (
    CommittableTick,
    ProduceScheduledSubscriptionMessage,
    ProvideCommitStrategy,
    ScheduledSubscriptionQueue,
    TickBuffer,
    TickSubscription,
)
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.subscriptions.utils import SchedulingWatermarkMode, Tick
from snuba.utils.streams.topics import Topic as SnubaTopic
from snuba.utils.types import Interval
from tests.backends.metrics import TestingMetricsBackend
from arroyo.processing.strategies import MessageRejected, ProcessingStrategy
from snuba.utils.metrics import MetricsBackend
from snuba.subscriptions.executor_consumer import ExecuteQuery
from typing import Mapping


class JustRunTheQueryStrategy(ProcessingStrategy[Tick]):
    def __init__(
        self,
        partitions: int,
        schedulers: Mapping[int, SubscriptionScheduler],
        metrics: MetricsBackend,
        next_step: ExecuteQuery,
    ) -> None:
        self.__partitions = partitions
        self.__next_step = next_step
        self.__metrics = metrics

        self.__schedulers = schedulers

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
        tasks = [task for task in self.__schedulers[tick.partition].find(tick)]
        assert tasks
        print("TASKS: ", len(tasks))
        # build the KafkaPayload
        # self.__next_step.submit(message=payload)

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True

    def join(self, timeout: Optional[float] = None) -> None:
        pass


def do_setup() -> None:
    store = RedisSubscriptionDataStore(redis_client, EntityKey.EVENTS, PartitionId(0))
    store.create(
        uuid.uuid4(),
        SubscriptionData(
            project_id=1,
            time_window_sec=60,
            resolution_sec=60,
            query="MATCH events SELECT count()",
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

    schedulers = {
        index: SubscriptionScheduler(
            EntityKey.EVENTS,
            RedisSubscriptionDataStore(
                redis_client, EntityKey.EVENTS, PartitionId(index)
            ),
            partition_id=PartitionId(index),
            cache_ttl=timedelta(seconds=300),
            metrics=metrics,
        )
        for index in range(num_partitions)
    }

    commit = mock.Mock()
    produce_step = mock.Mock()

    execute_strategy = ExecuteQuery(
        dataset,
        entity_names,
        executor,
        max_concurrent_queries,
        None,
        metrics,
        produce_step,
        commit,
    )

    topic = Topic("snuba-commit-log")
    partition = Partition(topic, 0)
    next_step = JustRunTheQueryStrategy(
        partitions=2,
        metrics=TestingMetricsBackend(),
        next_step=execute_strategy,
        schedulers=schedulers,
    )
    tick_buffer_strategy = TickBuffer(
        SchedulingWatermarkMode.PARTITION, num_partitions, None, next_step
    )

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

    tick_buffer_strategy.submit(message)
