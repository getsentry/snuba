import uuid
from collections import deque
from concurrent.futures import Future
from datetime import datetime, timedelta
from typing import Optional, Sequence
from unittest import mock

import pytest
from arroyo import Message, Partition, Topic
from arroyo.backends.kafka import KafkaPayload
from arroyo.backends.local.backend import LocalBroker as Broker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.types import BrokerValue
from arroyo.utils.clock import TestingClock

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.redis import RedisClientKey, get_redis_client
from snuba.subscriptions.codecs import SubscriptionScheduledTaskEncoder
from snuba.subscriptions.data import PartitionId, SubscriptionData
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
from snuba.subscriptions.types import Interval
from snuba.subscriptions.utils import SchedulingWatermarkMode, Tick
from snuba.utils.streams.topics import Topic as SnubaTopic
from tests.backends.metrics import TestingMetricsBackend


def test_tick_buffer_immediate() -> None:
    epoch = datetime(1970, 1, 1)
    next_step = mock.Mock()
    metrics = TestingMetricsBackend()

    strategy = TickBuffer(
        SchedulingWatermarkMode.PARTITION, 2, None, next_step, metrics
    )

    topic = Topic("messages")
    partition = Partition(topic, 0)

    message = Message(
        BrokerValue(
            Tick(
                0,
                offsets=Interval(1, 3),
                timestamps=Interval(epoch, epoch + timedelta(seconds=5)),
            ),
            partition,
            4,
            epoch,
        )
    )

    strategy.submit(message)

    assert next_step.submit.call_count == 1
    assert next_step.submit.call_args == mock.call(message)


def test_tick_buffer_wait_slowest() -> None:
    epoch = datetime(1970, 1, 1)
    now = datetime.now()
    next_step = mock.Mock()
    metrics = TestingMetricsBackend()

    # Create strategy with 2 partitions
    strategy = TickBuffer(SchedulingWatermarkMode.GLOBAL, 2, 10, next_step, metrics)

    topic = Topic("messages")
    commit_log_partition = Partition(topic, 0)

    # First message in partition 0, do not submit to next step
    message_0_0 = Message(
        BrokerValue(
            Tick(
                0,
                offsets=Interval(1, 3),
                timestamps=Interval(epoch, epoch + timedelta(seconds=5)),
            ),
            commit_log_partition,
            4,
            now,
        )
    )
    strategy.submit(message_0_0)

    assert next_step.submit.call_count == 0

    # Another message in partition 0, do not submit to next step
    message_0_1 = Message(
        BrokerValue(
            Tick(
                0,
                offsets=Interval(3, 4),
                timestamps=Interval(
                    epoch + timedelta(seconds=5), epoch + timedelta(seconds=10)
                ),
            ),
            commit_log_partition,
            5,
            now,
        )
    )
    strategy.submit(message_0_1)

    assert next_step.submit.call_count == 0

    # Message in partition 1 has the lowest timestamp so we submit to the next step
    message_1_0 = Message(
        BrokerValue(
            Tick(
                1,
                offsets=Interval(100, 120),
                timestamps=Interval(epoch, epoch + timedelta(seconds=4)),
            ),
            commit_log_partition,
            6,
            now,
        )
    )
    strategy.submit(message_1_0)

    assert next_step.submit.call_count == 1
    assert next_step.submit.call_args_list == [mock.call(message_1_0)]

    next_step.reset_mock()
    # Message in partition 1 has the same timestamp as the earliest message
    # in partition 0. Both should be submitted to the next step.
    message_1_1 = Message(
        BrokerValue(
            Tick(
                1,
                offsets=Interval(120, 130),
                timestamps=Interval(
                    epoch + timedelta(seconds=4), epoch + timedelta(seconds=5)
                ),
            ),
            commit_log_partition,
            7,
            now,
        )
    )
    strategy.submit(message_1_1)

    assert next_step.submit.call_count == 2
    assert next_step.submit.call_args_list == [
        mock.call(message_0_0),
        mock.call(message_1_1),
    ]

    next_step.reset_mock()

    # Submit another message to partition 1 with the same timestamp as
    # in partition 0. Two more messages should be submitted and the
    # the partition lag should be 0 now.
    message_1_2 = Message(
        BrokerValue(
            Tick(
                1,
                offsets=Interval(130, 140),
                timestamps=Interval(
                    epoch + timedelta(seconds=5), epoch + timedelta(seconds=10)
                ),
            ),
            commit_log_partition,
            7,
            now,
        )
    )
    strategy.submit(message_1_2)

    assert next_step.submit.call_count == 2
    assert next_step.submit.call_args_list == [
        mock.call(message_0_1),
        mock.call(message_1_2),
    ]

    next_step.reset_mock()

    # Submit 11 more messages to partition 0. Since we hit
    # `max_ticks_buffered_per_partition`, the first message (but
    # none of the others) should be submitted to the next step.
    messages = []
    for i in range(11):
        message = Message(
            BrokerValue(
                Tick(
                    1,
                    offsets=Interval(4 + i, 5 + i),
                    timestamps=Interval(
                        epoch + timedelta(seconds=10 + i),
                        epoch + timedelta(seconds=11 + i),
                    ),
                ),
                commit_log_partition,
                8 + i,
                now + timedelta(seconds=i),
            )
        )
        messages.append(message)
        strategy.submit(message)

    assert next_step.submit.call_count == 1
    assert next_step.submit.call_args_list == [mock.call(messages[0])]


def make_message_for_next_step(
    message: Message[Tick], offset_to_commit: Optional[int]
) -> Message[CommittableTick]:
    return message.replace(CommittableTick(message.payload, offset_to_commit))


def test_provide_commit_strategy() -> None:
    epoch = datetime(1970, 1, 1)
    next_step = mock.Mock()
    strategy = ProvideCommitStrategy(2, next_step, TestingMetricsBackend())

    topic = Topic("messages")
    partition = Partition(topic, 0)

    # First message for partition 0 -> do not commit offset
    message_0_0 = Message(
        BrokerValue(
            Tick(
                0,
                offsets=Interval(1, 2),
                timestamps=Interval(
                    epoch + timedelta(seconds=1), epoch + timedelta(seconds=2)
                ),
            ),
            partition,
            1,
            epoch,
        )
    )

    strategy.submit(message_0_0)
    assert next_step.submit.call_args_list == [
        mock.call(make_message_for_next_step(message_0_0, None))
    ]

    next_step.reset_mock()

    # Offset 2 on partition 1, now we can safely commit offset 1
    message_1_0 = Message(
        BrokerValue(
            Tick(
                1,
                offsets=Interval(11, 12),
                timestamps=Interval(
                    epoch + timedelta(seconds=2), epoch + timedelta(seconds=3)
                ),
            ),
            partition,
            2,
            epoch,
        )
    )

    strategy.submit(message_1_0)

    assert next_step.submit.call_args_list == [
        mock.call(make_message_for_next_step(message_1_0, 1))
    ]

    next_step.reset_mock()

    # Another message on partition 1, can't commit since partition 0 is still on offset 1
    message_1_1 = Message(
        BrokerValue(
            Tick(
                1,
                offsets=Interval(12, 13),
                timestamps=Interval(
                    epoch + timedelta(seconds=3), epoch + timedelta(seconds=6)
                ),
            ),
            partition,
            3,
            epoch,
        )
    )

    strategy.submit(message_1_1)

    assert next_step.submit.call_args_list == [
        mock.call(make_message_for_next_step(message_1_1, None))
    ]

    next_step.reset_mock()

    # A message on partition 0, now we can commit offset 3 since partition 1 is
    # still up to 3.
    message_0_1 = Message(
        BrokerValue(
            Tick(
                0,
                offsets=Interval(2, 4),
                timestamps=Interval(
                    epoch + timedelta(seconds=2), epoch + timedelta(seconds=5)
                ),
            ),
            partition,
            4,
            epoch,
        )
    )

    strategy.submit(message_0_1)

    assert next_step.submit.call_args_list == [
        mock.call(make_message_for_next_step(message_0_1, 3))
    ]


def test_tick_buffer_with_commit_strategy_partition() -> None:
    epoch = datetime(1970, 1, 1)
    now = datetime.now()

    metrics_backend = TestingMetricsBackend()
    next_step = mock.Mock()
    metrics = TestingMetricsBackend()

    strategy = TickBuffer(
        SchedulingWatermarkMode.PARTITION,
        2,
        10,
        ProvideCommitStrategy(2, next_step, metrics_backend),
        metrics,
    )

    topic = Topic("messages")
    commit_log_partition = Partition(topic, 0)

    # First message in partition 0
    # It is submitted for scheduling straight away because we're in partition mode
    # but we cannot commit yet because we need an offset for partition 1 before we can do that
    message_0_0 = Message(
        BrokerValue(
            Tick(
                0,
                offsets=Interval(1, 2),
                timestamps=Interval(epoch, epoch + timedelta(seconds=4)),
            ),
            commit_log_partition,
            4,
            now,
        )
    )
    strategy.submit(message_0_0)

    assert next_step.submit.call_count == 1
    assert next_step.submit.call_args_list == [
        mock.call(make_message_for_next_step(message_0_0, None)),
    ]
    next_step.reset_mock()

    # Message in partition 1 - submitted for scheduling immediately. Now we can commit offset 4
    message_1_0 = Message(
        BrokerValue(
            Tick(
                1,
                offsets=Interval(3, 6),
                timestamps=Interval(
                    epoch + timedelta(seconds=4), epoch + timedelta(seconds=6)
                ),
            ),
            commit_log_partition,
            5,
            now,
        )
    )
    strategy.submit(message_1_0)

    assert next_step.submit.call_count == 1
    assert next_step.submit.call_args_list == [
        mock.call(make_message_for_next_step(message_1_0, 4)),
    ]
    next_step.reset_mock()

    # Another message in partition 0. The timestamp is earlier than message_1_0 but it doesn't matter
    # We still submit immediately and the offset moves to 5
    message_0_1 = Message(
        BrokerValue(
            Tick(
                0,
                offsets=Interval(2, 3),
                timestamps=Interval(
                    epoch + timedelta(seconds=4), epoch + timedelta(seconds=6)
                ),
            ),
            commit_log_partition,
            6,
            now,
        )
    )
    strategy.submit(message_0_1)

    assert next_step.submit.call_count == 1
    assert next_step.submit.call_args_list == [
        mock.call(make_message_for_next_step(message_0_1, 5)),
    ]


def test_tick_buffer_with_commit_strategy_global() -> None:
    epoch = datetime(1970, 1, 1)
    now = datetime.now()

    metrics_backend = TestingMetricsBackend()
    next_step = mock.Mock()

    strategy = TickBuffer(
        SchedulingWatermarkMode.GLOBAL,
        2,
        10,
        ProvideCommitStrategy(2, next_step, metrics_backend),
        metrics_backend,
    )

    topic = Topic("messages")
    commit_log_partition = Partition(topic, 0)

    # First message in partition 0, not submitted to next step
    message_0_0 = Message(
        BrokerValue(
            Tick(
                0,
                offsets=Interval(1, 3),
                timestamps=Interval(epoch, epoch + timedelta(seconds=4)),
            ),
            commit_log_partition,
            4,
            now,
        )
    )
    strategy.submit(message_0_0)

    assert next_step.submit.call_count == 0

    # Another message in partition 0, cannot submit yet
    message_0_1 = Message(
        BrokerValue(
            Tick(
                0,
                offsets=Interval(3, 6),
                timestamps=Interval(
                    epoch + timedelta(seconds=4), epoch + timedelta(seconds=6)
                ),
            ),
            commit_log_partition,
            5,
            now,
        )
    )
    strategy.submit(message_0_1)

    assert next_step.submit.call_count == 0

    # Message in partition 1, submitted to next step since it has the earliest timestamp.
    # Does not commit since we have not submitted anything on the other partition yet.
    message_1_0 = Message(
        BrokerValue(
            Tick(
                1,
                offsets=Interval(100, 120),
                timestamps=Interval(epoch, epoch + timedelta(seconds=3)),
            ),
            commit_log_partition,
            6,
            now,
        )
    )
    strategy.submit(message_1_0)

    assert next_step.submit.call_count == 1
    assert next_step.submit.call_args_list == [
        mock.call(make_message_for_next_step(message_1_0, None)),
    ]

    next_step.reset_mock()

    # Another message in partition 1, now two more messages submitted
    # (message_0_0 and message_1_1). We can only safely commit the offset
    # of message_0_0 (4) when we are submitting it.
    message_1_1 = Message(
        BrokerValue(
            Tick(
                1,
                offsets=Interval(120, 140),
                timestamps=Interval(
                    epoch + timedelta(seconds=3), epoch + timedelta(seconds=4)
                ),
            ),
            commit_log_partition,
            7,
            now,
        )
    )
    strategy.submit(message_1_1)

    assert next_step.submit.call_count == 2
    assert next_step.submit.call_args_list == [
        mock.call(make_message_for_next_step(message_0_0, 4)),
        mock.call(make_message_for_next_step(message_1_1, None)),
    ]


def test_scheduled_subscription_queue() -> None:
    queue = ScheduledSubscriptionQueue()
    assert len(queue) == 0
    assert queue.peek() is None
    with pytest.raises(IndexError):
        queue.popleft()

    epoch = datetime(1970, 1, 1)
    partition = Partition(Topic("test"), 0)

    tick_message = BrokerValue(
        CommittableTick(
            Tick(
                0,
                offsets=Interval(1, 3),
                timestamps=Interval(epoch, epoch + timedelta(minutes=2)),
            ),
            1,
        ),
        partition,
        1,
        epoch,
    )

    futures: Sequence[Future[BrokerValue[KafkaPayload]]] = [Future(), Future()]

    queue.append(tick_message, deque(futures))

    assert len(queue) == 2
    assert queue.peek() == TickSubscription(
        tick_message, futures[0], offset_to_commit=None
    )
    assert queue.popleft() == TickSubscription(
        tick_message, futures[0], offset_to_commit=None
    )
    assert len(queue) == 1

    assert queue.popleft() == TickSubscription(
        tick_message, futures[1], offset_to_commit=1
    )
    assert len(queue) == 0


@pytest.mark.redis_db
def test_produce_scheduled_subscription_message() -> None:
    epoch = datetime(1970, 1, 1)
    metrics_backend = TestingMetricsBackend()
    partition_index = 0
    entity_key = EntityKey.EVENTS
    topic = Topic("scheduled-subscriptions-events")
    partition = Partition(topic, partition_index)

    clock = TestingClock()
    broker_storage: MemoryMessageStorage[KafkaPayload] = MemoryMessageStorage()
    broker: Broker[KafkaPayload] = Broker(broker_storage, clock)
    broker.create_topic(topic, partitions=1)
    producer = broker.get_producer()

    store = RedisSubscriptionDataStore(
        get_redis_client(RedisClientKey.SUBSCRIPTION_STORE),
        entity_key,
        PartitionId(partition_index),
    )
    entity = get_entity(EntityKey.EVENTS)
    # Create 2 subscriptions
    # Subscription 1
    store.create(
        uuid.uuid4(),
        SubscriptionData(
            project_id=1,
            time_window_sec=60,
            resolution_sec=60,
            query="MATCH events SELECT count()",
            entity=entity,
            metadata={},
        ),
    )

    # Subscription 2
    store.create(
        uuid.uuid4(),
        SubscriptionData(
            project_id=2,
            time_window_sec=2 * 60,
            resolution_sec=2 * 60,
            query="MATCH events SELECT count(event_id)",
            entity=entity,
            metadata={},
        ),
    )

    schedulers = {
        partition_index: SubscriptionScheduler(
            entity_key,
            store,
            PartitionId(partition_index),
            cache_ttl=timedelta(seconds=300),
            metrics=metrics_backend,
        )
    }

    commit = mock.Mock()

    strategy = ProduceScheduledSubscriptionMessage(
        schedulers,
        producer,
        KafkaTopicSpec(SnubaTopic.SUBSCRIPTION_SCHEDULED_EVENTS),
        commit,
        None,
        metrics_backend,
    )

    message = Message(
        BrokerValue(
            CommittableTick(
                Tick(
                    0,
                    offsets=Interval(1, 3),
                    timestamps=Interval(epoch, epoch + timedelta(minutes=2)),
                ),
                2,
            ),
            partition,
            1,
            epoch,
        )
    )

    strategy.submit(message)

    # 3 subscriptions should be scheduled (2 x subscription 1, 1 x subscription 2)
    codec = SubscriptionScheduledTaskEncoder()

    # 2 subscriptions scheduled at epoch
    first_message = broker_storage.consume(partition, 0)
    assert first_message is not None
    assert codec.decode(first_message.payload).timestamp == epoch

    second_message = broker_storage.consume(partition, 1)
    assert second_message is not None
    assert codec.decode(second_message.payload).timestamp == epoch

    # 1 subscription scheduled at epoch + 1
    third_message = broker_storage.consume(partition, 2)
    assert third_message is not None
    assert codec.decode(third_message.payload).timestamp == epoch + timedelta(minutes=1)

    # No 4th message
    assert broker_storage.consume(partition, 3) is None

    # Offset is committed when poll is called
    assert commit.call_count == 0
    strategy.poll()
    assert commit.call_count == 1
    assert commit.call_args == mock.call(message.committable)

    # Close the strategy
    strategy.close()
    strategy.join()


@pytest.mark.redis_db
def test_produce_stale_message() -> None:
    stale_threshold_seconds = 90
    now = datetime.now()
    metrics_backend = TestingMetricsBackend()
    partition_index = 0
    entity_key = EntityKey.EVENTS
    topic = Topic("scheduled-subscriptions-events")
    partition = Partition(topic, partition_index)

    clock = TestingClock()
    broker_storage: MemoryMessageStorage[KafkaPayload] = MemoryMessageStorage()
    broker: Broker[KafkaPayload] = Broker(broker_storage, clock)
    broker.create_topic(topic, partitions=1)
    producer = broker.get_producer()

    store = RedisSubscriptionDataStore(
        get_redis_client(RedisClientKey.SUBSCRIPTION_STORE),
        entity_key,
        PartitionId(partition_index),
    )

    entity = get_entity(EntityKey.EVENTS)
    # Create subscription
    store.create(
        uuid.uuid4(),
        SubscriptionData(
            project_id=1,
            time_window_sec=60,
            resolution_sec=60,
            query="MATCH events SELECT count()",
            entity=entity,
            metadata={},
        ),
    )

    schedulers = {
        partition_index: SubscriptionScheduler(
            entity_key,
            store,
            PartitionId(partition_index),
            cache_ttl=timedelta(seconds=300),
            metrics=metrics_backend,
        )
    }

    commit = mock.Mock()

    strategy = ProduceScheduledSubscriptionMessage(
        schedulers,
        producer,
        KafkaTopicSpec(SnubaTopic.SUBSCRIPTION_SCHEDULED_EVENTS),
        commit,
        stale_threshold_seconds,
        metrics_backend,
    )

    # Produce a stale message. Since the tick spans an interval of 60 seconds,
    # the subscription will be executed once in this window (no matter what
    # jitter gets applied to it)
    stale_message = Message(
        BrokerValue(
            CommittableTick(
                Tick(
                    0,
                    offsets=Interval(1, 3),
                    timestamps=Interval(
                        now - timedelta(minutes=3), now - timedelta(seconds=60)
                    ),
                ),
                2,
            ),
            partition,
            1,
            now,
        )
    )

    strategy.submit(stale_message)

    # Nothing got scheduled
    assert broker_storage.consume(partition, 0) is None

    # Offset gets committed
    assert commit.call_count == 1
    assert commit.call_args == mock.call(stale_message.committable)

    # Produce a non stale message
    non_stale_message = Message(
        BrokerValue(
            CommittableTick(
                Tick(
                    0,
                    offsets=Interval(3, 4),
                    timestamps=Interval(now - timedelta(seconds=60), now),
                ),
                2,
            ),
            partition,
            1,
            now,
        )
    )

    strategy.submit(non_stale_message)

    # Non stale message gets produced
    assert broker_storage.consume(partition, 0) is not None

    # Offset gets committed on poll
    strategy.poll()
    assert commit.call_count == 2
    assert commit.call_args == mock.call(non_stale_message.committable)

    # Close the strategy
    strategy.close()
    strategy.join()
