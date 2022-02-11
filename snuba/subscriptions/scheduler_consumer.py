import logging
from datetime import datetime, timedelta
from typing import Callable, Mapping, MutableMapping, NamedTuple, Optional, Sequence

from arroyo import Message, Partition, Topic
from arroyo.backends.abstract import Consumer, Producer
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload
from arroyo.backends.local.backend import LocalBroker as Broker
from arroyo.processing import StreamProcessor
from arroyo.processing.strategies import ProcessingStrategy
from arroyo.processing.strategies.abstract import ProcessingStrategyFactory
from arroyo.synchronized import commit_codec
from arroyo.types import Position
from arroyo.utils.profiler import ProcessingStrategyProfilerWrapperFactory

from snuba import settings
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.subscriptions.data import PartitionId
from snuba.subscriptions.scheduler_load_testing import LoadTestingSubscriptionScheduler
from snuba.subscriptions.scheduler_processing_strategy import (
    ProduceScheduledSubscriptionMessage,
    ProvideCommitStrategy,
    TickBuffer,
)
from snuba.subscriptions.utils import SchedulingWatermarkMode, Tick
from snuba.utils.metrics import MetricsBackend
from snuba.utils.streams.configuration_builder import build_kafka_consumer_configuration
from snuba.utils.types import Interval, InvalidRangeError

logger = logging.getLogger(__name__)


class MessageDetails(NamedTuple):
    offset: int
    orig_message_ts: datetime


class CommitLogTickConsumer(Consumer[Tick]):
    """
    The ``CommitLogTickConsumer`` is a ``Consumer`` implementation that differs
    from other ``Consumer`` implementations in that the messages returned returns
    contain a ``Tick`` that is derived from the timestamps of the previous
    two messages received within a partition.

    In other words, this consumer provides a measure of the progression of
    time, using the advancement of the broker timestamp within a Kafka
    partition as a "virtual clock" rather than depending on wall clock time.

    This consumer must follow a commit log topic, and requires that the
    ``message.timestamp.type`` configuration of the original topic being
    followed by the commit log topic  is set to ``LogAppendTime``, so that
    the message time is set by the primary broker for the topic -- not the
    producer of the message -- ensuring that each partition timestamp moves
    monotonically.
    """

    # Since this consumer deals with the intervals *between* messages rather
    # the individual messages themselves, this introduces some additional
    # complexity into the way that offsets are managed. Take this example,
    # where a partition contains three messages:
    #
    #    Message:            A         B         C         D
    #    Offset:             0         1         2         3
    #    Timeline:   --------+---------+---------+---------+------>>>
    #
    # Consuming message "A" (via a call to ``poll``) does not cause a tick to
    # be returned, since an tick interval cannot be formed with the timestamp
    # from only one message. When message B is consumed, we can form a tick
    # interval using the timestamps from A and B.
    #
    # When storing (or committing) offsets, we need to be careful that we
    # correctly commit the offsets that represent the interval so that
    # intervals are not repeated or skipped when a consumer restarts (or more
    # likely rebalances, which can be generalized to a restart operation.)
    #
    # Take the previously described scenario where we had just returned a tick
    # interval that was represented by the mesasges A and B: without taking any
    # precautions, the Kafka consumer would use the next offset from message B
    # for commit, which would be 2 in this case (1 + 1). If the consumer were
    # to crash and restart, it would resume at offset 2, causing the next tick
    # interval returned to be for the messages C and D -- in this case, B and C
    # was never returned! To avoid skipping intervals when restarting, the
    # consumer would have had to commit the offset 1 (the offset of message B)
    # to ensure that upon restart, the next interval would be the interval
    # between B and C, since the message B was the first message received by
    # the consumer.
    def __init__(
        self,
        consumer: Consumer[KafkaPayload],
        followed_consumer_group: str,
        time_shift: Optional[timedelta] = None,
    ) -> None:
        self.__consumer = consumer
        self.__followed_consumer_group = followed_consumer_group
        self.__previous_messages: MutableMapping[Partition, MessageDetails] = {}
        self.__time_shift = time_shift if time_shift is not None else timedelta()
        self.__end_of_messages = False

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        self.__consumer.subscribe(topics, on_assign=on_assign, on_revoke=on_revoke)

    def unsubscribe(self) -> None:
        self.__consumer.unsubscribe()

    def poll(self, timeout: Optional[float] = None) -> Optional[Message[Tick]]:
        message = self.__consumer.poll(timeout)
        if message is None:
            if self.__end_of_messages is False:
                self.__end_of_messages = True
                print("No more messages!")
                raise Exception("No more messages")
            return None

        commit = commit_codec.decode(message.payload)
        assert commit.orig_message_ts is not None

        if commit.group != self.__followed_consumer_group:
            return None

        previous_message = self.__previous_messages.get(commit.partition)

        result: Optional[Message[Tick]]
        if previous_message is not None:
            try:
                time_interval = Interval(
                    previous_message.orig_message_ts, commit.orig_message_ts
                )
            except InvalidRangeError:
                logger.warning(
                    "Could not construct valid time interval between %r and %r!",
                    previous_message,
                    MessageDetails(commit.offset, commit.orig_message_ts),
                    exc_info=True,
                )
                return None
            else:
                result = Message(
                    message.partition,
                    message.offset,
                    Tick(
                        commit.partition.index,
                        Interval(previous_message.offset, commit.offset),
                        time_interval,
                    ).time_shift(self.__time_shift),
                    message.timestamp,
                )
        else:
            result = None

        self.__previous_messages[commit.partition] = MessageDetails(
            commit.offset, commit.orig_message_ts
        )

        return result

    def pause(self, partitions: Sequence[Partition]) -> None:
        self.__consumer.pause(partitions)

    def resume(self, partitions: Sequence[Partition]) -> None:
        self.__consumer.resume(partitions)

    def paused(self) -> Sequence[Partition]:
        return self.__consumer.paused()

    def tell(self) -> Mapping[Partition, int]:
        return self.__consumer.tell()

    def seek(self, offsets: Mapping[Partition, int]) -> None:
        self.__previous_messages = {}

        self.__consumer.seek(offsets)

    def stage_positions(self, positions: Mapping[Partition, Position]) -> None:
        return self.__consumer.stage_positions(positions)

    def commit_positions(self) -> Mapping[Partition, Position]:
        return self.__consumer.commit_positions()

    def close(self, timeout: Optional[float] = None) -> None:
        return self.__consumer.close(timeout)

    @property
    def closed(self) -> bool:
        return self.__consumer.closed


class SchedulerBuilder:
    def __init__(
        self,
        broker: Broker[KafkaPayload],
        entity_name: str,
        consumer_group: str,
        followed_consumer_group: str,
        producer: Producer[KafkaPayload],
        auto_offset_reset: str,
        schedule_ttl: int,
        delay_seconds: Optional[int],
        metrics: MetricsBackend,
        profile_path: Optional[str] = None,
        load_factor: int = 1,
    ) -> None:
        self.__broker = broker
        self.__entity_key = EntityKey(entity_name)

        storage = get_entity(self.__entity_key).get_writable_storage()

        assert (
            storage is not None
        ), f"Entity {entity_name} does not have a writable storage by default."

        stream_loader = storage.get_table_writer().get_stream_loader()

        commit_log_topic_spec = stream_loader.get_commit_log_topic_spec()
        assert commit_log_topic_spec is not None
        self.__commit_log_topic_spec = commit_log_topic_spec

        scheduled_topic_spec = stream_loader.get_subscription_scheduled_topic_spec()
        assert scheduled_topic_spec is not None
        self.__scheduled_topic_spec = scheduled_topic_spec

        mode = stream_loader.get_subscription_scheduler_mode()
        assert mode is not None

        self.__mode = mode

        self.__partitions = stream_loader.get_default_topic_spec().partitions_number

        self.__consumer_group = consumer_group
        self.__followed_consumer_group = followed_consumer_group
        self.__producer = producer
        self.__auto_offset_reset = auto_offset_reset
        self.__schedule_ttl = schedule_ttl
        self.__delay_seconds = delay_seconds
        self.__metrics = metrics
        self.__profile_path = profile_path
        self.__load_factor = load_factor

    def build_consumer(self) -> StreamProcessor[Tick]:
        return StreamProcessor(
            self.__build_file_tick_consumer(),
            Topic(self.__commit_log_topic_spec.topic_name),
            self.__build_strategy_factory(),
        )

    def __build_strategy_factory(self) -> ProcessingStrategyFactory[Tick]:
        strategy_factory = SubscriptionSchedulerProcessingFactory(
            self.__entity_key,
            self.__mode,
            self.__schedule_ttl,
            self.__partitions,
            self.__producer,
            self.__scheduled_topic_spec,
            self.__metrics,
            self.__load_factor,
        )

        if self.__profile_path is not None:
            return ProcessingStrategyProfilerWrapperFactory(
                strategy_factory, self.__profile_path
            )

        return strategy_factory

    def __build_file_tick_consumer(self) -> CommitLogTickConsumer:
        consumer = self.__broker.get_consumer(self.__commit_log_topic_spec.topic_name)
        return CommitLogTickConsumer(
            consumer,
            followed_consumer_group=self.__followed_consumer_group,
            time_shift=(
                timedelta(seconds=self.__delay_seconds * -1)
                if self.__delay_seconds is not None
                else None
            ),
        )

    def __build_tick_consumer(self) -> CommitLogTickConsumer:
        return CommitLogTickConsumer(
            KafkaConsumer(
                build_kafka_consumer_configuration(
                    self.__commit_log_topic_spec.topic,
                    self.__consumer_group,
                    auto_offset_reset=self.__auto_offset_reset,
                ),
            ),
            followed_consumer_group=self.__followed_consumer_group,
            time_shift=(
                timedelta(seconds=self.__delay_seconds * -1)
                if self.__delay_seconds is not None
                else None
            ),
        )


class SubscriptionSchedulerProcessingFactory(ProcessingStrategyFactory[Tick]):
    def __init__(
        self,
        entity_key: EntityKey,
        mode: SchedulingWatermarkMode,
        schedule_ttl: int,
        partitions: int,
        producer: Producer[KafkaPayload],
        scheduled_topic_spec: KafkaTopicSpec,
        metrics: MetricsBackend,
        load_factor: int,
    ) -> None:
        self.__mode = mode
        self.__partitions = partitions
        self.__producer = producer
        self.__scheduled_topic_spec = scheduled_topic_spec
        self.__metrics = metrics

        self.__buffer_size = settings.SUBSCRIPTIONS_ENTITY_BUFFER_SIZE.get(
            entity_key.value, settings.SUBSCRIPTIONS_DEFAULT_BUFFER_SIZE
        )

        self.__schedulers = {
            index: LoadTestingSubscriptionScheduler(
                PartitionId(index),
                cache_ttl=timedelta(seconds=schedule_ttl),
                metrics=self.__metrics,
                entity_key=entity_key,
                load_factor=load_factor,
            )
            for index in range(self.__partitions)
        }

    def create(
        self, commit: Callable[[Mapping[Partition, Position]], None]
    ) -> ProcessingStrategy[Tick]:
        schedule_step = ProduceScheduledSubscriptionMessage(
            self.__schedulers,
            self.__producer,
            self.__scheduled_topic_spec,
            commit,
            self.__metrics,
        )

        return TickBuffer(
            self.__mode,
            self.__partitions,
            self.__buffer_size,
            ProvideCommitStrategy(self.__partitions, schedule_step, self.__metrics),
            self.__metrics,
        )
