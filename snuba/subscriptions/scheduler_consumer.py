import logging
from datetime import datetime, timedelta
from typing import Callable, Mapping, MutableMapping, NamedTuple, Optional, Sequence

from arroyo.backends.abstract import Consumer, Producer
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload
from arroyo.backends.kafka.commit import CommitCodec
from arroyo.commit import ONCE_PER_SECOND
from arroyo.processing import StreamProcessor
from arroyo.processing.strategies import ProcessingStrategy
from arroyo.processing.strategies.abstract import ProcessingStrategyFactory
from arroyo.processing.strategies.healthcheck import Healthcheck
from arroyo.types import BrokerValue, Commit, Partition, Topic

from snuba import settings
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.redis import RedisClientKey, get_redis_client
from snuba.subscriptions.data import PartitionId
from snuba.subscriptions.scheduler import SubscriptionScheduler
from snuba.subscriptions.scheduler_processing_strategy import (
    ProduceScheduledSubscriptionMessage,
    ProvideCommitStrategy,
    TickBuffer,
)
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.subscriptions.utils import SchedulingWatermarkMode, Tick
from snuba.utils.metrics import MetricsBackend
from snuba.utils.streams.configuration_builder import build_kafka_consumer_configuration
from snuba.utils.types import Interval, InvalidRangeError

logger = logging.getLogger(__name__)

commit_codec = CommitCodec()
redis_client = get_redis_client(RedisClientKey.SUBSCRIPTION_STORE)


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

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        def revocation_callback(partitions: Sequence[Partition]) -> None:
            self.__previous_messages = {}

            if on_revoke is not None:
                on_revoke(partitions)

        self.__consumer.subscribe(
            topics, on_assign=on_assign, on_revoke=revocation_callback
        )

    def unsubscribe(self) -> None:
        self.__consumer.unsubscribe()

    def poll(self, timeout: Optional[float] = None) -> Optional[BrokerValue[Tick]]:
        value = self.__consumer.poll(timeout)
        if value is None:
            return None

        try:
            commit = commit_codec.decode(value.payload)
            assert commit.orig_message_ts is not None
        except Exception:
            logger.error(
                f"Error decoding commit log message for followed group: {self.__followed_consumer_group}.",
                extra={"payload": str(value.payload), "offset": value.offset},
                exc_info=True,
            )
            return None

        if commit.group != self.__followed_consumer_group:
            return None

        previous_message = self.__previous_messages.get(commit.partition)

        result: Optional[BrokerValue[Tick]]
        if previous_message is not None:
            try:
                time_interval = Interval(
                    previous_message.orig_message_ts, commit.orig_message_ts
                )
                offset_interval = Interval(previous_message.offset, commit.offset)
            except InvalidRangeError:
                logger.warning(
                    "Could not construct valid interval between %r and %r!",
                    previous_message,
                    MessageDetails(commit.offset, commit.orig_message_ts),
                    exc_info=True,
                )
                return None
            else:
                result = BrokerValue(
                    Tick(
                        commit.partition.index,
                        offset_interval,
                        time_interval,
                    ).time_shift(self.__time_shift),
                    value.partition,
                    value.offset,
                    value.timestamp,
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

    def stage_offsets(self, offsets: Mapping[Partition, int]) -> None:
        return self.__consumer.stage_offsets(offsets)

    def commit_offsets(self) -> Mapping[Partition, int]:
        return self.__consumer.commit_offsets()

    def close(self, timeout: Optional[float] = None) -> None:
        return self.__consumer.close(timeout)

    @property
    def closed(self) -> bool:
        return self.__consumer.closed


class SchedulerBuilder:
    def __init__(
        self,
        entity_name: str,
        consumer_group: str,
        followed_consumer_group: str,
        bootstrap_servers: Sequence[str],
        producer: Producer[KafkaPayload],
        auto_offset_reset: str,
        strict_offset_reset: Optional[bool],
        schedule_ttl: int,
        delay_seconds: Optional[int],
        stale_threshold_seconds: Optional[int],
        metrics: MetricsBackend,
        slice_id: Optional[int] = None,
        health_check_file: Optional[str] = None,
    ) -> None:
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
        self.__bootstrap_servers = bootstrap_servers
        self.__producer = producer
        self.__auto_offset_reset = auto_offset_reset
        self.__strict_offset_reset = strict_offset_reset
        self.__schedule_ttl = schedule_ttl
        self.__delay_seconds = delay_seconds
        self.__stale_threshold_seconds = stale_threshold_seconds
        self.__metrics = metrics
        self.__slice_id = slice_id
        self.__health_check_file = health_check_file

    def build_consumer(self) -> StreamProcessor[Tick]:
        return StreamProcessor(
            self.__build_tick_consumer(),
            Topic(
                self.__commit_log_topic_spec.get_physical_topic_name(self.__slice_id)
            ),
            self.__build_strategy_factory(),
            ONCE_PER_SECOND,
        )

    def __build_strategy_factory(self) -> ProcessingStrategyFactory[Tick]:
        return SubscriptionSchedulerProcessingFactory(
            self.__entity_key,
            self.__mode,
            self.__schedule_ttl,
            self.__stale_threshold_seconds,
            self.__partitions,
            self.__producer,
            self.__scheduled_topic_spec,
            self.__metrics,
            self.__slice_id,
            self.__health_check_file,
        )

    def __build_tick_consumer(self) -> CommitLogTickConsumer:
        consumer_configuration = build_kafka_consumer_configuration(
            self.__commit_log_topic_spec.topic,
            self.__consumer_group,
            self.__slice_id,
            bootstrap_servers=self.__bootstrap_servers,
            auto_offset_reset=self.__auto_offset_reset,
            strict_offset_reset=self.__strict_offset_reset,
        )

        return CommitLogTickConsumer(
            KafkaConsumer(consumer_configuration),
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
        stale_threshold_seconds: Optional[int],
        partitions: int,
        producer: Producer[KafkaPayload],
        scheduled_topic_spec: KafkaTopicSpec,
        metrics: MetricsBackend,
        slice_id: Optional[int] = None,
        health_check_file: Optional[str] = None,
    ) -> None:
        self.__mode = mode
        self.__stale_threshold_seconds = stale_threshold_seconds
        self.__partitions = partitions
        self.__producer = producer
        self.__scheduled_topic_spec = scheduled_topic_spec
        self.__metrics = metrics
        self.__slice_id = slice_id
        self.__health_check_file = health_check_file

        self.__buffer_size = settings.SUBSCRIPTIONS_ENTITY_BUFFER_SIZE.get(
            entity_key.value, settings.SUBSCRIPTIONS_DEFAULT_BUFFER_SIZE
        )

        self.__schedulers = {
            index: SubscriptionScheduler(
                entity_key,
                RedisSubscriptionDataStore(
                    redis_client, entity_key, PartitionId(index)
                ),
                partition_id=PartitionId(index),
                cache_ttl=timedelta(seconds=schedule_ttl),
                metrics=self.__metrics,
                slice_id=self.__slice_id,
            )
            for index in range(self.__partitions)
        }

    def create_with_partitions(
        self,
        commit: Commit,
        partitions: Mapping[Partition, int],
    ) -> ProcessingStrategy[Tick]:
        schedule_step = ProduceScheduledSubscriptionMessage(
            self.__schedulers,
            self.__producer,
            self.__scheduled_topic_spec,
            commit,
            self.__stale_threshold_seconds,
            self.__metrics,
            self.__slice_id,
        )

        strategy: ProcessingStrategy[Tick] = TickBuffer(
            self.__mode,
            self.__partitions,
            self.__buffer_size,
            ProvideCommitStrategy(self.__partitions, schedule_step, self.__metrics),
            self.__metrics,
        )

        if self.__health_check_file:
            strategy = Healthcheck(self.__health_check_file, strategy)

        return strategy
