from datetime import timedelta
from typing import Mapping, NamedTuple, Optional, Sequence

from arroyo import Message, Partition, Topic
from arroyo.backends.abstract import Producer
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload
from arroyo.commit import ONCE_PER_SECOND
from arroyo.processing import StreamProcessor
from arroyo.processing.strategies import ProcessingStrategy
from arroyo.processing.strategies.abstract import ProcessingStrategyFactory
from arroyo.types import Commit

from snuba import settings
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.redis import RedisClientKey, get_redis_client
from snuba.subscriptions.codecs import SubscriptionScheduledTaskEncoder
from snuba.subscriptions.data import PartitionId
from snuba.subscriptions.executor_consumer import SubscriptionExecutorProcessingFactory
from snuba.subscriptions.scheduler import SubscriptionScheduler
from snuba.subscriptions.scheduler_consumer import CommitLogTickConsumer
from snuba.subscriptions.scheduler_processing_strategy import TickBuffer
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.subscriptions.utils import SchedulingWatermarkMode, Tick
from snuba.utils.metrics import MetricsBackend
from snuba.utils.streams.configuration_builder import build_kafka_consumer_configuration

redis_client = get_redis_client(RedisClientKey.SUBSCRIPTION_STORE)


class TopicConfig(NamedTuple):
    partition: int
    commit_log_topic: KafkaTopicSpec
    result_topic: KafkaTopicSpec
    synchronization_timestamp: str


def build_scheduler_executor_consumer(
    dataset_name: str,
    entity_names: Sequence[str],
    consumer_group: str,
    followed_consumer_group: str,
    producer: Producer[KafkaPayload],
    auto_offset_reset: str,
    strict_offset_reset: bool,
    schedule_ttl: int,
    delay_seconds: Optional[int],
    stale_threshold_seconds: Optional[int],
    total_concurrent_queries: int,
    metrics: MetricsBackend,
    health_check_file: Optional[str] = None,
) -> StreamProcessor[Tick]:
    dataset = get_dataset(dataset_name)

    # Only entities in the same dataset with the same partition count, commit log
    # topic and result topics may be run together.
    def get_topic_configuration_for_entity(
        entity_name: str,
    ) -> TopicConfig:
        storage = get_entity(EntityKey(entity_name)).get_writable_storage()
        assert storage is not None
        stream_loader = storage.get_table_writer().get_stream_loader()

        partition_count = stream_loader.get_default_topic_spec().partitions_number

        commit_log_topic_spec = stream_loader.get_commit_log_topic_spec()
        assert commit_log_topic_spec is not None

        result_topic_spec = stream_loader.get_subscription_result_topic_spec()
        assert result_topic_spec is not None

        synchronization_timestamp = (
            stream_loader.get_subscription_sychronization_timestamp()
        )
        assert synchronization_timestamp is not None

        return TopicConfig(
            partition_count,
            commit_log_topic_spec,
            result_topic_spec,
            synchronization_timestamp,
        )

    entity_topic_configurations = [
        get_topic_configuration_for_entity(entity_name) for entity_name in entity_names
    ]
    entity_topic_configuration = entity_topic_configurations[0]
    for c in entity_topic_configurations[1:]:
        assert c == entity_topic_configuration

    (
        partitions,
        commit_log_topic,
        result_topic,
        synchronization_timestamp,
    ) = entity_topic_configuration

    tick_consumer = CommitLogTickConsumer(
        KafkaConsumer(
            build_kafka_consumer_configuration(
                commit_log_topic.topic,
                consumer_group,
                auto_offset_reset=auto_offset_reset,
                strict_offset_reset=strict_offset_reset,
            ),
        ),
        followed_consumer_group=followed_consumer_group,
        metrics=metrics,
        synchronization_timestamp=synchronization_timestamp,
        time_shift=(
            timedelta(seconds=delay_seconds * -1) if delay_seconds is not None else None
        ),
    )

    factory = CombinedSchedulerExecutorFactory(
        dataset,
        entity_names,
        partitions,
        total_concurrent_queries,
        producer,
        metrics,
        stale_threshold_seconds,
        result_topic.topic_name,
        schedule_ttl,
        health_check_file,
    )

    return StreamProcessor(
        tick_consumer, Topic(commit_log_topic.topic_name), factory, ONCE_PER_SECOND
    )


class CombinedSchedulerExecutorFactory(ProcessingStrategyFactory[Tick]):
    """
    An alternative to the separate scheduler and executor which can be used to schedule
    and execute subscriptions for multiple entities in a dataset all at once. This is designed
    to be easier to run (less containers) but does not scale with subscription volume like
    the separate scheduler and executor.

    It should only be used in environments where:
    - there are relatively few subscriptions
    - the main topic only has a single partition (since offsets on the commit log
    are committed once a subscription has finished running - regardless of the
    original partition they came from)
    """

    def __init__(
        self,
        dataset: Dataset,
        entity_names: Sequence[str],
        partitions: int,
        total_concurrent_queries: int,
        producer: Producer[KafkaPayload],
        metrics: MetricsBackend,
        stale_threshold_seconds: Optional[int],
        result_topic: str,
        schedule_ttl: int,
        health_check_file: Optional[str] = None,
    ) -> None:
        self.__partitions = partitions
        self.__entity_names = entity_names
        self.__metrics = metrics

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

        # Just apply the max buffer size if they are configured differently
        # for each entity that is being run together
        self.__buffer_size = max(
            [
                settings.SUBSCRIPTIONS_ENTITY_BUFFER_SIZE.get(
                    entity_key.value, settings.SUBSCRIPTIONS_DEFAULT_BUFFER_SIZE
                )
                for entity_key in entity_keys
            ]
        )

        self.__executor_factory = SubscriptionExecutorProcessingFactory(
            total_concurrent_queries,
            # total_partition_count should always be 1 when using the combined executor/scheduler
            1,
            dataset,
            entity_names,
            producer,
            metrics,
            stale_threshold_seconds,
            result_topic,
            health_check_file,
        )

        modes = {
            self._get_entity_watermark_mode(entity_key) for entity_key in entity_keys
        }

        mode = modes.pop()

        assert len(modes) == 0, "Entities provided do not share the same mode"

        self.__mode = mode

    def _get_entity_watermark_mode(
        self, entity_key: EntityKey
    ) -> SchedulingWatermarkMode:
        storage = get_entity(entity_key).get_writable_storage()
        assert storage is not None, "Entity does not have a writable storage"
        stream_loader = storage.get_table_writer().get_stream_loader()
        mode = stream_loader.get_subscription_scheduler_mode()
        assert mode is not None, "Entity is not subscriptable"
        return mode

    def create_with_partitions(
        self,
        commit: Commit,
        partitions: Mapping[Partition, int],
    ) -> ProcessingStrategy[Tick]:
        execute_step = self.__executor_factory.create_with_partitions(
            commit, partitions
        )
        return TickBuffer(
            self.__mode,
            self.__partitions,
            self.__buffer_size,
            ForwardToExecutor(self.__schedulers, execute_step),
            self.__metrics,
        )


class ForwardToExecutor(ProcessingStrategy[Tick]):
    def __init__(
        self,
        schedulers: Sequence[Mapping[int, SubscriptionScheduler]],
        next_step: ProcessingStrategy[KafkaPayload],
    ) -> None:
        self.__next_step = next_step

        self.__schedulers = schedulers
        self.__encoder = SubscriptionScheduledTaskEncoder()

        self.__closed = False

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[Tick]) -> None:
        assert not self.__closed

        tick = message.payload
        assert tick.partition is not None

        tasks = []
        for entity_scheduler in self.__schedulers:
            tasks.extend([task for task in entity_scheduler[tick.partition].find(tick)])

        encoded_tasks = [self.__encoder.encode(task) for task in tasks]

        for task in encoded_tasks:
            self.__next_step.submit(message.replace(task))

    def close(self) -> None:
        self.__closed = True
        self.__next_step.close()

    def terminate(self) -> None:
        self.__closed = True
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.join(timeout)
