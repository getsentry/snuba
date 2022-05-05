from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import timedelta
from typing import Callable, Mapping, Optional, Sequence, cast

from arroyo import Message, Partition
from arroyo.backends.abstract import Producer
from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies import ProcessingStrategy
from arroyo.processing.strategies.abstract import ProcessingStrategyFactory
from arroyo.types import Position

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.redis import redis_client
from snuba.subscriptions.codecs import SubscriptionScheduledTaskEncoder
from snuba.subscriptions.data import PartitionId
from snuba.subscriptions.executor_consumer import SubscriptionExecutorProcessingFactory
from snuba.subscriptions.scheduler import SubscriptionScheduler
from snuba.subscriptions.scheduler_processing_strategy import TickBuffer
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.subscriptions.utils import SchedulingWatermarkMode, Tick
from snuba.utils.metrics import MetricsBackend


class CombinedSchedulerExecutorFactory(ProcessingStrategyFactory[Tick]):
    """
    An alternative to the separate schedule and executor consumers / strategies
    which can be used to schedule and execute subscriptions for multiple entities
    in a dataset all at once. This is designed to be easier to run (less pods)
    but does not scale with subscription volume as like the separate
    scheduler and executor.

    It should only be used in environments where:
    - there are relatively few subscriptions
    - the main topic only has a single partition (since offsets on the commit log
    are committed once a subscription has finished running - regardless of the
    original partition they came from)
    """

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

        self.__executor_factory = SubscriptionExecutorProcessingFactory(
            executor,
            max_concurrent_queries,
            dataset,
            entity_names,
            producer,
            metrics,
            stale_threshold_seconds,
            result_topic,
        )

        modes = {
            self._get_entity_watermark_mode(entity_key) for entity_key in entity_keys
        }

        mode = modes.pop()

        assert len(modes) == 0, "Entities provided to not share the same mode"

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

    def create(
        self, commit: Callable[[Mapping[Partition, Position]], None]
    ) -> ProcessingStrategy[Tick]:
        execute_step = self.__executor_factory.create(commit)
        return TickBuffer(
            self.__mode,
            self.__partitions,
            None,
            ForwardToExecutor(self.__schedulers, execute_step),
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
            self.__next_step.submit(
                cast(Message[KafkaPayload], replace(message, payload=task))
            )

    def close(self) -> None:
        self.__closed = True
        self.__next_step.close()

    def terminate(self) -> None:
        self.__closed = True
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.join(timeout)
