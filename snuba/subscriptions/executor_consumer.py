from __future__ import annotations

import logging
from typing import Callable, Mapping, Optional

from arroyo import Message, Partition, Topic
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload
from arroyo.processing import StreamProcessor
from arroyo.processing.strategies import ProcessingStrategy
from arroyo.processing.strategies.abstract import ProcessingStrategyFactory
from arroyo.types import Position

from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import ENTITY_NAME_LOOKUP, get_entity
from snuba.datasets.factory import get_dataset
from snuba.utils.metrics import MetricsBackend
from snuba.utils.streams.configuration_builder import build_kafka_consumer_configuration

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
