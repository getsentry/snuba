import logging
from typing import Callable, Mapping, Sequence, Tuple, Union

from arroyo import Message, Partition
from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies import ProcessingStrategy
from arroyo.processing.strategies import ProcessingStrategy as ProcessingStep
from arroyo.processing.strategies import ProcessingStrategyFactory
from arroyo.processing.strategies.streaming import CollectStep, TransformStep
from arroyo.types import Position

from snuba.consumers.consumer import (
    BytesInsertBatch,
    InsertBatchWriter,
    MultistorageCollector,
    ProcessedMessageBatchWriter,
)
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.processor import ReplacementBatch
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger("snuba.dead-letter-consumer")


def process_dead_letter_messages(
    message: Message[KafkaPayload],
) -> Sequence[Tuple[StorageKey, BytesInsertBatch]]:
    if message.payload.key:
        key = message.payload.key.decode("utf-8")
        storage_key = StorageKey(key)
        return [
            (
                storage_key,
                BytesInsertBatch(rows=[message.payload.value], origin_timestamp=None),
            )
        ]
    return []


class DeadLetterConsumerProcessingStrategyFactory(
    ProcessingStrategyFactory[KafkaPayload]
):
    def __init__(
        self,
        storages: Sequence[WritableTableStorage],
        max_batch_size: int,
        max_batch_time: float,
        metrics: MetricsBackend,
    ):
        self.__storages = storages
        self.__max_batch_size = max_batch_size
        self.__max_batch_time = max_batch_time
        self.__metrics = metrics

    def __build_batch_writer(
        self, storage: WritableTableStorage
    ) -> ProcessedMessageBatchWriter:
        return ProcessedMessageBatchWriter(
            InsertBatchWriter(
                storage.get_table_writer().get_batch_writer(
                    self.__metrics,
                    {"load_balancing": "in_order", "insert_distributed_sync": 1},
                ),
                MetricsWrapper(
                    self.__metrics,
                    "insertions",
                    {"storage": storage.get_storage_key().value},
                ),
            ),
            None,
        )

    def __build_collector(
        self,
    ) -> ProcessingStep[
        Sequence[Tuple[StorageKey, Union[None, BytesInsertBatch, ReplacementBatch]]]
    ]:
        return MultistorageCollector(
            {
                storage.get_storage_key(): self.__build_batch_writer(storage)
                for storage in self.__storages
            },
            None,
            None,
        )

    def create(
        self, commit: Callable[[Mapping[Partition, Position]], None]
    ) -> ProcessingStrategy[KafkaPayload]:

        collect = CollectStep(
            self.__build_collector,
            commit,
            self.__max_batch_size,
            self.__max_batch_time,
        )
        return TransformStep(process_dead_letter_messages, collect)
