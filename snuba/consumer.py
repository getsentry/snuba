import logging
from datetime import datetime
from typing import Any, Mapping, MutableSequence, NamedTuple, Optional, Sequence

import rapidjson
from confluent_kafka import Producer as ConfluentKafkaProducer

from snuba.datasets.storage import WritableTableStorage
from snuba.processor import InsertBatch, ProcessedMessage, ReplacementBatch
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.streams.batching import AbstractBatchWorker
from snuba.utils.streams.kafka import KafkaPayload
from snuba.utils.streams.types import Message, Topic
from snuba.writer import WriterTableRow


logger = logging.getLogger("snuba.consumer")


class KafkaMessageMetadata(NamedTuple):
    offset: int
    partition: int
    timestamp: datetime


class InvalidActionType(Exception):
    pass


class ConsumerWorker(AbstractBatchWorker[KafkaPayload, ProcessedMessage]):
    def __init__(
        self,
        storage: WritableTableStorage,
        metrics: MetricsBackend,
        producer: Optional[ConfluentKafkaProducer] = None,
        replacements_topic: Optional[Topic] = None,
    ) -> None:
        self.__storage = storage
        self.producer = producer
        self.replacements_topic = replacements_topic
        self.metrics = metrics
        table_writer = storage.get_table_writer()
        self.__writer = table_writer.get_writer(
            {"load_balancing": "in_order", "insert_distributed_sync": 1},
        )

        self.__pre_filter = table_writer.get_stream_loader().get_pre_filter()
        self.__processor = (
            self.__storage.get_table_writer().get_stream_loader().get_processor()
        )

    def process_message(
        self, message: Message[KafkaPayload]
    ) -> Optional[ProcessedMessage]:

        if self.__pre_filter and self.__pre_filter.should_drop(message):
            return None

        return self._process_message_impl(
            rapidjson.loads(message.payload.value),
            KafkaMessageMetadata(
                offset=message.offset,
                partition=message.partition.index,
                timestamp=message.timestamp,
            ),
        )

    def _process_message_impl(
        self, value: Mapping[str, Any], metadata: KafkaMessageMetadata,
    ) -> Optional[ProcessedMessage]:
        return self.__processor.process_message(value, metadata)

    def delivery_callback(self, error, message):
        if error is not None:
            # errors are KafkaError objects and inherit from BaseException
            raise error

    def flush_batch(self, batch: Sequence[ProcessedMessage]):
        """First write out all new INSERTs as a single batch, then reproduce any
        event replacements such as deletions, merges and unmerges."""
        inserts: MutableSequence[WriterTableRow] = []
        replacements: MutableSequence[ReplacementBatch] = []

        for item in batch:
            if isinstance(item, InsertBatch):
                inserts.extend(item.rows)
            elif isinstance(item, ReplacementBatch):
                replacements.append(item)
            else:
                raise TypeError(f"unexpected type: {item}")

        if inserts:
            self.__writer.write(inserts)

            self.metrics.timing("inserts", len(inserts))

        if replacements:
            for replacement in replacements:
                key = replacement.key.encode("utf-8")
                for value in replacement.values:
                    self.producer.produce(
                        self.replacements_topic.name,
                        key=key,
                        value=rapidjson.dumps(value).encode("utf-8"),
                        on_delivery=self.delivery_callback,
                    )

            self.producer.flush()
