import logging
from datetime import datetime
from typing import Any, Mapping, NamedTuple, Optional, Sequence

import simplejson as json
import rapidjson
from confluent_kafka import Producer as ConfluentKafkaProducer

from snuba.datasets.storage import WritableTableStorage
from snuba.processor import ProcessedMessage, ProcessorAction
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.streams.batching import AbstractBatchWorker
from snuba.utils.streams.kafka import KafkaPayload
from snuba.utils.streams.types import Message, Topic

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
        rapidjson_deserialize: bool = True,
        rapidjson_serialize: bool = True,
    ) -> None:
        self.__storage = storage
        self.producer = producer
        self.replacements_topic = replacements_topic
        self.metrics = metrics
        table_writer = storage.get_table_writer()
        self.__writer = table_writer.get_writer(
            {"load_balancing": "in_order", "insert_distributed_sync": 1},
            rapidjson_serialize=rapidjson_serialize,
        )

        self.__rapidjson_deserialize = rapidjson_deserialize
        self.__pre_filter = table_writer.get_stream_loader().get_pre_filter()

    def process_message(
        self, message: Message[KafkaPayload]
    ) -> Optional[ProcessedMessage]:

        if self.__pre_filter and self.__pre_filter.should_drop(message):
            return None

        if self.__rapidjson_deserialize:
            value = rapidjson.loads(message.payload.value)
        else:
            value = json.loads(message.payload.value)

        processed = self._process_message_impl(
            value,
            KafkaMessageMetadata(
                offset=message.offset,
                partition=message.partition.index,
                timestamp=message.timestamp,
            ),
        )

        if processed is None:
            return None

        if processed.action not in set(
            [ProcessorAction.INSERT, ProcessorAction.REPLACE]
        ):
            raise InvalidActionType("Invalid action type: {}".format(processed.action))

        return processed

    def _process_message_impl(
        self, value: Mapping[str, Any], metadata: KafkaMessageMetadata,
    ) -> Optional[ProcessedMessage]:
        processor = (
            self.__storage.get_table_writer().get_stream_loader().get_processor()
        )
        return processor.process_message(value, metadata)

    def delivery_callback(self, error, message):
        if error is not None:
            # errors are KafkaError objects and inherit from BaseException
            raise error

    def flush_batch(self, batch: Sequence[ProcessedMessage]):
        """First write out all new INSERTs as a single batch, then reproduce any
        event replacements such as deletions, merges and unmerges."""
        inserts = []
        replacements = []

        for message in batch:
            if message.action == ProcessorAction.INSERT:
                inserts.extend(message.data)
            elif message.action == ProcessorAction.REPLACE:
                replacements.extend(message.data)

        if inserts:
            self.__writer.write(inserts)

            self.metrics.timing("inserts", len(inserts))

        if replacements:
            for key, replacement in replacements:
                self.producer.produce(
                    self.replacements_topic.name,
                    key=str(key).encode("utf-8"),
                    value=json.dumps(replacement).encode("utf-8"),
                    on_delivery=self.delivery_callback,
                )

            self.producer.flush()
