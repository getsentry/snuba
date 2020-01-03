import collections
import logging
import simplejson as json

from typing import Any, Mapping, Optional, Sequence

from snuba.datasets.factory import enforce_table_writer
from snuba.processor import (
    ProcessedMessage,
    ProcessorAction,
)
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.streams.batching import AbstractBatchWorker
from snuba.utils.streams.kafka import KafkaPayload
from snuba.utils.streams.types import Message


logger = logging.getLogger("snuba.consumer")

KafkaMessageMetadata = collections.namedtuple(
    "KafkaMessageMetadata", "offset partition"
)


class InvalidActionType(Exception):
    pass


class ConsumerWorker(AbstractBatchWorker[KafkaPayload, ProcessedMessage]):
    def __init__(self, dataset, producer, replacements_topic, metrics: MetricsBackend):
        self.__dataset = dataset
        self.producer = producer
        self.replacements_topic = replacements_topic
        self.metrics = metrics
        self.__writer = enforce_table_writer(dataset).get_writer(
            {"load_balancing": "in_order", "insert_distributed_sync": 1}
        )

    def process_message(
        self, message: Message[KafkaPayload]
    ) -> Optional[ProcessedMessage]:
        # TODO: consider moving this inside the processor so we can do a quick
        # processing of messages we want to filter out without fully parsing the
        # json.
        value = json.loads(message.payload.value)
        metadata = KafkaMessageMetadata(
            offset=message.offset, partition=message.partition.index
        )
        processed = self._process_message_impl(value, metadata)
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
            enforce_table_writer(self.__dataset).get_stream_loader().get_processor()
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
                    self.replacements_topic,
                    key=str(key).encode("utf-8"),
                    value=json.dumps(replacement).encode("utf-8"),
                    on_delivery=self.delivery_callback,
                )

            self.producer.flush()
