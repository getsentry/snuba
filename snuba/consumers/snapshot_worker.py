import logging

from confluent_kafka import Producer
from typing import Any, Optional, Mapping

from snuba.consumer import ConsumerWorker, KafkaMessageMetadata
from snuba.stateful_consumer.control_protocol import TransactionData
from snuba.datasets import Dataset
from snuba.snapshots import SnapshotId

logger = logging.getLogger('snuba.snapshot-consumer')


class SnapshotAwareWorker(ConsumerWorker):
    def __init__(self,
        dataset: Dataset,
        producer: Producer,
        snapshot_id: SnapshotId,
        transaction_data: TransactionData,
        replacements_topic: Optional[str],
        metrics: Optional[Any] = None,
    ) -> None:
        super(SnapshotAwareWorker, self).__init__(
            dataset=dataset,
            producer=producer,
            replacements_topic=replacements_topic,
            metrics=metrics,
        )
        self.__snapshot_id = snapshot_id
        self.__transaction_data = transaction_data
        self.__catching_up = True
        logger.debug("Starting snapshot aware worker for id %s", self.__snapshot_id)

    def _process_message_impl(
        self,
        value: Mapping[str, Any],
        metadata: KafkaMessageMetadata,
    ):
        logger.debug("Catching up %s", self.__catching_up)
        return super(SnapshotAwareWorker, self)._process_message_impl(
            value,
            metadata,
        )
