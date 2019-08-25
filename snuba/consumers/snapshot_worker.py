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
        self.__skipped_batch = []
        self.__xip_list_applied = []
        logger.debug("Starting snapshot aware worker for id %s", self.__snapshot_id)

    def _process_message_impl(
        self,
        value: Mapping[str, Any],
        metadata: KafkaMessageMetadata,
    ):
        if self.__catching_up:
            xid = value.get("xid")
            if xid:
                if xid >= self.__transaction_data.xmax:
                    logger.info(
                        "Found xid(%s) >= xmax:(%s). Catch up phase is over",
                        xid,
                        self.__transaction_data.xmax,
                    )
                    logger.info(
                        "Skipped %d transactions. Applied %d from xip_list",
                        len(self.__skipped_batch),
                        len(self.__xip_list_applied),
                    )
                    self.__catching_up = False
                else:
                    if xid in self.__transaction_data.xip_list:
                        self.__xip_list_applied.append(xid)
                        if len(self.__xip_list_applied) % 100 == 0:
                            logger.info(
                                "Applied %d transacitons from xip_list",
                                len(self.__xip_list_applied),
                            )
                    else:
                        self.__skipped_batch.append(xid)
                        if len(self.__skipped_batch) % 100 == 0:
                            logger.info(
                                "Skipped %d transacitons",
                                len(self.__skipped_batch),
                            )
                        return

        return super(SnapshotAwareWorker, self)._process_message_impl(
            value,
            metadata,
        )
