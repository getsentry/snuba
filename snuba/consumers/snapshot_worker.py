import logging
from typing import Any, Mapping, Optional, Set

from confluent_kafka import Producer

from snuba.consumer import ConsumerWorker, KafkaMessageMetadata
from snuba.datasets.storage import WritableTableStorage
from snuba.processor import MessageProcessor, ProcessedMessage
from snuba.snapshots import SnapshotId
from snuba.stateful_consumer.control_protocol import TransactionData
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.streams import Topic

logger = logging.getLogger("snuba.snapshot-consumer")


CDCEvent = Mapping[str, Any]  # TODO: Replace with ``TypedDict``


class SnapshotProcessor(MessageProcessor):
    """
    Wraps a ``MessageProcessor`` instance, filtering out messages that have
    already been loaded into the destination dataset from an external
    snapshot.

    When we import a snapshot from Postgres into ClickHouse, we should apply
    only the transactions that we find in Kafka that are not part of the
    snapshot (which means that they were produced after the snapshot was
    taken).

    Failing to do so is not a critical issue if the destination table is a
    merging table, in that eventually consistency will be reestablished.
    Still the table would be in an inconsistent state (phantom reads, non
    monotonic reads) for the entire time the consumer takes to catch up to
    the snapshot.

    This consumer receives the coordinates of a snapshot through the
    constructor thus it is able to discard transactions that were already
    part of the snapshot.

    As such, this wrapper is only necessary while there is an overlap between
    the data contained within the source stream and the external snapshot.
    Once the consumer is "caught up" (the monotonic transaction ID from the
    source stream exceeds that of the snapshot maximum transaction ID), this
    filtering is no longer required and the stream can be safely consumed
    from without using this wrapper.
    """

    def __init__(
        self,
        processor: MessageProcessor,
        snapshot_id: SnapshotId,
        transaction_data: TransactionData,
    ) -> None:
        self.__processor = processor
        self.__snapshot_id = snapshot_id
        self.__transaction_data = transaction_data

        self.__catching_up = True
        self.__skipped: Set[int] = set()
        self.__xip_list_applied: Set[int] = set()
        logger.debug("Starting snapshot aware worker for id %s", self.__snapshot_id)

    def __accept_message(
        self, xid: int, value: CDCEvent, metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        if self.__catching_up and xid and xid >= self.__transaction_data.xmax:
            logger.info(
                "Found xid(%s) >= xmax:(%s). Catch up phase is over",
                xid,
                self.__transaction_data.xmax,
            )
            logger.info(
                "Skipped %d transactions. Applied %d from xip_list",
                len(self.__skipped),
                len(self.__xip_list_applied),
            )
            self.__skipped.clear()
            self.__xip_list_applied.clear()
            self.__catching_up = False

        return self.__processor.process_message(value, metadata)

    def __drop_message(self, xid: int) -> Optional[ProcessedMessage]:
        current_len = len(self.__skipped)
        self.__skipped.add(xid)
        new_len = len(self.__skipped)
        if new_len != current_len and new_len % 100 == 0:
            logger.info(
                "Skipped %d transactions", len(self.__skipped),
            )
        return None

    def process_message(
        self, message: CDCEvent, metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        """
        This delegates the processing of all events that were committed after
        a snapshot was taken to the wrapped processor and filters (discards)
        those that were contained within the snapshot (and thus presumed to
        be already loaded into the dataset.)

        It is based on the postgres snapshot descriptor which is composed of:
         - xmin: The earliest transaction still active when the snapshot was taken
                 thus not loaded as part of the snapshot.
         - xmax: The first transaction id that was not assigned yet at the time of the
                 snapshot.
         - xip_list: The list of transactions that were active at the time the snapshot
                     was taken, thus that have NOT been loaded yet.

        The process is based on these rules:
        - if the processor observes xmax (or a following transaction) it means we are caught
          up. Unless a transaction is duplicated on the WAL, we cannot see anymore transactions
          that was already committed at the time the snapshot was taken. By definition all
          committed transactions that are part of the snapshot will show up on the WAL
          before xmax.
        - if the processor observes a transaction lower than xmax, it has to apply that if
          it is part of xip_list. If the transaction is not part of xip_list it means it
          was already committed at the time the snapshot was taken thus it is already
          part of the snapshot.
        - After seeing xmax, all transactions have to be applied since they are either
          higher of xmax or part of xip_list.
        """
        xid = message.get("xid")

        if xid is not None:
            if self.__catching_up:
                if xid < self.__transaction_data.xmin - 2 ** 32:
                    # xid is the 32 bit integer transaction id. This means it can wrap around
                    # During normal operation this is not an issue, but if that happens while
                    # catching up after a snapshot, it would be a cataclysm since we would
                    # skip all transactions for almost 64 bits worth of transactions.
                    # Better raising this issue, so the user can stop the process and take a
                    # new snapshot.
                    logger.error(
                        "xid (%d) much lower than xmin (%d)!! Check that xid did not wrap around while paused.",
                        xid,
                        self.__transaction_data.xmin,
                    )
                if xid < self.__transaction_data.xmax:
                    if xid in self.__transaction_data.xip_list:
                        self.__xip_list_applied.add(xid)
                    else:
                        return self.__drop_message(xid)

        return self.__accept_message(xid, message, metadata)


class SnapshotAwareWorker(ConsumerWorker):
    def __init__(
        self,
        storage: WritableTableStorage,
        producer: Producer,
        snapshot_id: SnapshotId,
        transaction_data: TransactionData,
        metrics: MetricsBackend,
        replacements_topic: Optional[Topic] = None,
    ) -> None:
        super().__init__(
            storage=storage,
            producer=producer,
            replacements_topic=replacements_topic,
            metrics=metrics,
        )
        self.__snapshot_id = snapshot_id
        self.__transaction_data = transaction_data

    def get_processor(self) -> MessageProcessor:
        try:
            return self.__processor
        except AttributeError:
            self.__processor = SnapshotProcessor(
                super().get_processor(), self.__snapshot_id, self.__transaction_data
            )
            return self.__processor
