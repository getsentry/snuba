from snuba.stateful_consumer import StateOutput
from snuba.stateful_consumer.state_context import State
from snuba.consumers.simple_consumer import create_simple_consumer
from snuba.stateful_consumer.control_protocol import (
    parse_control_message,
    SnapshotInit,
    SnapshotLoaded,
    SnapshotAbort
)
from snuba import settings

import simplejson as json
import logging
from typing import Any, Sequence, Tuple
from confluent_kafka import TopicPartition

logger = logging.getLogger('snuba.snapshot-load')


class FoundSnapshots:
    def __init__(self):
        self.__snapshots = {}
        self.__current_open_snapshot = None

    def open_snapshot(self, snapshot_id: str) -> None:
        if snapshot_id in self.__snapshots:
            logger.error(
                "Found more than one snapshot init for snapshot %s. IGNORING!",
                snapshot_id,
            )

        if self.__current_open_snapshot:
            logger.error(
                "Overlapping snapshots found on the control topic. IGNORING!"
            )
            return
        self.__snapshots[snapshot_id] = True
        self.__current_open_snapshot = snapshot_id

    def close_snapshot(self, snapshot_id: str) -> None:
        self.__current_open_snapshot = None
        self.__snapshots[snapshot_id] = False

    def is_snapshot_managed(self, snapshot_id: str) -> bool:
        return snapshot_id in self.__snapshots

    def is_snapshot_active(self) -> bool:
        return self.__current_open_snapshot is not None


class BootstrapState(State[StateOutput]):
    """
    This is the state the consumer starts into.
    Its job is to either transition to normal operation or
    to recover a previously running snapshot if the conumer
    was restarted while the process was on going.
    The recovery process is done by consuming the whole
    control topic.
    """

    def __init__(self,
        topics: str,
        bootstrap_servers: Sequence[str],
        group_id: str,
    ):
        self.__consumer = create_simple_consumer(
            topics,
            bootstrap_servers,
            group_id,
            auto_offset_reset="earliest"
        )

    def handle(self, input: Any) -> Tuple[StateOutput, Any]:
        expected_product = settings.SNAPSHOT_LOAD_PRODUCT
        output = StateOutput.NO_SNAPSHOT
        current_snapshots = FoundSnapshots()

        partitions = self.__consumer.assignment()
        assert len(partitions) == 1, \
            "CDC can support one partition only"
        # TODO: properly manage assignment and revoke. This is not robust right
        # now. It is not guaranteed that by the time we are here we will have a
        # partition.
        watermark = partitions[0].offset

        message = self.__consumer.consume(1)
        while message:
            value = json.loads(message.value())
            parsed_message = parse_control_message(value)

            if isinstance(parsed_message, SnapshotInit):
                if parsed_message.product != expected_product:
                    if not current_snapshots.is_snapshot_active():
                        watermark = message.offset()
                else:
                    current_snapshots.open_snapshot(parsed_message.id)
                    output = StateOutput.SNAPSHOT_INIT_RECEIVED
            elif isinstance(parsed_message, SnapshotAbort):
                if not current_snapshots.is_snapshot_managed(parsed_message.id):
                    if not current_snapshots.is_snapshot_active():
                        watermark = message.offset()
                else:
                    current_snapshots.close_snapshot(parsed_message.id)
                    if not current_snapshots.is_snapshot_active():
                        watermark = message.offset()
                        output = StateOutput.NO_SNAPSHOT
            elif isinstance(parsed_message, SnapshotLoaded):
                if not current_snapshots.is_snapshot_managed(parsed_message.id):
                    if not current_snapshots.is_snapshot_active():
                        watermark = message.offset()
                else:
                    current_snapshots.close_snapshot(parsed_message.id)
                    if not current_snapshots.is_snapshot_active():
                        output = StateOutput.SNAPSHOT_LOAD_PRODUCT

            self.__consumer.commit(TopicPartition(
                partitions[0].topic,
                partitions[0].partition,
                watermark,
            ))
            message = self.__consumer.consume(1)

        return (output, None)
