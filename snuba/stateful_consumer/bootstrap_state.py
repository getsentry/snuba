import logging

from typing import Any, Sequence, Tuple
from confluent_kafka import Consumer, Message, TopicPartition

from snuba.stateful_consumer import StateOutput
from snuba.stateful_consumer.state_context import State
from snuba.consumers.strict_consumer import CommitDecision, StrictConsumer
from snuba import settings

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
        topic: str,
        bootstrap_servers: Sequence[str],
        group_id: str,
    ):
        super(BootstrapState, self).__init__()

        def on_partitions_assigned(
            consumer: Consumer,
            partitions: Sequence[TopicPartition],
        ):
            pass

        def on_partitions_revoked(
            consumer: Consumer,
            partitions: Sequence[TopicPartition],
        ):
            pass

        self.__consumer = StrictConsumer(
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            partition_assignment_timeout=settings.SNAPSHOT_CONTROL_TOPIC_INIT_TIMEOUT,
            on_partitions_assigned=on_partitions_assigned,
            on_partitions_revoked=on_partitions_revoked,
            on_message=self.__handle_msg,
        )

    def __handle_msg(self, message: Message) -> CommitDecision:
        logger.info(
            "MSG %r %r %r",
            message,
            message.value(),
            message.error(),
        )
        # TODO: Actually do something with the messages and drive the
        # state machine to the next state.
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

        return CommitDecision.DO_NOT_COMMIT

    def handle(self, input: Any) -> Tuple[StateOutput, Any]:
        output = StateOutput.NO_SNAPSHOT

        logger.info("Runnign Consumer")
        self.__consumer.run()

        logger.info("Caught up on the control topic")
        return (output, None)

    def set_shutdown(self) -> None:
        super(BootstrapState, self).set_shutdown()
        self.__consumer.shutdown()
