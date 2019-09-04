import json
import logging

from typing import Sequence, Tuple
from confluent_kafka import Message, Optional

from snuba import settings
from snuba.consumers.strict_consumer import CommitDecision, StrictConsumer
from snuba.stateful_consumer import ConsumerStateData, ConsumerStateCompletionEvent
from snuba.utils.state_machine import State
from snuba.stateful_consumer.control_protocol import (
    parse_control_message,
    SnapshotInit,
    SnapshotAbort,
    SnapshotLoaded,
    ControlMessage,
)

logger = logging.getLogger('snuba.snapshot-load')


class RecoveryState:
    """
    Contains the logic that decides what to do for every message read on the
    control topic.
    It knows which messages should be taken into account and which ones should
    be discarded. Knowing this, it is able to tell what to commit.
    """

    def __init__(self):
        self.__active_snapshot_msg = None
        self.__processed_snapshots = set()
        self.__completion_event = ConsumerStateCompletionEvent.NO_SNAPSHOT

    def get_completion_event(self) -> ConsumerStateCompletionEvent:
        return self.__completion_event

    def get_active_snapshot_msg(self) -> Optional[ControlMessage]:
        return self.__active_snapshot_msg

    def process_init(self, msg: SnapshotInit) -> None:
        logger.debug("Processing init message for %r", msg.id)
        if msg.product != settings.SNAPSHOT_LOAD_PRODUCT:
            return
        if self.__active_snapshot_msg:
            if isinstance(self.__active_snapshot_msg, SnapshotInit):
                logger.error(
                    "Overlapping snapshots. Ignoring. Running %r. Init received %r.",
                    msg.id,
                    self.__active_snapshot_msg.id,
                )
                return

        if msg.id in self.__processed_snapshots:
            logger.warning(
                "Duplicate Snapshot init: %r",
                msg.id,
            )
        self.__processed_snapshots.add(msg.id)
        self.__active_snapshot_msg = msg
        self.__completion_event = ConsumerStateCompletionEvent.SNAPSHOT_INIT_RECEIVED

    def process_abort(self, msg: SnapshotAbort) -> None:
        logger.debug("Processing abort message for %r", msg.id)
        if msg.id not in self.__processed_snapshots:
            return
        if self.__active_snapshot_msg.id != msg.id:
            logger.warning(
                "Aborting a snapshot that is not active. Active %r, Abort %r",
                self.__active_snapshot_msg.id,
                msg.id,
            )
            return
        self.__active_snapshot_msg = None
        self.__completion_event = ConsumerStateCompletionEvent.NO_SNAPSHOT

    def process_snapshot_loaded(self, msg: SnapshotLoaded) -> None:
        logger.debug("Processing ready message for %r", msg.id)
        if msg.id not in self.__processed_snapshots:
            return
        if self.__active_snapshot_msg.id != msg.id:
            logger.warning(
                "Loaded a snapshot that is not active. Active %r, Abort %r",
                self.__active_snapshot_msg.id,
                msg.id,
            )
            return
        self.__active_snapshot_msg = msg
        self.__completion_event = ConsumerStateCompletionEvent.SNAPSHOT_READY_RECEIVED


class BootstrapState(State[ConsumerStateCompletionEvent, ConsumerStateData]):
    """
    This is the state the consumer starts into.
    Its job is to either transition to normal operation or to recover a
    previously running snapshot if the conumer was restarted while the
    process was on going.
    The recovery process is done by consuming the whole control topic.
    """

    def __init__(self,
        topic: str,
        bootstrap_servers: Sequence[str],
        group_id: str,
    ):
        self.__consumer = StrictConsumer(
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            initial_auto_offset_reset="earliest",
            partition_assignment_timeout=settings.SNAPSHOT_CONTROL_TOPIC_INIT_TIMEOUT,
            on_partitions_assigned=None,
            on_partitions_revoked=None,
            on_message=self.__handle_msg,
        )

        self.__recovery_state = RecoveryState()

    def __handle_msg(self, message: Message) -> CommitDecision:
        value = json.loads(message.value())
        parsed_message = parse_control_message(value)

        current_snapshot = self.__recovery_state.get_active_snapshot_msg()
        if isinstance(parsed_message, SnapshotInit):
            self.__recovery_state.process_init(parsed_message)
        elif isinstance(parsed_message, SnapshotAbort):
            self.__recovery_state.process_abort(parsed_message)
        elif isinstance(parsed_message, SnapshotLoaded):
            self.__recovery_state.process_snapshot_loaded(
                parsed_message,
            )

        new_snap = self.__recovery_state.get_active_snapshot_msg()
        if new_snap is None:
            logger.debug("Committing offset %r ", message.offset())
            return CommitDecision.COMMIT_THIS
        elif current_snapshot is None or new_snap.id != current_snapshot.id:
            logger.debug("Committing previous offset to %r ", message.offset())
            return CommitDecision.COMMIT_PREV
        else:
            logger.debug("Not committing")
            return CommitDecision.DO_NOT_COMMIT

    def signal_shutdown(self) -> None:
        self.__consumer.signal_shutdown()

    def handle(self, state_data: ConsumerStateData) -> Tuple[ConsumerStateCompletionEvent, ConsumerStateData]:
        logger.info("Running %r", self.__consumer)
        self.__consumer.run()

        logger.info("Caught up on the control topic")
        return (
            self.__recovery_state.get_completion_event(),
            ConsumerStateData.no_snapshot_state(),
        )
