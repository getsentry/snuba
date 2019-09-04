import json
import logging

from typing import Optional, Sequence, Tuple
from confluent_kafka import Message

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
    When the stateful consumer starts, it consumes the entire content of the control
    topic. This is supposed to tell the consumer what to do next.
    There are a few possible states the control topic can be into:
    - empty: do nothing and start consuming the main topic
    - messages related to a snapshot that this consumer does not care for: ignore them
      and commit them.
    - a snapshot-init message the consumer is interested into is on top of the topic.
      In this case the consumer is suppsoed to pause till there is a snapshot-loaded message
      or a snapshot-abort message. This message should not be committed until we do not
      proceed with the next state to persist the current state.
    - a snapshot-loaded message (related with a snapshot-init) message is on the topic.
      This means a snapshot has been successfully uploaded and the consumer needs to interpret
      the transaciton data, catch up, and then start consuming normally.

    This class implements all the logic above by consuming all the messaages present
    on the control topic, and provide the next state the state machine should transition
    to and the offset to commit.

    Corner cases:
    - Overlapping snapshots: which means two subsequent snapshot-init messages. In this
      case the first one wins. No other init message will be taken into account until we
      do not observe a completion message for the current snapshot (abort or loaded).
    - Multiple snapshots present on the control topic in sequence. Here the last one wins
      and the previous ones are ignored so we do not need to know whether the previous ones
      were succesfully uploaded or not.
    """

    def __init__(self):
        # This represents the valid snapshot we are tracking. Using the message
        # itself is convenient because it already has all the information we need.
        # For each message this may change if a new snapshot is found on the control
        # topic.
        self.__active_snapshot_msg = None
        # Past processed snapshots. This is meant to tie abort and loaded messages
        # to an init message and filter out snapshot messages we are not interested
        # into.
        self.__past_snapshots = set()
        # Final state once all the messages have been processed.
        self.__completion_event = ConsumerStateCompletionEvent.NO_SNAPSHOT

    def get_completion_event(self) -> ConsumerStateCompletionEvent:
        return self.__completion_event

    def get_active_snapshot_msg(self) -> Optional[ControlMessage]:
        return self.__active_snapshot_msg

    def process_init(self, msg: SnapshotInit) -> None:
        """
        Processes a snapshot-init message found on the control topic.
        This may ignore the message and commit it if it is not for this consumer
        or move the state machine to a paused state.
        """
        logger.debug("Processing init message for %r", msg.id)
        if msg.product != settings.SNAPSHOT_LOAD_PRODUCT:
            return
        if self.__active_snapshot_msg:
            if isinstance(self.__active_snapshot_msg, SnapshotInit):
                logger.error(
                    "A new snapshot-init message was found before a previous one "
                    " was completed. Ignoring. Running %r. Init received %r.",
                    msg.id,
                    self.__active_snapshot_msg.id,
                )
                return

        if msg.id in self.__past_snapshots:
            logger.warning(
                "Duplicate Snapshot init: %r",
                msg.id,
            )
        self.__past_snapshots.add(msg.id)
        self.__active_snapshot_msg = msg
        self.__completion_event = ConsumerStateCompletionEvent.SNAPSHOT_INIT_RECEIVED

    def process_abort(self, msg: SnapshotAbort) -> None:
        """
        Processes an abort message found on the control topic. If this comes after
        a snapshot-init message, it makes the state machine drop that snapshot.
        """
        logger.debug("Processing abort message for %r", msg.id)
        if msg.id not in self.__past_snapshots:
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
        """
        Processes a snapshot-loaded message on the control topic. If there was
        a valid snapshot-init before, this transitions to catching up state, unless
        we find a newer snapshot on the control topic after this one.
        """
        logger.debug("Processing ready message for %r", msg.id)
        if msg.id not in self.__past_snapshots:
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
            """
            Commit the last offset found if there is no snapshot currently requiring
            the consumer's attention. So all the content of the topic is not interesting.
            """
            logger.debug("Committing offset %r", message.offset())
            return CommitDecision.COMMIT_THIS
        elif current_snapshot is None or new_snap.id != current_snapshot.id:
            """
            Commit the previous messages but leaves the last one on the topic. This
            is useful if there are many non interesting messages but there is something
            to keep on the topic in case of restart.
            """
            logger.debug("Committing previous offset to %r", message.offset())
            return CommitDecision.COMMIT_PREV
        else:
            """
            Do not commit if the messages read need to be kept on the topic in case the
            consumer restarts. Like a snapshot-init.
            """
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
