import json
import logging

from typing import Optional, Set, Tuple
from confluent_kafka import Message

from snuba import settings
from snuba.consumers.strict_consumer import CommitDecision, StrictConsumer
from snuba.datasets.cdc import CdcStorage
from snuba.stateful_consumer import ConsumerStateData, ConsumerStateCompletionEvent
from snuba.snapshots import SnapshotId
from snuba.stateful_consumer.control_protocol import (
    parse_control_message,
    SnapshotInit,
    SnapshotAbort,
    SnapshotLoaded,
    TransactionData,
)
from snuba.utils.state_machine import State
from snuba.utils.streams.backends.kafka import KafkaBrokerConfig

logger = logging.getLogger("snuba.snapshot-load")


class RecoveryState:
    """
    When the stateful consumer starts, it consumes the entire content of the control
    topic. This is supposed to tell the consumer what to do next.
    There are a few possible states the control topic can be into:
    - empty: do nothing and start consuming the main topic
    - messages related to a snapshot that this consumer does not care for: ignore
      and commit them.
    - a snapshot-init message the consumer is interested into is the last message of interest
      on the topic.
      In this case the consumer is supposed to pause till there is a snapshot-loaded message
      or a snapshot-abort message. This message should not be committed until the snapshot
      has been loaded and the consumer has caught up.
    - a snapshot-loaded message (related with a snapshot-init) message is on the topic.
      This means a snapshot has been successfully uploaded and the consumer needs to interpret
      the transaction data, catch up, and then start consuming normally.

    This class implements all the logic above by consuming all the messages present
    on the control topic, keeping track of the snapshot we expect to be the one the consumer
    is interested into and producing the ConsumerStateCompletionEvent the state should
    return.

    Corner cases:
    - Overlapping snapshots: which means two subsequent snapshot-init messages. In this
      case the first one wins. No other init message will be taken into account until we
      do not observe a completion message for the current snapshot (abort or loaded).
    - Multiple completed snapshots present on the control topic in sequence. Here the last one wins
      and the previous ones are ignored so we do not need to know whether the previous ones
      were succesfully uploaded or not.

    test_recovery_state shows some examples.
    """

    def __init__(self, postgres_table_name: str) -> None:
        # This represents the snapshot id we are currently tracking. It is set after
        # snapshot-init and reset by snapshot-abort or after the current snapshot is
        # loaded and a new init message is found.
        self.__active_snapshot_id: Optional[SnapshotId] = None
        # This tracks the transaction metadata for the currently loaded snapshot.
        # it is set after a snapshot-load message.
        self.__loaded_snapshot_transactions: Optional[TransactionData] = None
        # Past processed snapshots. This is meant to tie abort and loaded messages
        # to an init message and filter out snapshot messages we are not interested
        # into.
        self.__past_snapshots: Set[SnapshotId] = set()
        # The postgres table present in the snapshot this state is interested into.
        # The control topic may include snapshots regarding different postgres table.
        # The bootstrap state needs to filter out those it is not interested into.
        self.__postgres_table_name = postgres_table_name

    def get_completion_event(self) -> ConsumerStateCompletionEvent:
        """
        Returns the Completion State that drives the next state transition according
        to the list of consumed messages.
        """
        if not self.__active_snapshot_id:
            return ConsumerStateCompletionEvent.NO_SNAPSHOT
        elif not self.__loaded_snapshot_transactions:
            """
            This means we received an init message but we are still waiting for
            the snapshot loaded message.
            """
            return ConsumerStateCompletionEvent.SNAPSHOT_INIT_RECEIVED
        else:
            return ConsumerStateCompletionEvent.SNAPSHOT_READY_RECEIVED

    def get_active_snapshot(
        self,
    ) -> Optional[Tuple[SnapshotId, Optional[TransactionData]]]:
        if self.__active_snapshot_id:
            return (self.__active_snapshot_id, self.__loaded_snapshot_transactions)
        else:
            return None

    def __commit_if_no_active_snapshot(self) -> CommitDecision:
        if not self.__active_snapshot_id:
            return CommitDecision.COMMIT_THIS
        else:
            return CommitDecision.DO_NOT_COMMIT

    def process_init(self, msg: SnapshotInit) -> CommitDecision:
        """
        Processes a snapshot-init message found on the control topic.
        If the snapshot-init message is for this consumer and we are not already
        waiting on an existing snapshot this sets the active snapshot and does not
        commit the message for failover.
        """
        logger.debug("Processing init message for %r", msg.id)
        if (
            msg.product != settings.SNAPSHOT_LOAD_PRODUCT
            or self.__postgres_table_name not in msg.tables
        ):
            # Not a snapshot this consumer is interested into.
            return self.__commit_if_no_active_snapshot()

        if msg.id in self.__past_snapshots:
            logger.warning(
                "Duplicate Snapshot init: %r", msg.id,
            )
            return self.__commit_if_no_active_snapshot()

        if self.__active_snapshot_id and not self.__loaded_snapshot_transactions:
            logger.warning(
                "A new snapshot-init message was found before a previous one "
                "was completed. Ignoring %r. Continuing with %r.",
                msg.id,
                self.__active_snapshot_id,
            )
            return CommitDecision.DO_NOT_COMMIT

        self.__past_snapshots.add(msg.id)
        self.__active_snapshot_id = msg.id
        self.__loaded_snapshot_transactions = None
        return CommitDecision.COMMIT_PREV

    def process_abort(self, msg: SnapshotAbort) -> CommitDecision:
        """
        Processes an abort message found on the control topic. If this comes after
        a snapshot-init message, it makes the state machine drop that snapshot.
        """
        logger.debug("Processing abort message for %r", msg.id)
        if msg.id not in self.__past_snapshots:
            return self.__commit_if_no_active_snapshot()
        if self.__active_snapshot_id != msg.id:
            logger.warning(
                "Received an abort message for a different snapshot than "
                "the one I am expecting. Ignoring %r. Continuing with %r",
                msg.id,
                self.__active_snapshot_id,
            )
            return CommitDecision.DO_NOT_COMMIT
        self.__active_snapshot_id = None
        self.__loaded_snapshot_transactions = None
        return CommitDecision.COMMIT_THIS

    def process_snapshot_loaded(self, msg: SnapshotLoaded) -> CommitDecision:
        """
        Processes a snapshot-loaded message on the control topic. If there was
        a valid snapshot-init before, this transitions to catching up state, unless
        we find a newer snapshot on the control topic after this one.
        """
        logger.debug("Processing ready message for %r", msg.id)
        if msg.id not in self.__past_snapshots:
            return self.__commit_if_no_active_snapshot()
        if self.__active_snapshot_id != msg.id:
            logger.warning(
                "Received a snapshot-loaded event for a different snapshot than "
                "the one I am expecting. Ignoring %r. Continuing with %r",
                msg.id,
                self.__active_snapshot_id,
            )
            return CommitDecision.DO_NOT_COMMIT
        self.__loaded_snapshot_transactions = msg.transaction_info
        return CommitDecision.DO_NOT_COMMIT


class BootstrapState(State[ConsumerStateCompletionEvent, Optional[ConsumerStateData]]):
    """
    This is the state the consumer starts into.
    Its job is to either transition to normal operation or to recover a
    previously running snapshot if the conumer was restarted while the
    process was on going.
    The recovery process is done by consuming the whole control topic.
    """

    def __init__(
        self,
        topic: str,
        broker_config: KafkaBrokerConfig,
        group_id: str,
        storage: CdcStorage,
    ):
        self.__consumer = StrictConsumer(
            topic=topic,
            broker_config=broker_config,
            group_id=group_id,
            initial_auto_offset_reset="earliest",
            partition_assignment_timeout=settings.SNAPSHOT_CONTROL_TOPIC_INIT_TIMEOUT,
            on_message=self.__handle_msg,
        )

        self.__recovery_state = RecoveryState(storage.get_postgres_table())

    def __handle_msg(self, message: Message) -> CommitDecision:
        value = json.loads(message.value())
        parsed_message = parse_control_message(value)

        if isinstance(parsed_message, SnapshotInit):
            commit_decision = self.__recovery_state.process_init(parsed_message)
        elif isinstance(parsed_message, SnapshotAbort):
            commit_decision = self.__recovery_state.process_abort(parsed_message)
        elif isinstance(parsed_message, SnapshotLoaded):
            commit_decision = self.__recovery_state.process_snapshot_loaded(
                parsed_message,
            )
        else:
            logger.warning("Received an unrecognized message: %r", parsed_message)
            commit_decision = CommitDecision.DO_NOT_COMMIT

        return commit_decision

    def signal_shutdown(self) -> None:
        self.__consumer.signal_shutdown()

    def handle(
        self, state_data: Optional[ConsumerStateData],
    ) -> Tuple[ConsumerStateCompletionEvent, Optional[ConsumerStateData]]:
        logger.info("Running %r", self.__consumer)
        self.__consumer.run()

        snapshot = self.__recovery_state.get_active_snapshot()
        if snapshot and snapshot[1] is not None:
            state_data = ConsumerStateData.snapshot_ready_state(
                snapshot_id=snapshot[0], transaction_data=snapshot[1],
            )
        else:
            state_data = None

        logger.info("Caught up on the control topic")
        return (
            self.__recovery_state.get_completion_event(),
            state_data,
        )
