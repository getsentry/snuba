from typing import Sequence, Tuple

import pytest

from snuba.consumers.strict_consumer import CommitDecision
from snuba.snapshots import SnapshotId
from snuba.snapshots.postgres_snapshot import Xid
from snuba.stateful_consumer import ConsumerStateCompletionEvent
from snuba.stateful_consumer.control_protocol import (
    ControlMessage,
    SnapshotAbort,
    SnapshotInit,
    SnapshotLoaded,
    TransactionData,
)
from snuba.stateful_consumer.states.bootstrap import RecoveryState


class TestRecoveryState:
    transaction_data = TransactionData(xmin=Xid(1), xmax=Xid(2), xip_list=[])
    snapshot_id = SnapshotId("123asd")
    test_data = [
        (
            # Empty topic.
            [],
            ConsumerStateCompletionEvent.NO_SNAPSHOT,
            None,
        ),
        (
            # One snapshot started for a table I am not interested into
            [
                (
                    SnapshotInit(
                        id=snapshot_id, product="snuba", tables=["some_table"]
                    ),
                    CommitDecision.COMMIT_THIS,
                )
            ],
            ConsumerStateCompletionEvent.NO_SNAPSHOT,
            None,
        ),
        (
            # One snapshot started
            [
                (
                    SnapshotInit(
                        id=snapshot_id,
                        product="snuba",
                        tables=["sentry_groupedmessage"],
                    ),
                    CommitDecision.COMMIT_PREV,
                )
            ],
            ConsumerStateCompletionEvent.SNAPSHOT_INIT_RECEIVED,
            "123asd",
        ),
        (
            # initialized and aborted snapshot
            [
                (
                    SnapshotInit(
                        id=snapshot_id,
                        product="snuba",
                        tables=["sentry_groupedmessage"],
                    ),
                    CommitDecision.COMMIT_PREV,
                ),
                (SnapshotAbort(id=snapshot_id), CommitDecision.COMMIT_THIS),
            ],
            ConsumerStateCompletionEvent.NO_SNAPSHOT,
            None,
        ),
        (
            # Initialized and ready
            [
                (
                    SnapshotInit(
                        id=snapshot_id,
                        product="snuba",
                        tables=["sentry_groupedmessage"],
                    ),
                    CommitDecision.COMMIT_PREV,
                ),
                (
                    SnapshotLoaded(id=snapshot_id, transaction_info=transaction_data),
                    CommitDecision.DO_NOT_COMMIT,
                ),
            ],
            ConsumerStateCompletionEvent.SNAPSHOT_READY_RECEIVED,
            "123asd",
        ),
        (
            # Initialized and multiple overlapping snapshots that are ignored
            [
                (
                    SnapshotInit(
                        id=snapshot_id,
                        product="snuba",
                        tables=["sentry_groupedmessage"],
                    ),
                    CommitDecision.COMMIT_PREV,
                ),
                (
                    SnapshotInit(
                        id=SnapshotId("234asd"),
                        product="someoneelse",
                        tables=["sentry_groupedmessage"],
                    ),
                    CommitDecision.DO_NOT_COMMIT,
                ),
                (SnapshotAbort(id=SnapshotId("234asd")), CommitDecision.DO_NOT_COMMIT),
                (
                    SnapshotInit(
                        id=SnapshotId("345asd"),
                        product="snuba",
                        tables=["sentry_groupedmessage"],
                    ),
                    CommitDecision.DO_NOT_COMMIT,
                ),
            ],
            ConsumerStateCompletionEvent.SNAPSHOT_INIT_RECEIVED,
            "123asd",
        ),
        (
            # Multiple successful consecutive snapshots
            [
                (
                    SnapshotInit(
                        id=snapshot_id,
                        product="snuba",
                        tables=["sentry_groupedmessage"],
                    ),
                    CommitDecision.COMMIT_PREV,
                ),
                (
                    SnapshotLoaded(id=snapshot_id, transaction_info=transaction_data,),
                    CommitDecision.DO_NOT_COMMIT,
                ),
                (
                    SnapshotInit(
                        id=SnapshotId("234asd"),
                        product="snuba",
                        tables=["sentry_groupedmessage"],
                    ),
                    CommitDecision.COMMIT_PREV,
                ),
                (
                    SnapshotLoaded(
                        id=SnapshotId("234asd"), transaction_info=transaction_data,
                    ),
                    CommitDecision.DO_NOT_COMMIT,
                ),
                (
                    SnapshotInit(
                        id=SnapshotId("345asd"),
                        product="snuba",
                        tables=["sentry_groupedmessage"],
                    ),
                    CommitDecision.COMMIT_PREV,
                ),
            ],
            ConsumerStateCompletionEvent.SNAPSHOT_INIT_RECEIVED,
            "345asd",
        ),
    ]

    @pytest.mark.parametrize("events, outcome, expected_id", test_data)
    def test_recovery(
        self,
        events: Sequence[Tuple[ControlMessage, CommitDecision]],
        outcome: ConsumerStateCompletionEvent,
        expected_id: str,
    ) -> None:
        recovery = RecoveryState("sentry_groupedmessage")
        for message, expected_commit_decision in events:
            if isinstance(message, SnapshotInit):
                decision = recovery.process_init(message)
            elif isinstance(message, SnapshotAbort):
                decision = recovery.process_abort(message)
            elif isinstance(message, SnapshotLoaded):
                decision = recovery.process_snapshot_loaded(message,)
            assert decision == expected_commit_decision

        assert recovery.get_completion_event() == outcome
        active_snapshot = recovery.get_active_snapshot()
        if expected_id:
            assert active_snapshot is not None
            assert active_snapshot[0] == expected_id
        else:
            assert active_snapshot is None
