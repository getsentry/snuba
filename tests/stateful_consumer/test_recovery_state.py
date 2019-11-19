import pytest

from snuba.consumers.strict_consumer import CommitDecision
from snuba.stateful_consumer import ConsumerStateCompletionEvent
from snuba.stateful_consumer.states.bootstrap import RecoveryState
from snuba.stateful_consumer.control_protocol import (
    SnapshotInit,
    SnapshotAbort,
    SnapshotLoaded,
    TransactionData,
)


class TestRecoveryState:
    transaction_data = TransactionData(xmin=1, xmax=2, xip_list=[],)
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
                    SnapshotInit(id="123asd", product="snuba", tables=["some_table"]),
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
                        id="123asd", product="snuba", tables=["sentry_groupedmessage"]
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
                        id="123asd", product="snuba", tables=["sentry_groupedmessage"]
                    ),
                    CommitDecision.COMMIT_PREV,
                ),
                (SnapshotAbort(id="123asd"), CommitDecision.COMMIT_THIS),
            ],
            ConsumerStateCompletionEvent.NO_SNAPSHOT,
            None,
        ),
        (
            # Initialized and ready
            [
                (
                    SnapshotInit(
                        id="123asd", product="snuba", tables=["sentry_groupedmessage"]
                    ),
                    CommitDecision.COMMIT_PREV,
                ),
                (
                    SnapshotLoaded(id="123asd", transaction_info=transaction_data,),
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
                        id="123asd", product="snuba", tables=["sentry_groupedmessage"]
                    ),
                    CommitDecision.COMMIT_PREV,
                ),
                (
                    SnapshotInit(
                        id="234asd",
                        product="someoneelse",
                        tables=["sentry_groupedmessage"],
                    ),
                    CommitDecision.DO_NOT_COMMIT,
                ),
                (SnapshotAbort(id="234asd"), CommitDecision.DO_NOT_COMMIT),
                (
                    SnapshotInit(
                        id="345asd", product="snuba", tables=["sentry_groupedmessage"]
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
                        id="123asd", product="snuba", tables=["sentry_groupedmessage"]
                    ),
                    CommitDecision.COMMIT_PREV,
                ),
                (
                    SnapshotLoaded(id="123asd", transaction_info=transaction_data,),
                    CommitDecision.DO_NOT_COMMIT,
                ),
                (
                    SnapshotInit(
                        id="234asd", product="snuba", tables=["sentry_groupedmessage"]
                    ),
                    CommitDecision.COMMIT_PREV,
                ),
                (
                    SnapshotLoaded(id="234asd", transaction_info=transaction_data,),
                    CommitDecision.DO_NOT_COMMIT,
                ),
                (
                    SnapshotInit(
                        id="345asd", product="snuba", tables=["sentry_groupedmessage"]
                    ),
                    CommitDecision.COMMIT_PREV,
                ),
            ],
            ConsumerStateCompletionEvent.SNAPSHOT_INIT_RECEIVED,
            "345asd",
        ),
    ]

    @pytest.mark.parametrize("events, outcome, expected_id", test_data)
    def test_recovery(self, events, outcome, expected_id) -> None:
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
        if expected_id:
            assert recovery.get_active_snapshot()[0] == expected_id
        else:
            assert recovery.get_active_snapshot() is None
