import pytest

from snuba.stateful_consumer import StateOutput
from snuba.stateful_consumer.bootstrap_state import RecoveryState
from snuba.stateful_consumer.control_protocol import (
    SnapshotInit,
    SnapshotAbort,
    SnapshotLoaded,
)


class TestRecoveryState:
    test_data = [
        (
            # Empty topic.
            [],
            StateOutput.NO_SNAPSHOT,
            None,
        ),
        (
            # One snapshot started
            [
                SnapshotInit(id="123asd", product="snuba")
            ],
            StateOutput.SNAPSHOT_INIT_RECEIVED,
            "123asd"
        ),
        (
            # initialized and aborted snapshot
            [
                SnapshotInit(id="123asd", product="snuba"),
                SnapshotAbort(id="123asd"),
            ],
            StateOutput.NO_SNAPSHOT,
            None,
        ),
        (
            # Initialized and ready
            [
                SnapshotInit(id="123asd", product="snuba"),
                SnapshotLoaded(
                    id="123asd",
                    datasets=None,
                    transaction_info=None,
                ),
            ],
            StateOutput.SNAPSHOT_READY_RECEIVED,
            "123asd"
        ),
        (
            # Initialized and multiple overlapping snapshots that are ignored
            [
                SnapshotInit(id="123asd", product="snuba"),
                SnapshotInit(id="234asd", product="someoneelse"),
                SnapshotAbort(id="234asd"),
                SnapshotInit(id="345asd", product="snuba"),
            ],
            StateOutput.SNAPSHOT_INIT_RECEIVED,
            "123asd"
        ),
        (
            # Multiple successful consecutive snapshots
            [
                SnapshotInit(id="123asd", product="snuba"),
                SnapshotLoaded(
                    id="123asd",
                    datasets=None,
                    transaction_info=None,
                ),
                SnapshotInit(id="234asd", product="snuba"),
                SnapshotLoaded(
                    id="234asd",
                    datasets=None,
                    transaction_info=None,
                ),
                SnapshotInit(id="345asd", product="snuba"),
            ],
            StateOutput.SNAPSHOT_INIT_RECEIVED,
            "345asd"
        )
    ]

    @pytest.mark.parametrize("events, outcome, expected_id", test_data)
    def test_recovery(self, events, outcome, expected_id) -> None:
        recovery = RecoveryState()
        for message in events:
            if isinstance(message, SnapshotInit):
                recovery.process_init(message)
            elif isinstance(message, SnapshotAbort):
                recovery.process_abort(message)
            elif isinstance(message, SnapshotLoaded):
                recovery.process_snapshot_loaded(
                    message,
                )

        assert recovery.get_output() == outcome
        if expected_id:
            assert recovery.get_active_snapshot_msg().id == expected_id
        else:
            assert recovery.get_active_snapshot_msg() is None
