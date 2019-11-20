from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from snuba.stateful_consumer.control_protocol import TransactionData
from snuba.snapshots import SnapshotId


class ConsumerStateCompletionEvent(Enum):
    CONSUMPTION_COMPLETED = 0
    SNAPSHOT_INIT_RECEIVED = 1
    SNAPSHOT_READY_RECEIVED = 2
    NO_SNAPSHOT = 3
    SNAPSHOT_CATCHUP_COMPLETED = 4


@dataclass
class ConsumerStateData:
    """
    Represent the state information we pass from one
    state to the other.
    """

    snapshot_id: SnapshotId
    transaction_data: TransactionData

    @classmethod
    def snapshot_ready_state(
        cls, snapshot_id: SnapshotId, transaction_data: TransactionData,
    ) -> ConsumerStateData:
        """
        Builds the StateData to share when we have a valid snapshot id to
        work on.
        """
        return ConsumerStateData(
            snapshot_id=snapshot_id, transaction_data=transaction_data,
        )
