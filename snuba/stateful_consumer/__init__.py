from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from snuba.stateful_consumer.control_protocol import TransactionData


class StateType(Enum):
    BOOTSTRAP = 0
    CONSUMING = 1
    SNAPSHOT_PAUSED = 2
    CATCHING_UP = 3
    FINISHED = 4


class StateOutput(Enum):
    FINISH = 0
    SNAPSHOT_INIT_RECEIVED = 1
    SNAPSHOT_READY_RECEIVED = 2
    NO_SNAPSHOT = 3
    SNAPSHOT_CATCHUP_COMPLETED = 4


@dataclass
class StateData:
    """
    Represent the state information we pass from one
    state to the other.
    """
    snapshot_id: Optional[str]
    transaction_data: Optional[TransactionData]

    @classmethod
    def no_snapshot_state(cls) -> StateData:
        """
        Builds an empty StateData that represent a state where there is no
        snapshot to care about.
        """
        return StateData(None, None)

    @classmethod
    def snapshot_ready_state(
        cls,
        snapshot_id: str,
        transaction_data: TransactionData,
    ) -> StateData:
        """
        Builds the StateData to share when we have a valid snapshot id to
        work on.
        """
        return StateData(
            snapshot_id=snapshot_id,
            transaction_data=transaction_data,
        )
