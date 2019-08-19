from dataclasses import dataclass
from typing import Any, Mapping
from abc import ABC

from snuba.snapshots import SnapshotId


@dataclass(frozen=True)
class ControlMessage(ABC):
    id: SnapshotId
    product: str


@dataclass(frozen=True)
class SnapshotInit(ControlMessage):
    pass


@dataclass(frozen=True)
class SnapshotLoaded(ControlMessage):
    pass


def parse_control_message(message: Mapping[str, Any]) -> ControlMessage:
    event_type = message["event"]
    if event_type == "snapshot-init":
        return SnapshotInit(
            id=message["snapshot-id"],
            product=message["product"],
        )
    elif event_type == "snapshot-loaded":
        return SnapshotLoaded(
            id=message["snapshot-id"],
            product=message["product"],
        )
    else:
        raise ValueError("Invalid control message")
