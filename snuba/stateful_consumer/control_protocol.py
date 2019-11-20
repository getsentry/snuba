from __future__ import annotations

import jsonschema

from dataclasses import dataclass
from typing import Any, Mapping, Sequence
from abc import ABC

from snuba.snapshots import SnapshotId
from snuba.snapshots.postgres_snapshot import Xid

# TODO: Putting all messages in a single shema makes it hard to interpret errors
# since the parser will always try all the options and it will rarely tell us which
# is the actual error because it does not know which is the expected message.
# We should try to split them into three schemas.
CONTROL_MSG_SCHEMA = {
    "anyOf": [
        {"$ref": "#/definitions/snapshot-init"},
        {"$ref": "#/definitions/snapshot-abort"},
        {"$ref": "#/definitions/snapshot-loaded"},
    ],
    "definitions": {
        "base": {
            "type": "object",
            "properties": {"snapshot-id": {"type": "string"}},
            "required": ["snapshot-id"],
        },
        "snapshot-init": {
            "allOf": [
                {"$ref": "#/definitions/base"},
                {
                    "properties": {
                        "event": {"const": "snapshot-init"},
                        "product": {"type": "string"},
                        "tables": {"type": "array", "items": [{"type": "string"}]},
                    },
                    "required": ["event", "product", "tables"],
                },
            ]
        },
        "snapshot-abort": {
            "allOf": [
                {"$ref": "#/definitions/base"},
                {
                    "properties": {"event": {"const": "snapshot-abort"}},
                    "required": ["event"],
                },
            ]
        },
        "snapshot-loaded": {
            "allOf": [
                {"$ref": "#/definitions/base"},
                {
                    "properties": {
                        "event": {"const": "snapshot-loaded"},
                        "transaction-info": {
                            "type": "object",
                            "properties": {
                                "xmin": {"type": "number"},
                                "xmax": {"type": "number"},
                                "xip-list": {
                                    "type": "array",
                                    "items": [{"type": "number"}],
                                },
                            },
                            "required": ["xmin", "xmax", "xip-list"],
                        },
                    },
                    "required": ["event", "transaction-info"],
                },
            ]
        },
    },
}


@dataclass(frozen=True)
class ControlMessage(ABC):
    id: SnapshotId

    @classmethod
    def from_json(cls, json: Mapping[str, Any]) -> ControlMessage:
        raise NotImplementedError


@dataclass(frozen=True)
class SnapshotInit(ControlMessage):
    product: str
    tables: Sequence[str]

    @classmethod
    def from_json(cls, json: Mapping[str, Any]) -> ControlMessage:
        assert json["event"] == "snapshot-init"
        return cls(
            id=json["snapshot-id"], tables=json["tables"], product=json["product"],
        )


@dataclass(frozen=True)
class SnapshotAbort(ControlMessage):
    @classmethod
    def from_json(cls, json: Mapping[str, Any]) -> ControlMessage:
        assert json["event"] == "snapshot-abort"
        return cls(id=json["snapshot-id"],)


@dataclass(frozen=True)
class TransactionData:
    """
    Provides the metadata for the loaded snapshot.
    """

    xmin: Xid
    xmax: Xid
    xip_list: Sequence[Xid]


@dataclass(frozen=True)
class SnapshotLoaded(ControlMessage):
    transaction_info: TransactionData

    @classmethod
    def from_json(cls, json: Mapping[str, Any]) -> ControlMessage:
        assert json["event"] == "snapshot-loaded"
        return cls(
            id=json["snapshot-id"],
            transaction_info=TransactionData(
                xmin=json["transaction-info"]["xmin"],
                xmax=json["transaction-info"]["xmax"],
                xip_list=json["transaction-info"]["xip-list"],
            ),
        )

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "event": "snapshot-loaded",
            "snapshot-id": self.id,
            "transaction-info": {
                "xmin": self.transaction_info.xmin,
                "xmax": self.transaction_info.xmax,
                "xip-list": self.transaction_info.xip_list,
            },
        }


def parse_control_message(message: Mapping[str, Any]) -> ControlMessage:
    jsonschema.validate(message, CONTROL_MSG_SCHEMA)
    event_type = message["event"]
    if event_type == "snapshot-init":
        return SnapshotInit.from_json(message)
    elif event_type == "snapshot-abort":
        return SnapshotAbort.from_json(message)
    elif event_type == "snapshot-loaded":
        return SnapshotLoaded.from_json(message)
    else:
        raise ValueError(f"Invalid control message with event type: {event_type}")
