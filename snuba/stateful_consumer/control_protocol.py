from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping
from abc import ABC, abstractclassmethod

import jsonschema

from snuba.snapshots import SnapshotId


CONTROL_MSG_SCHEMA = {
    'anyOf': [
        {"$ref": "#/definitions/snapshot-init"},
        {"$ref": "#/definitions/snapshot-abort"},
        {"$ref": "#/definitions/snapshot-loaded"},
    ],
    "definitions": {
        "base": {
            "type": "object",
            "properties": {
                "id": {
                    "type": "string"
                }
            },
            "required": ["event", "id"],
        },
        "snapshot-init": {
            "allOf": [
                {"$ref": "#/definitions/base"},
                {
                    "properties": {
                        "event": {"const": "snapshot-init"},
                        "product": {"type": "string"},
                    },
                    "required": ["event", "product"]
                }
            ]
        },
        "snapshot-abort": {
            "allOf": [
                {"$ref": "#/definitions/base"},
                {
                    "properties": {
                        "event": {"const": "snapshot-abort"},
                    },
                    "required": ["event"]
                }
            ]
        },
        "dataset": {
            "type": "object",
            "properties": {
                "temp_table": {"type": "string"},
            },
            "required": ["temp_table"],
        },
        "snapshot-loaded": {
            "allOf": [
                {"$ref": "#/definitions/base"},
                {
                    "properties": {
                        "event": {"const": "snapshot-loaded"},
                        "metadata": {
                            "type": "object",
                            "properties": {
                                "datasets": {
                                    "type": "object",
                                    "additionalProperties": {
                                        "type": {'$ref': '#/definitions/dataset'},
                                    },
                                },
                                "transaction-info": {
                                    "type": "object",
                                    "properties": {
                                        "xmin": {"type": "number"},
                                        "xmax": {"type": "number"},
                                        "xip-list": {
                                            "type": "array",
                                            "items": {
                                                {"type": "number"}
                                            }
                                        },
                                    }
                                }
                            },
                            "required": ["datasets", "transaction-info"],
                        }
                    },
                    "required": ["event", "product", "metadata"]
                }
            ]
        }
    }
}


@dataclass(frozen=True)
class ControlMessage(ABC):
    id: SnapshotId

    @abstractclassmethod
    def from_json(cls, json: str) -> ControlMessage:
        raise NotImplementedError


@dataclass(frozen=True)
class SnapshotInit(ControlMessage):
    product: str

    def


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
