from typing import Any, Literal, Sequence, Union, TypedDict


class BeginEvent(TypedDict):
    event: Literal["begin"]
    xid: int


class ChangeEvent(TypedDict):
    event: Literal["change"]
    xid: int
    timestamp: str
    schema: str
    table: str


class InsertEvent(ChangeEvent):
    kind: Literal["insert"]
    columnnames: Sequence[str]
    columntypes: Sequence[str]
    columnvalues: Sequence[Any]


class OldKeys(TypedDict):
    keynames: Sequence[str]
    keytypes: Sequence[str]
    keyvalues: Sequence[Any]


class UpdateEvent(ChangeEvent):
    kind: Literal["update"]
    columnnames: Sequence[str]
    columntypes: Sequence[str]
    columnvalues: Sequence[Any]
    oldkeys: OldKeys


class DeleteEvent(ChangeEvent):
    kind: Literal["delete"]
    oldkeys: OldKeys


class CommitEvent(TypedDict):
    event: Literal["commit"]


Event = Union[
    BeginEvent, InsertEvent, UpdateEvent, DeleteEvent, CommitEvent,
]
