from dataclasses import dataclass
from datetime import datetime
from typing import Sequence, Union


@dataclass
class Column:
    __slots__ = ["name", "type"]

    name: str
    type: str


Scalar = Union[None, bool, int, float, str, datetime]
Value = Union[Scalar, Sequence[Scalar]]


@dataclass
class Result:
    __slots__ = ["columns", "rows"]

    columns: Sequence[Column]
    rows: Sequence[Sequence[Value]]
