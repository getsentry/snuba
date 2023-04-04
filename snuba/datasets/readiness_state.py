from __future__ import annotations

from enum import Enum
from typing import Any


class ReadinessState(Enum):
    LIMITED = "limited", 1
    DEPRECATE = "deprecate", 2
    PARTIAL = "partial", 3
    COMPLETE = "complete", 4

    def __new__(
        cls: Any, *args: tuple[Any], **kwargs: dict[str, Any]
    ) -> ReadinessState:
        obj: ReadinessState = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    def __init__(self, _: str, level: int):
        self.level = level
