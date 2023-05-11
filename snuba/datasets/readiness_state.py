from __future__ import annotations

from enum import Enum


class ReadinessState(Enum):
    LIMITED = "limited"
    DEPRECATE = "deprecate"
    PARTIAL = "partial"
    COMPLETE = "complete"

    def __init__(self, _: str) -> None:
        self.order = {"limited": 1, "deprecate": 2, "partial": 3, "complete": 4}

    def __gt__(self, other_rs: ReadinessState) -> bool:
        return self.order[self.value] > self.order[other_rs.value]

    def __lt__(self, other_rs: ReadinessState) -> bool:
        return self.order[self.value] < self.order[other_rs.value]
