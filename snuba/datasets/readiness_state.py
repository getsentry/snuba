from __future__ import annotations

from enum import Enum


class ReadinessState(Enum):
    LIMITED = "limited"
    DEPRECATE = "deprecate"
    PARTIAL = "partial"
    COMPLETE = "complete"
    EXPERIMENTAL = "experimental"

    def __init__(self, _: str) -> None:
        self.order = {
            "limited": 1,
            "deprecate": 2,
            "experimental": 3,
            "partial": 4,
            "complete": 5,
        }

    def __gt__(self, other_rs: ReadinessState) -> bool:
        return self.order[self.value] > self.order[other_rs.value]

    def __lt__(self, other_rs: ReadinessState) -> bool:
        return self.order[self.value] < self.order[other_rs.value]
