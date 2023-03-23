from enum import Enum


class ReadinessState(Enum):
    LIMITED = "limited"
    DEPRECATE = "deprecate"
    PARTIAL = "partial"
    COMPLETE = "complete"
