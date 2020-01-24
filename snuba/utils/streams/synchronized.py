from dataclasses import dataclass

from snuba.utils.streams.types import Partition


@dataclass(frozen=True)
class Commit:
    __slots__ = ["group", "partition", "offset"]

    group: str
    partition: Partition
    offset: int
