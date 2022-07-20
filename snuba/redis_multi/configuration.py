from enum import Enum
from typing import NamedTuple, Optional, Sequence, Union


class NodeDescriptor(NamedTuple):
    host: str
    port: int
    db: int
    password: Optional[str]


ConnectionDescriptor = Union[NodeDescriptor, Sequence[NodeDescriptor]]


class ClusterFunction(Enum):
    CACHE = 0
    RATE_LIMITING = 1
    SUBSCRIPTION_STORAGE = 2
    REPLACEMENT_STORAGE = 3
