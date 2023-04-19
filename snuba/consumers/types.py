from datetime import datetime
from typing import Mapping, NamedTuple, Optional


class KafkaMessageMetadata(NamedTuple):
    offset: int
    partition: int
    timestamp: datetime
    headers: Optional[Mapping[str, str]] = None
