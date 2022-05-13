from datetime import datetime
from typing import NamedTuple, Optional

from arroyo.backends.kafka.consumer import Headers


class KafkaMessageMetadata(NamedTuple):
    offset: int
    partition: int
    timestamp: datetime
    topic: str
    key: Optional[bytes]
    headers: Headers
