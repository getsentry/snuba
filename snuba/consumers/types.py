from datetime import datetime

from typing import NamedTuple


class KafkaMessageMetadata(NamedTuple):
    offset: int
    partition: int
    timestamp: datetime
