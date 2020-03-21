from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ClickhouseConnectionConfig:
    host: str
    port: int
    http_port: Optional[int]
