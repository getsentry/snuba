from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class AppID:
    key: str
    created_by: str = "unknown"
    date_created: datetime = datetime.utcnow()

    @staticmethod
    def from_dict(obj: dict[str, Any]) -> AppID:
        return AppID(
            key=str(obj["key"]),
            created_by=str(obj["created_by"]),
            date_created=obj["date_created"],
        )
