from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Mapping, Optional


class AuditLog(ABC):
    @abstractmethod
    def _record(
        self, user: str, timestamp: str, action: str, data: Mapping[str, Any]
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def _notify(
        self, user: str, timestamp: str, action: str, data: Mapping[str, Any]
    ) -> None:
        raise NotImplementedError

    @property
    def timestamp(self) -> str:
        time = datetime.now()
        return time.strftime("%B %d, %Y %H:%M:%S %p")

    def audit(
        self,
        user: str,
        action: str,
        data: Mapping[str, Any],
        notify: bool = False,
        timestamp: Optional[str] = None,
    ) -> None:
        if not timestamp:
            timestamp = self.timestamp
        self._record(user, timestamp, action, data)
        if notify:
            self._notify(user, timestamp, action, data)
