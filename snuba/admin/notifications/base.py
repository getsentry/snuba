from abc import ABC
from datetime import datetime
from enum import Enum
from typing import Any, Mapping, Optional


class NotificationAction(Enum):
    ADDED = "added"
    REMOVED = "removed"
    UPDATED = "updated"


class NotificationBase(ABC):
    @property
    def timestamp(self) -> str:
        time = datetime.now()
        return time.strftime("%B %d, %Y %H:%M:%S %p")

    def notify(
        self,
        action: NotificationAction,
        data: Mapping[str, Any],
        user: Optional[str],
        timestamp: Optional[str] = None,
    ) -> None:
        """
        Given
        action: past-tense verb: 'created', 'removed', 'sent' etc.
        data: any extra data that is need to notify
        user: email address or other user identification of person taking action (could also be "auto")
        timestamp: time the action occurred (or can be generated when notify is called)
        """
        raise NotImplementedError()
