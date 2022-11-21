from datetime import datetime
from enum import Enum
from typing import Any, Mapping, MutableMapping, Optional, Union

import structlog

from snuba.admin.notifications.slack.client import slack_client
from snuba.admin.notifications.slack.utils import build_blocks


class AuditLogAction(Enum):
    # action.resource
    ADDED_OPTION = "added.option"
    REMOVED_OPTION = "removed.option"
    UPDATED_OPTION = "updated.option"
    RAN_QUERY = "ran.query"
    RAN_MIGRATION = "ran.migration"
    REVERSED_MIGRATION = "reversed.migration"


class AuditLog:
    def __init__(self) -> None:
        self.logger = structlog.get_logger()
        self.client = slack_client

    @property
    def timestamp(self) -> str:
        time = datetime.now()
        return time.strftime("%B %d, %Y %H:%M:%S %p")

    def record(
        self,
        user: str,
        action: AuditLogAction,
        data: Mapping[str, Union[str, int]],
        timestamp: Optional[str],
        notify: Optional[bool] = False,
    ) -> None:
        if not timestamp:
            timestamp = self.timestamp

        self.logger.info(
            event=f"{user} {action.value}",
            user=user,
            timestamp=timestamp,
            **data,
        )
        if notify and self.client.is_configured:
            blocks = build_blocks(data, action, timestamp, user)
            payload: MutableMapping[str, Any] = {"blocks": blocks}

            self.client.post_message(message=payload)
