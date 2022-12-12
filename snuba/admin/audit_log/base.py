from datetime import datetime
from typing import Any, Mapping, MutableMapping, Optional, Union

import structlog

from snuba.admin.audit_log.action import AuditLogAction
from snuba.admin.notifications.slack.client import SlackClient
from snuba.admin.notifications.slack.utils import build_blocks


class AuditLog:
    def __init__(self) -> None:
        self.logger = structlog.get_logger().bind(
            module=self.__class__.__module__, context_class=self.__class__.__qualname__
        )
        self.client = SlackClient()

    def record(
        self,
        user: str,
        action: AuditLogAction,
        data: Mapping[str, Union[str, int]],
        notify: Optional[bool] = False,
    ) -> None:
        timestamp = datetime.now().strftime("%B %d, %Y %H:%M:%S %p")
        self.logger.info(
            event=action.value,
            user=user,
            timestamp=timestamp,
            **data,
        )
        if notify and self.client.is_configured:
            blocks = build_blocks(data, action, timestamp, user)
            payload: MutableMapping[str, Any] = {"blocks": blocks}
            self.client.post_message(message=payload)
