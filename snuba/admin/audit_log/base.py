from datetime import datetime
from typing import Any, Mapping, MutableMapping, Optional

import structlog

from snuba.admin.notifications.slack.client import slack_client
from snuba.admin.notifications.slack.utils import build_blocks


class AuditLog:
    def __init__(self, module: Optional[str] = None):
        if not module:
            module = __name__
        self.logger = structlog.get_logger().bind(module=module)
        self.client = slack_client

    def _format_data(
        self, user: str, timestamp: str, action: str, data: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        return data

    def _record(
        self, user: str, timestamp: str, action: str, data: Mapping[str, Any]
    ) -> None:
        formatted = self._format_data(user, timestamp, action, data)
        self.logger.info(event=action, user=user, timestamp=timestamp, **formatted)

    def _notify(
        self, user: str, timestamp: str, action: str, data: Mapping[str, Any]
    ) -> None:
        if self.client.is_configured:
            blocks = build_blocks(data, action, timestamp, user)
            payload: MutableMapping[str, Any] = {"blocks": blocks}

            self.client.post_message(message=payload)

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
