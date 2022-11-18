from typing import Any, Mapping, MutableMapping

import structlog

from snuba.admin.audit_log.base import AuditLog
from snuba.admin.notifications.slack.client import slack_client
from snuba.admin.notifications.slack.utils import build_blocks


class RuntimeConfigAuditLog(AuditLog):
    def __init__(self) -> None:
        self.logger = structlog.get_logger().bind(module=__name__)
        self.client = slack_client

    def _record(
        self, user: str, timestamp: str, action: str, data: Mapping[str, Any]
    ) -> None:
        """
        Data has the runtime option, its old value, and new value.
        If it's being removed, the new value will be `None`. Likewise
        if it's being added, the old value will be `None`.

        example:
            {
                "option": "enable_events_read_only_table",
                "old": 0, // or None if added
                "new": 1, // or None if removed
            }

        """

        self.logger.info(
            event="value_updated",
            user=user,
            action=action,
            option=data.get("option"),
            old=data.get("old"),
            new=data.get("new"),
            timestamp=timestamp,
        )

    def _notify(
        self, user: str, timestamp: str, action: str, data: Mapping[str, Any]
    ) -> None:
        """
        Data has the runtime option, its old value, and new value.
        If it's being removed, the new value will be `None`. Likewise
        if it's being added, the old value will be `None`.

        example:
            {
                "option": "enable_events_read_only_table",
                "old": 0, // or None if added
                "new": 1, // or None if removed
            }

        """
        if self.client.is_configured:
            blocks = build_blocks(data, action, timestamp, user)
            payload: MutableMapping[str, Any] = {"blocks": blocks}

            self.client.post_message(message=payload)


runtime_config_auditlog = RuntimeConfigAuditLog()
