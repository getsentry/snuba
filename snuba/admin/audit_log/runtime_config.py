from typing import Any, Mapping, MutableMapping, Optional

import structlog

from snuba import settings
from snuba.admin.audit_log.base import AuditLog
from snuba.admin.notifications.slack.client import SlackClient
from snuba.admin.notifications.slack.utils import build_blocks


class RuntimeConfigAuditLog(AuditLog):
    def __init__(self) -> None:
        self.logger = structlog.get_logger().bind(module=__name__)
        self.notification_client: Optional[SlackClient] = None
        self.channel_id: Optional[str] = None

        if (
            settings.SNUBA_SLACK_CHANNEL_ID is not None
            and settings.SLACK_API_TOKEN is not None
        ):
            self.notification_client = SlackClient()
            # feed-sns-admin channel
            self.channel_id = settings.SNUBA_SLACK_CHANNEL_ID

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
            option=data["option"],
            old=data["old"],
            new=data["new"],
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
        if self.notification_client and self.channel_id:
            blocks = build_blocks(data, action, timestamp, user)
            payload: MutableMapping[str, Any] = {"blocks": blocks}

            self.notification_client.post_message(
                message=payload, channel=self.channel_id
            )


runtime_config_auditlog = RuntimeConfigAuditLog()
