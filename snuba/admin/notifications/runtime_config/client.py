from typing import Any, Mapping, MutableMapping, Optional, Union

import structlog

from snuba import settings
from snuba.admin.notifications.base import NotificationAction, NotificationBase
from snuba.admin.notifications.slack.client import slack_client
from snuba.admin.notifications.slack.utils import build_blocks

ConfigType = Union[str, int, float]


class RuntimeConfigLogClient(NotificationBase):
    def __init__(self) -> None:
        self.logger = structlog.get_logger().bind(module=__name__)

    def _get_value_text(
        self,
        action: NotificationAction,
        old: Optional[ConfigType],
        new: Optional[ConfigType],
    ) -> str:
        if action == NotificationAction.CONFIG_OPTION_REMOVED:
            return f"(value:{old})"
        elif action == NotificationAction.CONFIG_OPTION_ADDED:
            return f"(value:{new})"
        elif action == NotificationAction.CONFIG_OPTION_UPDATED:
            return f"(from value:{old} to value:{new})"
        else:
            return ""

    def notify(
        self,
        action: NotificationAction,
        data: Mapping[str, Any],
        user: Optional[str],
        timestamp: Optional[str] = None,
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
            action=action.value,
            option=data["option"],
            old=data["old"],
            new=data["new"],
            timestamp=timestamp,
        )


class RuntimeConfigSlackClient(NotificationBase):
    def __init__(self) -> None:
        self.client = slack_client
        # feed-sns-admin channel
        self.channel_id = settings.SNUBA_SLACK_CHANNEL_ID

    def notify(
        self,
        action: NotificationAction,
        data: Mapping[str, Any],
        user: Optional[str],
        timestamp: Optional[str] = None,
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
        if not timestamp:
            timestamp = self.timestamp

        blocks = build_blocks(data, action, timestamp, user or "")
        payload: MutableMapping[str, Any] = {"blocks": blocks}

        self.client.post_message(message=payload, channel=self.channel_id)


class RuntimeConfigAutoClient(NotificationBase):
    """
    Uses the Slack client if is is configured, otherwise falls back to the log client
    """

    def __init__(self) -> None:
        is_slack_configured = (
            settings.SNUBA_SLACK_CHANNEL_ID is not None
            and settings.SLACK_API_TOKEN is not None
        )
        self.__notification_client = (
            RuntimeConfigSlackClient()
            if is_slack_configured
            else RuntimeConfigLogClient()
        )

    def notify(
        self,
        action: NotificationAction,
        data: Mapping[str, Any],
        user: Optional[str],
        timestamp: Optional[str] = None,
    ) -> None:
        return self.__notification_client.notify(action, data, user, timestamp)
