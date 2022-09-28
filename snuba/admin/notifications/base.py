from abc import ABC
from datetime import datetime
from enum import Enum
from typing import Any, MutableMapping, Optional, TypedDict, Union

import structlog

from snuba import settings
from snuba.admin.notifications.slack.client import SlackClient
from snuba.admin.notifications.slack.utils import build_blocks


class RuntimeConfigAction(Enum):
    ADDED = "added"
    REMOVED = "removed"
    UPDATED = "updated"


ConfigType = Union[str, int, float]

NotificationData = TypedDict(
    "NotificationData",
    {"option": str, "old": Optional[ConfigType], "new": Optional[ConfigType]},
)


class NotificationBase(ABC):
    @property
    def timestamp(self) -> str:
        time = datetime.now()
        return time.strftime("%B %d, %Y %H:%M:%S %p")

    def notify(
        self,
        action: RuntimeConfigAction,
        data: NotificationData,
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


class RuntimeConfigLogClient(NotificationBase):
    def __init__(self) -> None:
        self.logger = structlog.get_logger().bind(module=__name__)

    def _get_value_text(
        self,
        action: RuntimeConfigAction,
        old: Optional[ConfigType],
        new: Optional[ConfigType],
    ) -> str:
        if action == RuntimeConfigAction.REMOVED:
            return f"(value:{old})"
        elif action == RuntimeConfigAction.ADDED:
            return f"(value:{new})"
        elif action == RuntimeConfigAction.UPDATED:
            return f"(from value:{old} to value:{new})"
        else:
            return ""

    def notify(
        self,
        action: RuntimeConfigAction,
        data: NotificationData,
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
        self.client = SlackClient()
        # feed-sns-admin channel
        self.channel_id = settings.SNUBA_SLACK_CHANNEL_ID

    def notify(
        self,
        action: RuntimeConfigAction,
        data: NotificationData,
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

        blocks = build_blocks(data, action.value, timestamp, user or "")
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
        action: RuntimeConfigAction,
        data: NotificationData,
        user: Optional[str],
        timestamp: Optional[str] = None,
    ) -> None:
        return self.__notification_client.notify(action, data, user, timestamp)
