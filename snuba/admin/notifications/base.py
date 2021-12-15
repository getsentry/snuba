import logging
from abc import ABC
from datetime import datetime
from typing import Any, Dict, MutableMapping, Optional, Union

from snuba import settings
from snuba.admin.notifications.slack.client import SlackClient
from snuba.admin.notifications.slack.utils import build_blocks


class NotificationBase(ABC):
    @property
    def timestamp(self) -> str:
        time = datetime.now()
        return time.strftime("%B %d, %Y %H:%M:%S %p")

    def notify(
        self,
        action: str,
        data: Dict[str, Union[str, float, int]],
        user: str,
        timestamp: Optional[str],
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
        self.logger = logging.getLogger("runtime-config.notify")

    def _get_value_text(
        self, action: str, old: Union[str, int, float], new: Union[str, int, float]
    ) -> str:
        if action == "removed":
            return f"(value:{old})"
        elif action == "added":
            return f"(value:{new})"
        elif action == "updated":
            return f"(from value:{old} to value:{new})"
        else:
            return ""

    def notify(
        self,
        action: str,
        data: Dict[str, Union[str, float, int]],
        user: str,
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

            The text generated will look like:
              * meredith@sentry.io added enable_events_read_only_table (value:1)
              * meredith@sentry.io removed enable_events_read_only_table (value:0)
              * meredith@sentry.io updated enable_events_read_only_table (from value:0 to value:1)

        """

        base_text = f'{user} {action} {data["option"]}'
        value_text = self._get_value_text(action, data["old"], data["new"])
        full_text = f"{base_text} {value_text}"

        self.logger.info(full_text)


class RuntimeConfigSlackClient(NotificationBase):
    def __init__(self) -> None:
        self.client = SlackClient()
        # feed-sns-admin channel
        self.channel_id = settings.SNUBA_SLACK_CHANNEL_ID

    def notify(
        self,
        action: str,
        data: Dict[str, Union[str, float, int]],
        user: str,
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

        blocks = build_blocks(data, action, timestamp, user)
        payload: MutableMapping[str, Any] = {"blocks": blocks}

        self.client.post_message(message=payload, channel=self.channel_id)


rc_log_client = RuntimeConfigLogClient()
rc_slack_client = RuntimeConfigSlackClient()
