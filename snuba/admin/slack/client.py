from datetime import datetime
from typing import Any, Dict, MutableMapping, Optional, Union

import requests

from snuba import settings
from snuba.admin.slack.utils import build_blocks


class SlackClient(object):
    @property
    def token(self) -> Optional[str]:
        return settings.SLACK_API_TOKEN

    def post_message(
        self, message: MutableMapping[str, Any], channel: Optional[str] = None
    ) -> None:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

        if channel:
            message["channel"] = channel

        try:
            resp = requests.post(
                "https://slack.com/api/chat.postMessage", headers=headers, json=message,
            )
        except Exception:
            # todo: log non 200 failures
            return

        # todo: slack is annoying be a 200 could still be a failed case
        # you have to check the "ok" param in the response, so we should
        # check for ok: False and log those failures too.
        print(resp.status_code)
        print(resp.content)


class RuntimeConfigSlackClient(object):
    def __init__(self) -> None:
        self.client = SlackClient()
        # feed-sns-admin channel
        self.channel_id = settings.SNUBA_SLACK_CHANNEL_ID

    def notify(
        self,
        action: str,
        option_data: Dict[str, Union[str, float, int]],
        user: str,
        timestamp: Optional[str] = None,
    ) -> None:
        # Option data has the config option itself, and then the old or new values
        # {
        #     "option": "enable_events_read_only_table",
        #     "old": 0,
        #     "new": 1,
        # }
        if not timestamp:
            time = datetime.now()
            timestamp = time.strftime("%B %d, %Y %H:%M:%S %p")

        blocks = build_blocks(option_data, action, timestamp, user)
        payload: MutableMapping[str, Any] = {"blocks": blocks}

        self.client.post_message(message=payload, channel=self.channel_id)


runtime_config_slack_client = RuntimeConfigSlackClient()
