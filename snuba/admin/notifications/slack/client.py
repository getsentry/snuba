import logging
from typing import Any, MutableMapping, Optional

import requests

from snuba import settings

logger = logging.getLogger("snuba.admin.notifications.slack")


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
        except Exception as exc:
            logger.error(exc, exc_info=True)

        # todo: slack is annoying be a 200 could still be a failed case
        # you have to check the "ok" param in the response, so we should
        # check for ok: False and log those failures too.
        if resp.status_code != 200:
            logger.error("Slack error: {resp.content}")
