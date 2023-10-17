from typing import Any, MutableMapping, Optional

import requests
import structlog

logger = structlog.get_logger().bind(module=__name__)


class SlackClient(object):
    def __init__(
        self, channel_id: Optional[str] = None, token: Optional[str] = None
    ) -> None:
        self.__channel_id = channel_id
        self.__token = token

    @property
    def is_configured(self) -> bool:
        return self.__channel_id is not None and self.__token is not None

    def post_message(self, message: MutableMapping[str, Any]) -> None:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.__token}",
        }

        message["channel"] = self.__channel_id

        try:
            resp = requests.post(
                "https://slack.com/api/chat.postMessage",
                headers=headers,
                json=message,
            )
        except Exception as exc:
            logger.error(exc, exc_info=True)
            return

        content_type = resp.headers["content-type"]
        if content_type == "text/html":
            is_ok = str(resp.content) == "ok"
            error_option = resp.content

        else:
            response = resp.json()
            is_ok = response.get("ok")
            error_option = response.get("error")

        if not is_ok:
            logger.error(f"Slack error: {str(error_option)}")

    def post_file(
        self,
        file_name: str,
        file_path: str,
        file_type: str,
        initial_comment: Optional[str] = None,
    ) -> None:
        headers = {
            "Authorization": f"Bearer {self.__token}",
        }

        data = {
            "channels": self.__channel_id,
            "initial_comment": initial_comment,
        }

        files = {
            "file": (file_name, open(file_path, "rb"), file_type),
        }

        try:
            resp = requests.post(
                "https://slack.com/api/files.upload",
                headers=headers,
                data=data,
                files=files,
            )
        except Exception as exc:
            logger.error(exc, exc_info=True)
            return

        content_type = resp.headers["content-type"]
        if content_type == "text/html":
            is_ok = str(resp.content) == "ok"
            error_option = resp.content

        else:
            response = resp.json()
            is_ok = response.get("ok")
            error_option = response.get("error")

        if not is_ok:
            logger.error(f"Slack error: {str(error_option)}")
