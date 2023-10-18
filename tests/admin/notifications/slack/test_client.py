import tempfile
from unittest.mock import ANY, MagicMock, patch

import pytest

from snuba.admin.notifications.slack.client import SlackClient


@pytest.fixture
def slack_client() -> SlackClient:
    return SlackClient(channel_id="test_channel", token="test_token")


def test_post_message(slack_client: SlackClient) -> None:
    message = {"text": "test message"}
    with patch("requests.post", MagicMock()) as mock_post:
        slack_client.post_message(message)

        mock_post.assert_called_once_with(
            "https://slack.com/api/chat.postMessage",
            headers={
                "Authorization": "Bearer test_token",
                "Content-Type": "application/json",
            },
            json={"channel": "test_channel", **message},
        )


def test_post_file(slack_client: SlackClient) -> None:
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        # Write some test data to the file
        f.write(b"test,data")
        # Get the file path
        file_path = f.name
        print(file_path)

        file_name = "test_file"
        file_type = "text/plain"
        initial_comment = "test comment"

        with patch("requests.post", MagicMock()) as mock_post:
            slack_client.post_file(file_name, file_path, file_type, initial_comment)

            mock_post.assert_called_once_with(
                "https://slack.com/api/files.upload",
                headers={"Authorization": "Bearer test_token"},
                data={
                    "channels": "test_channel",
                    "initial_comment": initial_comment,
                },
                files={
                    "file": (file_name, ANY, file_type),
                },
            )
