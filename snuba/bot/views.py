from __future__ import annotations

import threading
from datetime import datetime
from functools import partial
from typing import Any

import requests
import simplejson as json
import structlog
from flask import Flask, Response, jsonify, make_response, request
from humanize import naturaltime

from snuba.bot.github import fetch_notifications, initialize_github

logger = structlog.get_logger().bind(module=__name__)


application = Flask(__name__)


@application.errorhandler(Exception)
def handle_exception(e: Exception) -> Response:
    logger.error("Error in request", error=str(e))
    return make_response(jsonify({"text": "Error in request"}), 200)


def serialize_notification(notif: dict[str, Any]) -> dict[str, Any]:
    title = notif["subject"]["title"]
    url = notif["subject"]["html_url"]  # This is added in our fetching code

    repo_name = notif["repository"]["full_name"]
    updated_at = notif["updated_at"]
    try:
        iso_datetime = datetime.fromisoformat(updated_at.rstrip("Z"))
        human_updated_at = naturaltime(iso_datetime)
    except Exception:
        human_updated_at = updated_at

    block = {
        "type": "section",
        "fields": [
            {
                "type": "mrkdwn",
                "text": f"*<{url}|{title}>*\n{repo_name}",
            },
            {"type": "mrkdwn", "text": f"{human_updated_at} --- {updated_at}"},
        ],
    }

    return block


thread_count = 0
thread_count_lock = threading.Lock()


def start_response_thread(github_token: str, response_url: str) -> None:
    global thread_count

    with thread_count_lock:
        if thread_count > 10:
            raise Exception("Too many threads")
        thread_count += 1

    try:
        thread = threading.Thread(
            target=respond_to_slack, args=(github_token, response_url)
        )
        thread.start()
    finally:
        with thread_count_lock:
            thread_count -= 1


def respond_to_slack(github_token: str, response_url: str) -> None:
    slack_resp = partial(send_to_slack, response_url)
    try:
        api, github_login = initialize_github(github_token)
    except Exception as e:
        logger.error("Failed to initialize github", error=str(e))
        slack_resp({"text": "Failed to initialize github"})
        return

    try:
        notifications = fetch_notifications(api, github_login)
    except Exception as e:
        logger.error("Failed to pull notifications", error=str(e))
        slack_resp({"text": "Failed to fetch github notifications"})
        return

    try:
        response = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "Notifications",
                    },
                },
                *[serialize_notification(n) for n in notifications],
            ]
        }
        slack_resp(response)
    except Exception as e:
        logger.error("Failed to serialize notifications", error=str(e))
        return


def send_to_slack(response_url: str, response: dict[str, Any]) -> None:
    try:
        response["replace_original"] = "true"
        slack_resp = requests.post(response_url, data=json.dumps(response))
        if slack_resp.status_code != 200:
            raise Exception(f"Failed to post response to slack: {slack_resp.text}")
    except Exception as e:
        logger.error("Failed to post response to slack", error=str(e))


@application.route("/notifications", methods=["POST"])
def gh_notifications() -> Response:
    response_url = request.values["response_url"]  # TODO: will probably need this
    github_token = request.values["text"]

    start_response_thread(github_token, response_url)

    response = {
        "response_type": "ephemeral",
        "text": "Fetching notifications :hourglass_flowing_sand:",
    }
    return make_response(jsonify(response), 200)
