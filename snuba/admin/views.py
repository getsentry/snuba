from flask import Flask, Response

application = Flask(__name__, static_url_path="", static_folder="dist")


@application.route("/")
def root() -> Response:
    return application.send_static_file("index.html")


@application.route("/test-slack")
def test_slack() -> Response:
    from typing import Dict, Union

    from snuba.admin.slack.client import runtime_config_slack_client

    data: Dict[str, Union[str, float, int]] = {
        "option": "enable_events_read_only_table",
        "old": 0,
        "new": 1,
    }

    runtime_config_slack_client.notify(
        action="Removed", option_data=data, user="meredith@sentry.io"
    )

    return application.send_static_file("index.html")
