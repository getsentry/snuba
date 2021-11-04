from typing import Union

import simplejson as json
from flask import Flask, Response

from snuba import state

application = Flask(__name__, static_url_path="/static", static_folder="dist")


@application.route("/")
def root() -> Response:
    return application.send_static_file("index.html")


@application.route("/health")
def health() -> Response:
    return Response("OK", 200)


@application.route("/configs", methods=["GET"])
def configs() -> Response:
    config_data = [
        {"key": k, "value": v, "type": get_config_type(v)}
        for (k, v) in state.get_raw_configs().items()
    ]

    return Response(json.dumps(config_data), 200, {"Content-Type": "application/json"},)


def get_config_type(value: Union[str, int, float]) -> str:
    if isinstance(value, str):
        return "string"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    raise ValueError("Unexpected config type")
