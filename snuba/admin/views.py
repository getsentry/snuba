import simplejson as json
from flask import Flask, Response

from snuba import state
from snuba.admin.runtime_config import get_config_type_from_value

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
        {"key": k, "value": v, "type": get_config_type_from_value(v)}
        for (k, v) in state.get_raw_configs().items()
    ]

    return Response(json.dumps(config_data), 200, {"Content-Type": "application/json"},)


@application.route("/config_auditlog")
def config_changes() -> Response:
    data = [
        {"key": key, "timestamp": ts, "user": user, "before": before, "after": after}
        for [key, [ts, user, before, after]] in state.get_config_changes()
    ]

    return Response(json.dumps(data), 200, {"Content-Type": "application/json"},)
