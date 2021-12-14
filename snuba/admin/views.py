from typing import Optional

import simplejson as json
from flask import Flask, Response, request

from snuba import state
from snuba.admin.clickhouse.nodes import get_storage_info
from snuba.admin.runtime_config import (
    ConfigChange,
    ConfigType,
    get_config_type_from_value,
)

application = Flask(__name__, static_url_path="/static", static_folder="dist")


@application.route("/")
def root() -> Response:
    return application.send_static_file("index.html")


@application.route("/health")
def health() -> Response:
    return Response("OK", 200)


@application.route("/configs", methods=["GET", "POST"])
def configs() -> Response:
    if request.method == "POST":
        data = json.loads(request.data)
        try:
            key, value = data["key"], data["value"]

            assert isinstance(key, str), "Invalid key"
            assert isinstance(value, str), "Invalid value"
            assert key != "", "Key cannot be empty string"

        except (KeyError, AssertionError) as exc:
            return Response(
                json.dumps({"error": f"Invalid config: {str(exc)}"}),
                400,
                {"Content-Type": "application/json"},
            )

        existing_config = state.get_uncached_config(key)
        if existing_config is not None:
            return Response(
                json.dumps({"error": f"Config with key {key} exists"}),
                400,
                {"Content-Type": "application/json"},
            )

        state.set_config(
            key, value, user=request.headers.get("X-Goog-Authenticated-User-Email"),
        )

        evaluated_value = state.get_uncached_config(key)
        assert evaluated_value is not None
        evaluated_type = get_config_type_from_value(evaluated_value)

        config = {"key": key, "value": str(evaluated_value), "type": evaluated_type}

        return Response(json.dumps(config), 200, {"Content-Type": "application/json"})

    else:

        config_data = [
            {
                "key": k,
                "value": str(v) if v is not None else None,
                "type": get_config_type_from_value(v),
            }
            for (k, v) in state.get_raw_configs().items()
        ]

        return Response(
            json.dumps(config_data), 200, {"Content-Type": "application/json"},
        )


@application.route("/config_auditlog")
def config_changes() -> Response:
    def serialize(
        key: str,
        ts: float,
        user: Optional[str],
        before: Optional[ConfigType],
        after: Optional[ConfigType],
    ) -> ConfigChange:
        return {
            "key": key,
            "timestamp": ts,
            "user": user,
            "before": str(before) if before is not None else None,
            "beforeType": get_config_type_from_value(before),
            "after": str(after) if after is not None else None,
            "afterType": get_config_type_from_value(after),
        }

    data = [
        serialize(key, ts, user, before, after)
        for [key, ts, user, before, after] in state.get_config_changes()
    ]

    return Response(json.dumps(data), 200, {"Content-Type": "application/json"})


@application.route("/clickhouse_nodes")
def clickhouse_nodes() -> Response:
    return Response(
        json.dumps(get_storage_info()), 200, {"Content-Type": "application/json"}
    )
