from __future__ import annotations

from typing import Any

import sentry_sdk
from yaml import safe_load


def load_configuration_data(path: str, validators: dict[str, Any]) -> dict[str, Any]:
    """
    Loads a configuration file from the given path
    Returns an untyped dict of dicts
    """
    with sentry_sdk.start_span(op="load_and_validate") as span:
        span.set_tag("file", path)
        with open(path) as file:
            config = safe_load(file)
        assert isinstance(config, dict)
        validators[config["kind"]](config)
        span.description = config["name"]
        return config
