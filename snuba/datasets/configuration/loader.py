from __future__ import annotations

from typing import Any

from sentry_sdk import traces
from yaml import safe_load

from snuba import settings


def load_configuration_data(path: str, validators: dict[str, Any]) -> dict[str, Any]:
    """
    Loads a configuration file from the given path
    Returns an untyped dict of dicts
    """
    with traces.start_span(
        name="load_and_validate",
        attributes={"sentry.op": "load_and_validate", "file": path},
    ) as span:
        with open(path) as file:
            config = safe_load(file)
        assert isinstance(config, dict)
        if settings.VALIDATE_DATASET_YAMLS_ON_STARTUP:
            validators[config["kind"]](config)
        span.name = config["name"]
        return config
