from __future__ import annotations

from typing import Any, Mapping

import fastjsonschema
import sentry_sdk
from yaml import safe_load


def load_configuration_data(
    path: str, validation_schemas: Mapping[str, Any]
) -> dict[str, Any]:
    """
    Loads a configuration file from the given path
    Returns an untyped dict of dicts
    """
    with sentry_sdk.start_span(
        op="load_and_validate", description="Config File"
    ) as span:
        span.set_tag("file", path)
        file = open(path)
        config = safe_load(file)
        assert isinstance(config, dict)
        validator = fastjsonschema.compile(validation_schemas[config["kind"]])
        validator(config)
        return config
