from __future__ import annotations

import logging

from snuba import environment, state
from snuba.utils.metrics.wrapper import MetricsWrapper

from .appid import AppID
from .default import APPIDS as DEFAULT_APPIDS

metrics = MetricsWrapper(environment.metrics, "snuba.attribution")
logger = logging.getLogger("snuba.attribution")

APPIDS: dict[str, AppID] = {}
DEFAULT_APPID = AppID.from_dict(DEFAULT_APPIDS[0])
INVALID_APPID = AppID.from_dict(DEFAULT_APPIDS[1])
for obj in DEFAULT_APPIDS:
    app_id = AppID.from_dict(obj)
    APPIDS[app_id.key] = app_id


def get_app_id(key: str) -> AppID:
    if state.get_config("use_attribution", 0):
        app_id = APPIDS.get(key)
        if not app_id:
            metrics.increment("invalid_app_id", tags={"app_id": key})
            app_id = INVALID_APPID
        return app_id
    else:
        return DEFAULT_APPID


__all__ = (
    "AppID",
    "get_app_id",
)
