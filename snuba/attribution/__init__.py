from __future__ import annotations

from datetime import datetime

from snuba import environment
from snuba.utils.metrics.wrapper import MetricsWrapper

from .appid import AppID

metrics = MetricsWrapper(environment.metrics, "snuba.attribution")


DEFAULT_APPID = AppID("default", "evanh", datetime(2022, 3, 24))
INVALID_APPID = AppID("invalid", "evanh", datetime(2022, 3, 25))


def get_app_id(app_id: str) -> AppID:
    if not app_id:
        return INVALID_APPID

    return AppID(app_id)


__all__ = (
    "AppID",
    "get_app_id",
)
