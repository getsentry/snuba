from __future__ import annotations

from datetime import datetime

from snuba import environment
from snuba.utils.metrics.wrapper import MetricsWrapper

from .appid import AppID

metrics = MetricsWrapper(environment.metrics, "snuba.attribution")


DEFAULT_APPID = AppID("default", "sns", datetime(2022, 3, 24))
INVALID_APPID = AppID("invalid", "sns", datetime(2022, 3, 25))


def get_app_id(app_id: str) -> AppID:
    return AppID(app_id) if app_id else INVALID_APPID


__all__ = (
    "AppID",
    "get_app_id",
)
