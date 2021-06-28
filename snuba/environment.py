from __future__ import absolute_import

import logging
import os
from typing import Any, Mapping, Optional

import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.integrations.gnu_backtrace import GnuBacktraceIntegration
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.integrations.redis import RedisIntegration

from snuba import settings
from snuba.util import create_metrics


def setup_logging(level: Optional[str] = None) -> None:
    if level is None:
        level = settings.LOG_LEVEL

    logging.basicConfig(
        level=getattr(logging, level.upper()), format=settings.LOG_FORMAT,
    )


def traces_sampler(sampling_context: Mapping[str, Any]) -> Any:
    return sampling_context["parent_sampled"] or False


def setup_sentry() -> None:
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        integrations=[
            FlaskIntegration(),
            GnuBacktraceIntegration(),
            LoggingIntegration(event_level=logging.WARNING),
            RedisIntegration(),
        ],
        release=os.getenv("SNUBA_RELEASE"),
        traces_sampler=traces_sampler,
    )


metrics = create_metrics(
    "snuba", tags=None, sample_rates=settings.DOGSTATSD_SAMPLING_RATES,
)
