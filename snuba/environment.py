from __future__ import absolute_import

import logging
import os
from typing import Optional

import sentry_sdk
import structlog
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.integrations.gnu_backtrace import GnuBacktraceIntegration
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.integrations.redis import RedisIntegration
from structlog.processors import JSONRenderer
from structlog.types import EventDict

from snuba import settings
from snuba.util import create_metrics


def add_severity_attribute(
    logger: logging.Logger, method_name: str, event_dict: EventDict
) -> EventDict:
    if method_name == "warn":
        # The stdlib has an alias
        method_name = "warning"

    event_dict["severity"] = method_name

    return event_dict


def setup_logging(level: Optional[str] = None) -> None:
    if level is None:
        level = settings.LOG_LEVEL

    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=settings.LOG_FORMAT,
    )

    structlog.configure(
        cache_logger_on_first_use=True,
        processors=[
            add_severity_attribute,
            structlog.contextvars.merge_contextvars,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            JSONRenderer(),
        ],
    )


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
        traces_sample_rate=settings.SENTRY_TRACE_SAMPLE_RATE,
    )


metrics = create_metrics(
    "snuba",
    tags=None,
    sample_rates=settings.DOGSTATSD_SAMPLING_RATES,
)
