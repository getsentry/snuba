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
from sentry_sdk.integrations.threading import ThreadingIntegration
from structlog.processors import JSONRenderer
from structlog.types import EventDict
from structlog_sentry import SentryProcessor

from snuba import settings
from snuba.utils.metrics.util import create_metrics


def add_severity_attribute(
    logger: logging.Logger, method_name: str, event_dict: EventDict
) -> EventDict:
    """
    Set the severity attribute for Google Cloud logging ingestion
    """
    if method_name == "warn":
        method_name = "warning"

    event_dict["severity"] = method_name
    event_dict["level"] = method_name

    return event_dict


def drop_level(
    logger: logging.Logger, method_name: str, event_dict: EventDict
) -> EventDict:
    """
    The "SentryProcessor" requires a `level` field but we're already
    emitting it as `severity` for Google Cloud, so we delete the duplication
    if SentryProcessor is done
    """
    del event_dict["level"]

    return event_dict


def setup_logging(level: Optional[str] = None) -> None:
    if level is None:
        level = settings.LOG_LEVEL

    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=settings.LOG_FORMAT,
        force=True,
    )

    structlog.configure(
        cache_logger_on_first_use=True,
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        processors=[
            add_severity_attribute,
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            SentryProcessor(),
            drop_level,
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
            ThreadingIntegration(propagate_hub=True),
        ],
        release=os.getenv("SNUBA_RELEASE"),
        traces_sample_rate=settings.SENTRY_TRACE_SAMPLE_RATE,
    )


metrics = create_metrics(
    "snuba",
    tags=None,
    sample_rates=settings.DOGSTATSD_SAMPLING_RATES,
)
