from __future__ import absolute_import

import logging
import os
from typing import Optional

import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.integrations.gnu_backtrace import GnuBacktraceIntegration
from sentry_sdk.integrations.logging import LoggingIntegration

from snuba import settings
from snuba.util import create_metrics


def setup_logging(level: Optional[str] = None) -> None:
    if level is None:
        level = settings.LOG_LEVEL

    logging.basicConfig(
        level=getattr(logging, level.upper()), format=settings.LOG_FORMAT,
    )


def setup_sentry() -> None:
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        integrations=[
            FlaskIntegration(),
            GnuBacktraceIntegration(),
            LoggingIntegration(event_level=logging.WARNING),
        ],
        release=os.getenv("SNUBA_RELEASE"),
    )


metrics = create_metrics("snuba")
