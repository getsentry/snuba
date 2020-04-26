from __future__ import absolute_import

import logging
import os
from typing import Optional

import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.integrations.gnu_backtrace import GnuBacktraceIntegration

from snuba import settings
from snuba.clickhouse.formatter import ClickhouseQueryFormatter
from snuba.clickhouse.native import ClickhousePool, NativeDriverReader
from snuba.reader import Reader
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
        integrations=[FlaskIntegration(), GnuBacktraceIntegration()],
        release=os.getenv("SNUBA_RELEASE"),
    )


clickhouse_rw = ClickhousePool(settings.CLICKHOUSE_HOST, settings.CLICKHOUSE_PORT)
clickhouse_ro = ClickhousePool(
    settings.CLICKHOUSE_HOST,
    settings.CLICKHOUSE_PORT,
    client_settings={"readonly": True},
)

metrics = create_metrics("snuba")

reader: Reader[ClickhouseQueryFormatter] = NativeDriverReader(clickhouse_ro)
