from __future__ import absolute_import

import logging
import os
from typing import Optional

import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.integrations.gnu_backtrace import GnuBacktraceIntegration

from snuba import settings
from snuba.clickhouse.native import ClickhousePool
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

def get_clickhouse_rw(dataset: Optional[str] = None) -> ClickhousePool:
    if not dataset:
        return ClickhousePool(settings.CLICKHOUSE_HOST, settings.CLICKHOUSE_PORT)

    host = settings.CLICKHOUSE_HOST_BY_DATASET.get(dataset, settings.CLICKHOUSE_HOST)
    port = settings.CLICKHOUSE_PORT_BY_DATASET.get(dataset, settings.CLICKHOUSE_PORT)
    return ClickhousePool(host, port)


def get_clickhouse_ro(dataset: Optional[str] = None) -> ClickhousePool:
    if not dataset:
        return ClickhousePool(
            settings.CLICKHOUSE_HOST,
            settings.CLICKHOUSE_PORT,
            client_settings={"readonly": True}
        )

    host = settings.CLICKHOUSE_HOST_BY_DATASET.get(dataset, settings.CLICKHOUSE_HOST)
    port = settings.CLICKHOUSE_PORT_BY_DATASET.get(dataset, settings.CLICKHOUSE_PORT)
    return ClickhousePool(host, port, client_settings={"readonly": True})


metrics = create_metrics("snuba")
