from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass

import simplejson as json

from snuba import environment
from snuba.environment import setup_logging
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "api")
setup_logging(None)
logger = logging.getLogger("snuba.health")


@dataclass
class HealthInfo:
    body: str
    status: int
    content_type: dict[str, str]


def get_health_info(thorough: bool | str) -> HealthInfo:
    start = time.time()

    body: Mapping[str, str | bool] = {"status": "ok"}
    payload = json.dumps(body)

    metrics.timing(
        "healthcheck.latency",
        time.time() - start,
        tags={"thorough": str(thorough)},
    )

    return HealthInfo(
        body=payload,
        status=200,
        content_type={"Content-Type": "application/json"},
    )
