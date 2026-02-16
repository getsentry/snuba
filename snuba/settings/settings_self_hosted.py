import os
from typing import Set

from snuba.utils.metrics.addr_config import get_statsd_addr

env = os.environ.get

ALLOCATION_POLICY_ENABLED = False
DEBUG = env("DEBUG", "0").lower() in ("1", "true")

DEFAULT_RETENTION_DAYS = int(env("SENTRY_EVENT_RETENTION_DAYS", 90))
VALID_RETENTION_DAYS = set([int(env("SENTRY_EVENT_RETENTION_DAYS", 90)), 30, 60])
LOWER_RETENTION_DAYS = min(DEFAULT_RETENTION_DAYS, 30)

API_WORKERS = int(env("SNUBA_API_WORKERS", 1))
API_THREADS = int(env("SNUBA_API_THREADS", 8))
API_WORKERS_LIFETIME = (
    int(env("SNUBA_API_WORKERS_LIFETIME")) if env("SNUBA_API_WORKERS_LIFETIME") else None
)
API_WORKERS_MAX_RSS = (
    int(env("SNUBA_API_WORKERS_MAX_RSS")) if env("SNUBA_API_WORKERS_MAX_RSS") else None
)

REDIS_HOST = env("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(env("REDIS_PORT", 6379))
REDIS_PASSWORD = env("REDIS_PASSWORD")
REDIS_DB = int(env("REDIS_DB", 1))
USE_REDIS_CLUSTER = False

# Dogstatsd Options
DOGSTATSD_HOST, DOGSTATSD_PORT = get_statsd_addr()

# Dataset readiness states supported in this environment
SUPPORTED_STATES: Set[str] = {"deprecate", "complete"}
READINESS_STATE_FAIL_QUERIES: bool = False


SENTRY_DSN = env("SENTRY_DSN")

VALIDATE_DATASET_YAMLS_ON_STARTUP = True
