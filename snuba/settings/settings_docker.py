import os
from typing import Mapping

from snuba.redis_multi.configuration import (
    ClusterFunction,
    ConnectionDescriptor,
    NodeDescriptor,
)

env = os.environ.get

DEBUG = env("DEBUG", "0").lower() in ("1", "true")

DEFAULT_RETENTION_DAYS = env("SENTRY_EVENT_RETENTION_DAYS", 90)

REDIS_HOST = env("REDIS_HOST", "localhost")
REDIS_PORT = int(env("REDIS_PORT", 6379))
REDIS_PASSWORD = env("REDIS_PASSWORD")
REDIS_DB = int(env("REDIS_DB", 1))

# Dogstatsd Options
DOGSTATSD_HOST = env("DOGSTATSD_HOST")
DOGSTATSD_PORT = env("DOGSTATSD_PORT")

SENTRY_DSN = env("SENTRY_DSN")

REDIS_SINGLE_NODE_DESCRIPTOR = NodeDescriptor(
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD
)
REDIS_CLUSTER_MAP: Mapping[ClusterFunction, ConnectionDescriptor] = {
    ClusterFunction.CACHE: REDIS_SINGLE_NODE_DESCRIPTOR,
    ClusterFunction.RATE_LIMITING: REDIS_SINGLE_NODE_DESCRIPTOR,
    ClusterFunction.REPLACEMENT_STORAGE: REDIS_SINGLE_NODE_DESCRIPTOR,
    ClusterFunction.SUBSCRIPTION_STORAGE: REDIS_SINGLE_NODE_DESCRIPTOR,
}
