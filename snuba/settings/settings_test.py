import os
from datetime import timedelta
from typing import Mapping, Set

from snuba.redis_multi.configuration import (
    ClusterFunction,
    ConnectionDescriptor,
    NodeDescriptor,
)
from snuba.settings import REDIS_HOST, REDIS_PASSWORD, REDIS_PORT, USE_REDIS_CLUSTER

TESTING = True

REDIS_DB = int(os.environ.get("REDIS_DB", 2))
STATS_IN_RESPONSE = True
CONFIG_MEMOIZE_TIMEOUT = 0

RECORD_QUERIES = True
USE_RESULT_CACHE = True

SENTRY_DSN = os.getenv("SENTRY_DSN")

SKIPPED_MIGRATION_GROUPS: Set[str] = set()
ENABLE_DEV_FEATURES = True

# Sometimes we want the raw structure of an expression
# rather than the pretty formatted one. If you're debugging
# something and you're at your wit's end, try setting this to False
# to explore the unrefined Expression structure
PRETTY_FORMAT_EXPRESSIONS = True

# override replacer threshold to write to redis every time a replacement message is consumed
REPLACER_PROCESSING_TIMEOUT_THRESHOLD = 0  # ms

# Set enforce retention to true for tests
ENFORCE_RETENTION = True

# Ignore optimize job cut off time for tests
OPTIMIZE_JOB_CUTOFF_TIME = timedelta(days=1)

REDIS_SINGLE_NODE_DESCRIPTOR = NodeDescriptor(
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD
)
REDIS_CLUSTER_MAP: Mapping[ClusterFunction, ConnectionDescriptor] = (
    {
        ClusterFunction.CACHE: (REDIS_SINGLE_NODE_DESCRIPTOR,),
        ClusterFunction.RATE_LIMITING: (REDIS_SINGLE_NODE_DESCRIPTOR,),
        ClusterFunction.REPLACEMENT_STORAGE: (REDIS_SINGLE_NODE_DESCRIPTOR,),
        ClusterFunction.SUBSCRIPTION_STORAGE: (REDIS_SINGLE_NODE_DESCRIPTOR,),
    }
    if USE_REDIS_CLUSTER
    else {
        ClusterFunction.CACHE: REDIS_SINGLE_NODE_DESCRIPTOR,
        ClusterFunction.RATE_LIMITING: REDIS_SINGLE_NODE_DESCRIPTOR,
        ClusterFunction.REPLACEMENT_STORAGE: REDIS_SINGLE_NODE_DESCRIPTOR,
        ClusterFunction.SUBSCRIPTION_STORAGE: REDIS_SINGLE_NODE_DESCRIPTOR,
    }
)
