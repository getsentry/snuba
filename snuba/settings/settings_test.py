import os
from datetime import timedelta
from typing import Set

TESTING = True

REDIS_DB = 2
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

OPTIMIZE_PARALLEL_MAX_JITTER_MINUTES = 0

REDIS_CLUSTERS = {
    key: {
        "use_redis_cluster": os.environ.get("USE_REDIS_CLUSTER", "0") != "0",
        "cluster_startup_nodes": None,
        "host": os.environ.get("REDIS_HOST", "localhost"),
        "port": int(os.environ.get("REDIS_PORT", 6379)),
        "password": os.environ.get("REDIS_PASSWORD"),
        "db": i,
        "reinitialize_steps": 10,
    }
    for i, key in [
        (2, "cache"),
        (3, "rate_limiter"),
        (4, "subscription_store"),
        (5, "replacements_store"),
        (6, "config"),
        (7, "dlq"),
        (8, "optimize"),
    ]
}
