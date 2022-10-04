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
        "use_redis_cluster": False,
        "cluster_startup_nodes": None,
        "host": "localhost",
        "port": 6379,
        "password": None,
        "db": i + 1,
        "reinitialize_steps": 10,
    }
    for i, key in enumerate(
        ["cache", "rate_limiter", "subscription_store", "replacements_store", "misc"]
    )
}
