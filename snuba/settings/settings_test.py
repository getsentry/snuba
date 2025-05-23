import os
from typing import Set

TESTING = True

REDIS_DB = 2
STATS_IN_RESPONSE = True
CONFIG_MEMOIZE_TIMEOUT = 0

RECORD_QUERIES = True

SENTRY_DSN = os.getenv("SENTRY_DSN")

SKIPPED_MIGRATION_GROUPS: Set[str] = set()
SUPPORTED_STATES: Set[str] = {
    "deprecate",
    "limited",
    "partial",
    "complete",
    "experimental",
}
ENABLE_DEV_FEATURES = True

# Sometimes we want the raw structure of an expression
# rather than the pretty formatted one. If you're debugging
# something and you're at your wit's end, try setting this to False
# to explore the unrefined Expression structure
PRETTY_FORMAT_EXPRESSIONS = os.environ.get("PRETTY_FORMAT_EXPRESSIONS", "1") == "1"

# By default, allocation policies won't block requests from going through in a production
# environment to not cause incidents unnecessarily. But if you're testing the policy, it
# should fail on bad code
RAISE_ON_ALLOCATION_POLICY_FAILURES = True
RAISE_ON_ROUTING_STRATEGY_FAILURES = True
RAISE_ON_READTHROUGH_CACHE_REDIS_FAILURES = True

# override replacer threshold to write to redis every time a replacement message is consumed
REPLACER_PROCESSING_TIMEOUT_THRESHOLD = 0  # ms

# Set enforce retention to true for tests
ENFORCE_RETENTION = True

ADMIN_ALLOWED_PROD_PROJECTS = [1, 11276]
ADMIN_ALLOWED_ORG_IDS = [123]

REDIS_CLUSTERS = {
    key: {
        "use_redis_cluster": os.environ.get("USE_REDIS_CLUSTER", "0") != "0",
        "cluster_startup_nodes": None,
        "host": os.environ.get("REDIS_HOST", "127.0.0.1"),
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
        (9, "admin_auth"),
        (10, "manual_jobs"),
    ]
}
VALIDATE_DATASET_YAMLS_ON_STARTUP = True
