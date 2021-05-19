import os
from typing import Set

TESTING = True

REDIS_DB = 2
STATS_IN_RESPONSE = True
CONFIG_MEMOIZE_TIMEOUT = 0

RECORD_QUERIES = True
USE_RESULT_CACHE = True

SENTRY_DSN = os.getenv("SENTRY_DSN")

SKIPPED_MIGRATION_GROUPS: Set[str] = {"metrics"}
