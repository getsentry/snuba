from typing import Set
import os

TESTING = True

REDIS_DB = 2
STATS_IN_RESPONSE = True
CONFIG_MEMOIZE_TIMEOUT = 0

RECORD_QUERIES = True
USE_RESULT_CACHE = True
DISABLED_DATASETS: Set[str] = set()

SENTRY_DSN = os.getenv("SENTRY_DSN")
