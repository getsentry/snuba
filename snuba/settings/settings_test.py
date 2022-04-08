import os
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
