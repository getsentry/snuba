import os
from collections import defaultdict


class dynamicdict(defaultdict):
    def __missing__(self, key):
        if self.default_factory:
            self.__setitem__(key, self.default_factory(key))
            return self[key]
        else:
            return super(dynamicdict, self).__missing__(key)


LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')

TESTING = False
DEBUG = True

PORT = 1218

# Clickhouse Options
CLICKHOUSE_SERVER = os.environ.get('CLICKHOUSE_SERVER', 'localhost:9000')
CLICKHOUSE_CLUSTER = None
CLICKHOUSE_TABLE = 'dev'
CLICKHOUSE_MAX_POOL_SIZE = 25

# Dogstatsd Options
DOGSTATSD_HOST = 'localhost'
DOGSTATSD_PORT = 8125

# Redis Options
USE_REDIS_CLUSTER = False
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 1

# Query Recording Options
RECORD_QUERIES = False
QUERIES_TOPIC = 'snuba-queries'

# Runtime Config Options
CONFIG_MEMOIZE_TIMEOUT = 10

# Sentry Options
SENTRY_DSN = None

# Snuba Options
TIME_GROUPS = dynamicdict(
    lambda sec: 'toDateTime(intDiv(toUInt32(timestamp), {0}) * {0})'.format(sec),
    {
        3600: 'toStartOfHour(timestamp)',
        60: 'toStartOfMinute(timestamp)',
        86400: 'toDate(timestamp)',
    }
)

TIME_GROUP_COLUMN = 'time'

# Processor/Writer Options
DEFAULT_BROKERS = ['localhost:9093']
DEFAULT_MAX_BATCH_SIZE = 50000
DEFAULT_MAX_BATCH_TIME_MS = 2 * 1000
DEFAULT_QUEUED_MAX_MESSAGE_KBYTES = 50000
DEFAULT_QUEUED_MIN_MESSAGES = 20000
DISCARD_OLD_EVENTS = True

# project_id and timestamp are included for queries, event_id is included for ReplacingMergeTree
DEFAULT_SAMPLE_EXPR = 'cityHash64(toString(event_id))'
DEFAULT_ORDER_BY = '(project_id, toStartOfDay(timestamp), %s)' % DEFAULT_SAMPLE_EXPR
DEFAULT_PARTITION_BY = '(toMonday(timestamp), if(equals(retention_days, 30), 30, 90))'
DEFAULT_VERSION_COLUMN = 'deleted'
DEFAULT_SHARDING_KEY = 'cityHash64(toString(event_id))'
DEFAULT_LOCAL_TABLE = 'sentry_local'
DEFAULT_DIST_TABLE = 'sentry_dist'
DEFAULT_RETENTION_DAYS = 90

RETENTION_OVERRIDES = {}

# the list of keys that will upgrade from a WHERE condition to a PREWHERE
PREWHERE_KEYS = ['project_id']
MAX_PREWHERE_CONDITIONS = 1

STATS_IN_RESPONSE = False

PAYLOAD_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

REPLACER_MAX_BLOCK_SIZE = 512
REPLACER_MAX_MEMORY_USAGE = 10 * (1024**3)  # 10GB
# TLL of Redis key that denotes whether a project had replacements
# run recently. Useful for decidig whether or not to add FINAL clause
# to queries.
REPLACER_KEY_TTL = 12 * 60 * 60
REPLACER_MAX_GROUP_IDS_TO_EXCLUDE = 256

TURBO_SAMPLE_RATE = 0.1
