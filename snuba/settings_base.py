import os

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

TESTING = False
DEBUG = True

PORT = 1218

DEFAULT_DATASET_NAME = "events"
DISABLED_DATASETS = {}
DATASET_MODE = "local"

# Clickhouse Options
# TODO: Warn about using `CLICKHOUSE_SERVER`, users should use the new settings instead.
[default_clickhouse_host, default_clickhouse_port] = os.environ.get(
    "CLICKHOUSE_SERVER", "localhost:9000"
).split(":", 1)
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", default_clickhouse_host)
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", default_clickhouse_port))
CLICKHOUSE_HTTP_PORT = int(os.environ.get("CLICKHOUSE_HTTP_PORT", 8123))
CLICKHOUSE_MAX_POOL_SIZE = 25

# Dogstatsd Options
DOGSTATSD_HOST = "localhost"
DOGSTATSD_PORT = 8125

# Redis Options
USE_REDIS_CLUSTER = False
REDIS_CLUSTER_STARTUP_NODES = None
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = 6379
REDIS_PASSWORD = None
REDIS_DB = 1

# Query Recording Options
RECORD_QUERIES = False
QUERIES_TOPIC = "snuba-queries"

# Runtime Config Options
CONFIG_MEMOIZE_TIMEOUT = 10

# Sentry Options
SENTRY_DSN = None

# Snuba Options

SNAPSHOT_LOAD_PRODUCT = "snuba"

SNAPSHOT_CONTROL_TOPIC_INIT_TIMEOUT = 30
BULK_CLICKHOUSE_BUFFER = 10000

# Processor/Writer Options
DEFAULT_BROKERS = ["localhost:9092"]
DEFAULT_DATASET_BROKERS = {}

DEFAULT_MAX_BATCH_SIZE = 50000
DEFAULT_MAX_BATCH_TIME_MS = 2 * 1000
DEFAULT_QUEUED_MAX_MESSAGE_KBYTES = 10000
DEFAULT_QUEUED_MIN_MESSAGES = 10000
DISCARD_OLD_EVENTS = True
CLICKHOUSE_HTTP_CHUNK_SIZE = 1

DEFAULT_RETENTION_DAYS = 90
RETENTION_OVERRIDES = {}

MAX_PREWHERE_CONDITIONS = 1

STATS_IN_RESPONSE = False

PAYLOAD_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

REPLACER_MAX_BLOCK_SIZE = 512
REPLACER_MAX_MEMORY_USAGE = 10 * (1024 ** 3)  # 10GB
# TLL of Redis key that denotes whether a project had replacements
# run recently. Useful for decidig whether or not to add FINAL clause
# to queries.
REPLACER_KEY_TTL = 12 * 60 * 60
REPLACER_MAX_GROUP_IDS_TO_EXCLUDE = 256

TURBO_SAMPLE_RATE = 0.1

PROJECT_STACKTRACE_BLACKLIST = set()

# Number of queries each subscription consumer can run concurrently.
SUBSCRIPTIONS_MAX_CONCURRENT_QUERIES = 10
