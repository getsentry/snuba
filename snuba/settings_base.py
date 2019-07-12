import os
import six

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')

TESTING = False
DEBUG = True

PORT = 1218

DEFAULT_DATASET_NAME = 'events'
DATASET_MODE = 'local'

DISABLED_DATASETS = {}

KAFKA_CLUSTERS = {
    "default": {
        "brokers": ['localhost:9093'],
        "max_batch_size": 50000,
        "max_batch_time_ms": 2 * 1000,
        "queued_max_message_kbytes": 50000,
        "queued_min_messages": 20000,
    }
}

DATASETS = {
    "events": {
        "consumer": {
            "kafka_cluster_base": "default",
            "message_topic": {
                "name": "events",
                "consumer_group": "snuba-consumers",
            },
            "commit_log_topic": "snuba-commit-log",
            "replacement_topic": "event-replacements",
        }
    },
    "groupedmessage": {
        "consumer": {
            "kafka_cluster_base": "default",
            "message_topic": {
                "name": "cdc",
                "consumer_group": "snuba-consumers",
            },
            "commit_log_topic": None,
            "replacement_topic": None,
        }
    },
}

# Clickhouse Options
CLICKHOUSE_SERVER = os.environ.get('CLICKHOUSE_SERVER', 'localhost:9000')
CLICKHOUSE_MAX_POOL_SIZE = 25

# Dogstatsd Options
DOGSTATSD_HOST = 'localhost'
DOGSTATSD_PORT = 8125

# Redis Options
USE_REDIS_CLUSTER = False
REDIS_CLUSTER_STARTUP_NODES = None
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
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

# Convenience columns that evaluate to a bucketed time, the
# bucketing depends on the granularity parameter.
TIME_GROUP_COLUMNS = {
    'time': 'timestamp',
    'rtime': 'received'
}

# Processor/Writer Options
DISCARD_OLD_EVENTS = True

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
