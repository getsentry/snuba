import os
import six


LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')

TESTING = False
DEBUG = True

PORT = 1218

DEFAULT_DATASET_TYPE = 'events'
DATASETS = {
    'events': 'snuba.datasets.DevEventsDataSet',
    'spans': 'snuba.datasets.DevSpansDataSet',
}
def get_dataset(name):
    assert name in DATASETS
    dataset = DATASETS[name]
    if isinstance(dataset, six.string_types):
        path = dataset.split('.')
        assert len(path) >= 2
        mod = __import__('.'.join(path[:-1]), fromlist=path[-1:])
        dataset = DATASETS[name] = getattr(mod, path[-1])()
    return dataset

# Clickhouse Options
CLICKHOUSE_SERVER = os.environ.get('CLICKHOUSE_SERVER', 'localhost:9000')
CLICKHOUSE_MAX_POOL_SIZE = 25

# Dogstatsd Options
DOGSTATSD_HOST = 'localhost'
DOGSTATSD_PORT = 8125

# Redis Options
USE_REDIS_CLUSTER = False
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

# Processor/Writer Options
DEFAULT_BROKERS = ['localhost:9093']
DEFAULT_MAX_BATCH_SIZE = 50000
DEFAULT_MAX_BATCH_TIME_MS = 2 * 1000
DEFAULT_QUEUED_MAX_MESSAGE_KBYTES = 50000
DEFAULT_QUEUED_MIN_MESSAGES = 20000
DISCARD_OLD_EVENTS = True

# TODO this belongs on a dataset.
KAFKA_TOPICS = {
    'raw-events': {
        'topic': 'events',
        'replication_factor': 1,
        'num_partitions': 1,
    },
    'replacements': {
        'topic': 'event-replacements',
        'replication_factor': 1,
        'num_partitions': 1,
    },
    'commit-log': {
        'topic': 'snuba-commit-log',
        'replication_factor': 1,
        'num_partitions': 1,
    },
}

# TODO datasets should also define their own retention
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
