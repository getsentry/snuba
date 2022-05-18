import os
from datetime import datetime, timedelta
from typing import Any, Mapping, MutableMapping, Optional, Sequence, Set

from snuba.settings.validation import validate_settings

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s %(message)s"

TESTING = False
DEBUG = True

HOST = "0.0.0.0"
PORT = 1218

ADMIN_HOST = os.environ.get("ADMIN_HOST", "0.0.0.0")
ADMIN_PORT = int(os.environ.get("ADMIN_PORT", 1219))
ADMIN_URL = os.environ.get("ADMIN_URL", "http://localhost:1219")

ADMIN_AUTH_PROVIDER = "NOOP"

ENABLE_DEV_FEATURES = os.environ.get("ENABLE_DEV_FEATURES", False)

DEFAULT_DATASET_NAME = "events"
DISABLED_DATASETS: Set[str] = set()

# Clickhouse Options
CLICKHOUSE_MAX_POOL_SIZE = 25

CLUSTERS: Sequence[Mapping[str, Any]] = [
    {
        "host": os.environ.get("CLICKHOUSE_HOST", "localhost"),
        "port": int(os.environ.get("CLICKHOUSE_PORT", 9000)),
        "user": os.environ.get("CLICKHOUSE_USER", "default"),
        "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
        "database": os.environ.get("CLICKHOUSE_DATABASE", "default"),
        "http_port": int(os.environ.get("CLICKHOUSE_HTTP_PORT", 8123)),
        "storage_sets": {
            "cdc",
            "discover",
            "events",
            "events_ro",
            "metrics",
            "migrations",
            "outcomes",
            "querylog",
            "sessions",
            "transactions",
            "transactions_ro",
            "transactions_v2",
            "errors_v2",
            "errors_v2_ro",
            "profiles",
            "experimental",
        },
        "single_node": True,
    },
]


# Dogstatsd Options
DOGSTATSD_HOST = None
DOGSTATSD_PORT = None
DOGSTATSD_SAMPLING_RATES = {
    "subscriptions.receive_latency": 0.1,
    "subscriptions.process_message": 0.1,
    "subscriptions.executor.latency": 0.1,
    "metrics.processor.set.size": 0.1,
    "metrics.processor.distribution.size": 0.1,
}

CLICKHOUSE_READONLY_USER = os.environ.get("CLICKHOUSE_READONLY_USER", "default")
CLICKHOUSE_READONLY_PASSWORD = os.environ.get("CLICKHOUSE_READONLY_PASS", "")

CLICKHOUSE_TRACE_USER = os.environ.get("CLICKHOUSE_TRACE_USER", "default")
CLICKHOUSE_TRACE_PASSWORD = os.environ.get("CLICKHOUSE_TRACE_PASS", "")

# Redis Options
USE_REDIS_CLUSTER = os.environ.get("USE_REDIS_CLUSTER", "0") != "0"

REDIS_CLUSTER_STARTUP_NODES = None
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")
REDIS_DB = int(os.environ.get("REDIS_DB", 1))
REDIS_INIT_MAX_RETRIES = 3

USE_RESULT_CACHE = True

# Query Recording Options
RECORD_QUERIES = False

# Runtime Config Options
CONFIG_MEMOIZE_TIMEOUT = 10

# Sentry Options
SENTRY_DSN = None

# Snuba Admin Options
SLACK_API_TOKEN = os.environ.get("SLACK_API_TOKEN")
SNUBA_SLACK_CHANNEL_ID = os.environ.get("SNUBA_SLACK_CHANNEL_ID")


# Query Specifics
CLUSTERS = [
    {  # Proxied via Envoy Sidecar
        "host": "127.0.0.1",
        "port": 9000,
        "http_port": 8123,
        "storage_sets": {"discover", "migrations"},
        "single_node": True,
        "database": "default",
    },
    {  # Proxied via Envoy Sidecar
        # Defined separately from the interface to the other clusters so that
        # the replacer can address individual storage nodes.
        "host": "127.0.0.1",
        "port": 9000,
        "http_port": 8123,
        "storage_sets": {"events"},
        "single_node": True,
        "cluster_name": "snuba_errors",
        "distributed_cluster_name": None,
        "database": "default",
    },
    {  # Proxied via Envoy Sidecar
        # Defined separately as query/dist nodes must match events for joins
        "host": "127.0.0.1",
        "port": 9000,
        "http_port": 8123,
        "storage_sets": {"events_ro"},
        "database": "default",
        "single_node": True,
        "cluster_name": "snuba_errors_ro",
        "distributed_cluster_name": None,
    },
    {
        "host": "127.0.0.1",
        "port": 9000,
        "http_port": 8123,
        "storage_sets": {"transactions", "transactions_ro"},
        "single_node": True,
        "database": "default",
        "cluster_name": "snuba_transactions",
        "distributed_cluster_name": None,
    },
    {
        "host": "127.0.0.1",
        "port": 9000,
        "http_port": 8123,
        "storage_sets": {"outcomes"},
        "single_node": True,
        "database": "default",
        "cluster_name": "snuba_outcomes",
        "distributed_cluster_name": None,
    },
    {
        "host": "127.0.0.1",
        "port": 9000,
        "http_port": 8123,
        "storage_sets": {"sessions"},
        "single_node": True,
        "cluster_name": "snuba_sessions",
        "database": "default",
        "distributed_cluster_name": None,
    },
    {  # Proxied via Envoy Sidecar
        # Defined separately as query/dist nodes must match events for joins
        "host": "127.0.0.1",
        "port": 9000,
        "http_port": 8123,
        "storage_sets": {"cdc"},
        "single_node": True,
        "cluster_name": "cdc",
        "database": "default",
        "distributed_cluster_name": None,
    },
    {  # Proxied via Envoy Sidecar
        # Defined separately (with different ports) so proxy can talk
        # directly to storage nodes - no query nodes needed
        "host": "127.0.0.1",
        "port": 9000,
        "http_port": 8124,
        "storage_sets": {"metrics"},
        "single_node": True,
        "cluster_name": "snuba-metrics",
        "database": "default",
        "distributed_cluster_name": None,
    },
    {  # Singular host for query logging.
        "host": "localhost",
        "port": 9000,
        "http_port": 8123,
        "storage_sets": {"querylog"},
        "single_node": True,
        "database": "default",
    },
    {  # Proxied via Envoy Sidecar to the Tiger transactions cluster.
        "host": "127.0.0.1",
        "port": 9000,
        "http_port": 8133,
        "storage_sets": {"transactions_v2"},
        "single_node": True,
        "database": "default",
        "cluster_name": "snuba-transactions-tiger",
        "distributed_cluster_name": None,
        "cache_partition_id": "tiger_transactions",
    },
    {  # Proxied via Envoy Sidecar to the Tiger errors cluster.
        "host": "127.0.0.1",
        "port": 9000,
        "http_port": 8133,
        "storage_sets": {"errors_v2"},
        "single_node": True,
        "cluster_name": "snuba-errors-tiger",
        "distributed_cluster_name": None,
        "database": "default",
        "cache_partition_id": "tiger_errors",
    },
    {  # Proxied via Envoy Sidecar to the Tiger errors readonly cluster.
        "host": "127.0.0.1",
        "port": 9000,
        "http_port": 8133,
        "storage_sets": {"errors_v2_ro"},
        "database": "default",
        "single_node": True,
        "cluster_name": "snuba-errors-ro-tiger",
        "distributed_cluster_name": None,
        "cache_partition_id": "tiger_errors",
    },
    {  # Proxied via Envoy Sidecar
        # Defined separately (with different ports) so proxy can talk
        # directly to storage nodes - no query nodes needed
        "host": "127.0.0.1",
        "port": 9000,
        "http_port": 8134,
        "storage_sets": {"profiles"},
        "single_node": True,
        "database": "default",
        "cluster_name": "snuba-profiles",
        "distributed_cluster_name": None,
    },
]

# Snuba Options

SNAPSHOT_LOAD_PRODUCT = "snuba"

SNAPSHOT_CONTROL_TOPIC_INIT_TIMEOUT = 30
BULK_CLICKHOUSE_BUFFER = 10000
BULK_BINARY_LOAD_CHUNK = 2**22  # 4 MB

# Processor/Writer Options

BROKER_CONFIG: Mapping[str, Any] = {
    # See snuba/utils/streams/backends/kafka.py for the supported options
    "bootstrap.servers": os.environ.get("DEFAULT_BROKERS", "localhost:9092"),
}

# Mapping of default Kafka topic name to custom names
KAFKA_TOPIC_MAP: Mapping[str, str] = {
    # TODO: Remove once we are done splitting transactions from the shared events topic
    "transactions": "events",
    "snuba-transactions-commit-log": "snuba-commit-log",
}

# Mapping of default Kafka topic name to broker config
KAFKA_BROKER_CONFIG: Mapping[str, Mapping[str, Any]] = {}

DEFAULT_MAX_BATCH_SIZE = 50000
DEFAULT_MAX_BATCH_TIME_MS = 2 * 1000
DEFAULT_QUEUED_MAX_MESSAGE_KBYTES = 10000
DEFAULT_QUEUED_MIN_MESSAGES = 10000
DISCARD_OLD_EVENTS = True
CLICKHOUSE_HTTP_CHUNK_SIZE = 8192
HTTP_WRITER_BUFFER_SIZE = 1

# Retention related settings
ENFORCE_RETENTION: bool = False
LOWER_RETENTION_DAYS = 30
DEFAULT_RETENTION_DAYS = 90
RETENTION_OVERRIDES: Mapping[int, int] = {}

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
REPLACER_IMMEDIATE_OPTIMIZE = False
REPLACER_PROCESSING_TIMEOUT_THRESHOLD = 2 * 60  # 2 minutes in seconds
REPLACER_PROCESSING_TIMEOUT_THRESHOLD_KEY_TTL = 60 * 60  # 1 hour in seconds

TURBO_SAMPLE_RATE = 0.1

PROJECT_STACKTRACE_BLACKLIST: Set[int] = set()
PRETTY_FORMAT_EXPRESSIONS = True

TOPIC_PARTITION_COUNTS: Mapping[str, int] = {}  # (topic name, # of partitions)

COLUMN_SPLIT_MIN_COLS = 6
COLUMN_SPLIT_MAX_LIMIT = 1000
COLUMN_SPLIT_MAX_RESULTS = 5000

# Migrations in skipped groups will not be run
SKIPPED_MIGRATION_GROUPS: Set[str] = {"querylog", "profiles"}

MAX_RESOLUTION_FOR_JITTER = 60

# These contexts will not be stored in the transactions table
# Example: {123: {"context1", "context2"}}
# where 123 is the project id.
TRANSACT_SKIP_CONTEXT_STORE: Mapping[int, Set[str]] = {}

# Map the Zookeeper path for the replicated merge tree to something else
CLICKHOUSE_ZOOKEEPER_OVERRIDE: Mapping[str, str] = {}

# Enable Sentry Metrics (used for the snuba metrics consumer)
ENABLE_SENTRY_METRICS_DEV = os.environ.get("ENABLE_SENTRY_METRICS_DEV", False)

# Metric Alerts Subscription Options
ENABLE_SESSIONS_SUBSCRIPTIONS = os.environ.get("ENABLE_SESSIONS_SUBSCRIPTIONS", False)
ENABLE_METRICS_SUBSCRIPTIONS = os.environ.get("ENABLE_METRICS_SUBSCRIPTIONS", False)

# Use the subscriptions pipeline in devserver
ENABLE_NEW_SUBSCRIPTIONS = os.environ.get("ENABLE_NEW_SUBSCRIPTIONS", False)

# Subscriptions scheduler buffer size
SUBSCRIPTIONS_DEFAULT_BUFFER_SIZE = 10000
SUBSCRIPTIONS_ENTITY_BUFFER_SIZE: Mapping[str, int] = {}  # (entity name, buffer size)

TRANSACTIONS_DIRECT_TO_READONLY_REFERRERS: Set[str] = set()

# Used for migrating to/from writing metrics directly to aggregate tables
# rather than using materialized views
WRITE_METRICS_AGG_DIRECTLY = False
ENABLED_MATERIALIZATION_VERSION = 4

# Enable profiles ingestion
ENABLE_PROFILES_CONSUMER = os.environ.get("ENABLE_PROFILES_CONSUMER", False)

# Place the actual time we start ingesting on the new version.
ERRORS_UPGRADE_BEGINING_OF_TIME: Optional[datetime] = datetime(2022, 2, 23, 0, 0, 0)
TRANSACTIONS_UPGRADE_BEGINING_OF_TIME: Optional[datetime] = datetime(
    2022, 2, 18, 0, 0, 0
)

MAX_ROWS_TO_CHECK_FOR_SIMILARITY = 1000

OPTIMIZE_JOB_CUTOFF_TIME = timedelta(hours=23)


def _load_settings(obj: MutableMapping[str, Any] = locals()) -> None:
    """Load settings from the path provided in the SNUBA_SETTINGS environment
    variable if provided. Users can provide a short name like `test` that will
    be expanded to `settings_test.py` in the main Snuba directory, or they can
    provide a full absolute path such as `/foo/bar/my_settings.py`."""

    import importlib
    import importlib.util
    import os

    settings = os.environ.get("SNUBA_SETTINGS")

    if settings:
        if settings.startswith("/"):
            if not settings.endswith(".py"):
                settings += ".py"

            # Code below is adapted from https://stackoverflow.com/a/41595552/90297S
            settings_spec = importlib.util.spec_from_file_location(
                "snuba.settings.custom", settings
            )
            settings_module = importlib.util.module_from_spec(settings_spec)
            assert isinstance(settings_spec.loader, importlib.abc.Loader)
            settings_spec.loader.exec_module(settings_module)
        else:
            module_format = (
                ".%s" if settings.startswith("settings_") else ".settings_%s"
            )
            settings_module = importlib.import_module(
                module_format % settings, "snuba.settings"
            )

        for attr in dir(settings_module):
            if attr.isupper():
                obj[attr] = getattr(settings_module, attr)


_load_settings()
validate_settings(locals())
