from __future__ import annotations

import os
from pathlib import Path
from typing import (
    Any,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypedDict,
)

from snuba.settings.validation import validate_settings

# All settings must be uppercased, have a default value and cannot start with _.
# The Rust consumer relies on this to create a JSON file from the evaluated settings
# upon startup with any variables in this module that conform to this format.
# Similarly, variables that are not supposed to be settings for override/export should not
# follow this convention otherwise they will be included in the JSON.
# Sets will be converted to arrays.

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s %(message)s"

TESTING = False
DEBUG = True

HOST = "0.0.0.0"
PORT = 1218

##################
# Admin Settings #
##################

ADMIN_HOST = os.environ.get("ADMIN_HOST", "0.0.0.0")
ADMIN_PORT = int(os.environ.get("ADMIN_PORT", 1219))
ADMIN_URL = os.environ.get("ADMIN_URL", "http://127.0.0.1:1219")

ADMIN_AUTH_PROVIDER = os.environ.get("ADMIN_AUTH_PROVIDER", "NOOP")
ADMIN_AUTH_JWT_AUDIENCE = os.environ.get("ADMIN_AUTH_JWT_AUDIENCE", "")

# file path to the IAM policy file which contains the roles
ADMIN_IAM_POLICY_FILE = os.environ.get(
    "ADMIN_IAM_POLICY_FILE",
    f"{Path(__file__).parent.parent.as_posix()}/admin/iam_policy/iam_policy.json",
)

ADMIN_FRONTEND_DSN = os.environ.get("ADMIN_FRONTEND_DSN", "")
ADMIN_TRACE_SAMPLE_RATE = float(os.environ.get("ADMIN_TRACE_SAMPLE_RATE", 1.0))
ADMIN_REPLAYS_SAMPLE_RATE = float(os.environ.get("ADMIN_REPLAYS_SAMPLE_RATE", 0.1))
ADMIN_REPLAYS_SAMPLE_RATE_ON_ERROR = float(
    os.environ.get("ADMIN_REPLAYS_SAMPLE_RATE_ON_ERROR", 1.0)
)


ADMIN_ALLOWED_PROD_PROJECTS: Sequence[int] = []
ADMIN_ROLES_REDIS_TTL = 600

######################
# End Admin Settings #
######################

MAX_MIGRATIONS_REVERT_TIME_WINDOW_HRS = 24

ENABLE_DEV_FEATURES = os.environ.get("ENABLE_DEV_FEATURES", False)

DEFAULT_DATASET_NAME = "events"
DISABLED_ENTITIES: Set[str] = set()
DISABLED_DATASETS: Set[str] = set()

# Clickhouse Options
CLICKHOUSE_MAX_POOL_SIZE = 25

CLUSTERS: Sequence[Mapping[str, Any]] = [
    {
        "host": os.environ.get("CLICKHOUSE_HOST", "127.0.0.1"),
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
            "profiles",
            "functions",
            "replays",
            "generic_metrics_sets",
            "generic_metrics_distributions",
            "search_issues",
            "generic_metrics_counters",
            "spans",
            "group_attributes",
        },
        "single_node": True,
    },
]

# Dogstatsd Options
DOGSTATSD_HOST: str | None = None
DOGSTATSD_PORT: int | None = None
DOGSTATSD_SAMPLING_RATES = {
    "metrics.processor.set.size": 0.1,
    "metrics.processor.distribution.size": 0.1,
}

CLICKHOUSE_READONLY_USER = os.environ.get("CLICKHOUSE_READONLY_USER", "default")
CLICKHOUSE_READONLY_PASSWORD = os.environ.get("CLICKHOUSE_READONLY_PASS", "")

CLICKHOUSE_TRACE_USER = os.environ.get("CLICKHOUSE_TRACE_USER", "default")
CLICKHOUSE_TRACE_PASSWORD = os.environ.get("CLICKHOUSE_TRACE_PASS", "")

# Redis Options


class RedisClusterConfig(TypedDict):
    use_redis_cluster: bool

    cluster_startup_nodes: list[dict[str, Any]] | None
    host: str
    port: int
    password: str | None
    db: int
    ssl: bool
    reinitialize_steps: int


# The default cluster is configured using these global constants. If a config
# for a particular usecase in REDIS_CLUSTERS is missing/null, the default
# cluster is used.
USE_REDIS_CLUSTER = os.environ.get("USE_REDIS_CLUSTER", "0") != "0"

REDIS_CLUSTER_STARTUP_NODES: list[dict[str, Any]] | None = None
REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")
REDIS_DB = int(os.environ.get("REDIS_DB", 1))
REDIS_SSL = bool(os.environ.get("REDIS_SSL", False))
REDIS_INIT_MAX_RETRIES = 3
REDIS_REINITIALIZE_STEPS = 10


class RedisClusters(TypedDict):
    cache: RedisClusterConfig | None
    rate_limiter: RedisClusterConfig | None
    subscription_store: RedisClusterConfig | None
    replacements_store: RedisClusterConfig | None
    config: RedisClusterConfig | None
    dlq: RedisClusterConfig | None
    optimize: RedisClusterConfig | None
    admin_auth: RedisClusterConfig | None


REDIS_CLUSTERS: RedisClusters = {
    "cache": None,
    "rate_limiter": None,
    "subscription_store": None,
    "replacements_store": None,
    "config": None,
    "dlq": None,
    "optimize": None,
    "admin_auth": None,
}

# Query Recording Options
RECORD_QUERIES = False

# Runtime Config Options
CONFIG_MEMOIZE_TIMEOUT = 10
CONFIG_STATE: Mapping[str, Optional[Any]] = {}

# Sentry Options
SENTRY_DSN: str | None = None
SENTRY_TRACE_SAMPLE_RATE = 0

# Snuba Admin Options
SLACK_API_TOKEN = os.environ.get("SLACK_API_TOKEN")
SNUBA_SLACK_CHANNEL_ID = os.environ.get("SNUBA_SLACK_CHANNEL_ID")

# Snuba Options

SNAPSHOT_LOAD_PRODUCT = "snuba"

BULK_CLICKHOUSE_BUFFER = 10000
BULK_BINARY_LOAD_CHUNK = 2**22  # 4 MB


# Processor/Writer Options


BROKER_CONFIG: Mapping[str, Any] = {
    # See https://github.com/getsentry/arroyo/blob/main/arroyo/backends/kafka/configuration.py#L16-L38 for the supported options
    "bootstrap.servers": os.environ.get("DEFAULT_BROKERS", "127.0.0.1:9092"),
    "security.protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", "plaintext"),
    "ssl.ca.location": os.environ.get("KAFKA_SSL_CA_PATH", ""),
    "ssl.certificate.location": os.environ.get("KAFKA_SSL_CERT_PATH", ""),
    "ssl.key.location": os.environ.get("KAFKA_SSL_KEY_PATH", ""),
    "sasl.mechanism": os.environ.get("KAFKA_SASL_MECHANISM", None),
    "sasl.username": os.environ.get("KAFKA_SASL_USERNAME", None),
    "sasl.password": os.environ.get("KAFKA_SASL_PASSWORD", None),
}

# Mapping of default Kafka topic name to custom names
KAFKA_TOPIC_MAP: Mapping[str, str] = {}

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
VALID_RETENTION_DAYS = set([30, 90])

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

# Capacity Management
# HACK: This is necessary because single tenant does not have snuba-admin deployed / accessible
# so we can't change policy configs ourselves. This should be removed once we have snuba-admin
# available for single tenant since we can enable/disable policies at runtime there.
ENFORCE_BYTES_SCANNED_WINDOW_POLICY = True

# By default, allocation policies won't block requests from going through in a production
# environment to not cause incidents unnecessarily. If something goes wrong with allocation
# policy code, the request will still be able to go through (but it will create a dangerous
# situation eventually)
RAISE_ON_ALLOCATION_POLICY_FAILURES = False

# (logical topic name, # of partitions)
TOPIC_PARTITION_COUNTS: Mapping[str, int] = {}

COLUMN_SPLIT_MIN_COLS = 6
COLUMN_SPLIT_MAX_LIMIT = 1000
COLUMN_SPLIT_MAX_RESULTS = 5000

# The migration groups that can be skipped are listed in OPTIONAL_GROUPS.
# Migrations for skipped groups will not be run.
SKIPPED_MIGRATION_GROUPS: Set[str] = {"group_attributes"}

# Dataset readiness states supported in this environment
SUPPORTED_STATES: Set[str] = {"deprecate", "limited", "partial", "complete"}
# [04-18-2023] These two readiness state settings are temporary and used to facilitate the rollout of readiness states.
# We expect to remove them after all storages and migration groups have been migrated.
READINESS_STATE_FAIL_QUERIES: bool = True

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
ENABLE_METRICS_SUBSCRIPTIONS = os.environ.get("ENABLE_METRICS_SUBSCRIPTIONS", False)

SEPARATE_SCHEDULER_EXECUTOR_SUBSCRIPTIONS_DEV = os.environ.get(
    "SEPARATE_SCHEDULER_EXECUTOR_SUBSCRIPTIONS_DEV", False
)

# Subscriptions scheduler buffer size
SUBSCRIPTIONS_DEFAULT_BUFFER_SIZE = 10000
# (entity name, buffer size)
SUBSCRIPTIONS_ENTITY_BUFFER_SIZE: Mapping[str, int] = {}

# Used for migrating to/from writing metrics directly to aggregate tables
# rather than using materialized views
WRITE_METRICS_AGG_DIRECTLY = False
ENABLED_MATERIALIZATION_VERSION = 4

# Enable profiles ingestion
ENABLE_PROFILES_CONSUMER = os.environ.get("ENABLE_PROFILES_CONSUMER", False)

# Enable replays ingestion
ENABLE_REPLAYS_CONSUMER = os.environ.get("ENABLE_REPLAYS_CONSUMER", False)

# Enable issue occurrence ingestion
ENABLE_ISSUE_OCCURRENCE_CONSUMER = os.environ.get(
    "ENABLE_ISSUE_OCCURRENCE_CONSUMER", False
)

# Enable spans ingestion
ENABLE_SPANS_CONSUMER = os.environ.get("ENABLE_SPANS_CONSUMER", False)

# Cutoff time from UTC 00:00:00 to stop running optimize jobs to
# avoid spilling over to the next day.
OPTIMIZE_JOB_CUTOFF_TIME = 23
OPTIMIZE_QUERY_TIMEOUT = 4 * 60 * 60  # 4 hours
# sleep time to wait for a merge to complete
OPTIMIZE_BASE_SLEEP_TIME = 300  # 5 mins
OPTIMIZE_MAX_SLEEP_TIME = 2 * 60 * 60  # 2 hours
# merges longer than this will be considered long running
OPTIMIZE_MERGE_MIN_ELAPSED_CUTTOFF_TIME = 10 * 60  # 10 mins
# merges larger than this will be considered large and will be waited on
OPTIMIZE_MERGE_SIZE_CUTOFF = 50_000_000_000  # 50GB
# Maximum jitter to add to the scheduling of threads of an optimize job
OPTIMIZE_PARALLEL_MAX_JITTER_MINUTES = 30

# Start time in hours from UTC 00:00:00 after which we are allowed to run
# optimize jobs in parallel.
PARALLEL_OPTIMIZE_JOB_START_TIME = 0

# Cutoff time from UTC 00:00:00 to stop running optimize jobs in
# parallel to avoid running in parallel when peak traffic starts.
PARALLEL_OPTIMIZE_JOB_END_TIME = OPTIMIZE_JOB_CUTOFF_TIME

# Configuration directory settings
CONFIG_FILES_PATH = f"{Path(__file__).parent.parent.as_posix()}/datasets/configuration"

ROOT_REPO_PATH = f"{Path(__file__).parent.parent.parent.as_posix()}"

# File path glob for configs
STORAGE_CONFIG_FILES_GLOB = f"{CONFIG_FILES_PATH}/**/storages/*.yaml"
ENTITY_CONFIG_FILES_GLOB = f"{CONFIG_FILES_PATH}/**/entities/*.yaml"
DATASET_CONFIG_FILES_GLOB = f"{CONFIG_FILES_PATH}/**/dataset.yaml"

# Counter utility class window size in minutes
COUNTER_WINDOW_SIZE_MINUTES = 10


# Slicing Configuration

# Mapping of storage set key to slice count
# This is only for sliced storage sets
SLICED_STORAGE_SETS: Mapping[str, int] = {}

# Mapping storage set key to a mapping of logical partition
# to slice id
LOGICAL_PARTITION_MAPPING: Mapping[str, Mapping[int, int]] = {}

# The slice configs below are the "SLICED" versions to the equivalent default
# settings above. For example, "SLICED_KAFKA_TOPIC_MAP" is the "SLICED"
# version of "KAFKA_TOPIC_MAP". These should be filled out for any
# corresponding sliced storages defined above, with the applicable number of
# slices in mind.

# Cluster access can happen in one of the following ways:
# 1. The storage set is not sliced. In this case, the storage set key should
#    be defined in CLUSTERS only.
# 2. The storage set is sliced and there is no mega-cluster needed. In this
#    case, the storage set key should be defined in SLICED_CLUSTERS only.
# 3. The storage set is sliced and there is a mega-cluster needed. In this
#    case, the storage set key should be defined in both CLUSTERS and
#    SLICED_CLUSTERS. SLICED_CLUSTERS would contain the cluster information
#    of the sliced cluster. CLUSTERS would contain the cluster information of
#    the mega-cluster.
#
# We define sliced clusters, i.e. clusters that reside on multiple slices
# in SLICED_CLUSTERS. We define all associated(storage set, slice id) pairs in
# SLICED_CLUSTERS in the storage_sets field. Other fields are defined in the
# same way as they are in CLUSTERS.
SLICED_CLUSTERS: Sequence[Mapping[str, Any]] = []

# Mapping of (logical topic names, slice id) pairs to custom physical topic names
# This is only for sliced Kafka topics
SLICED_KAFKA_TOPIC_MAP: Mapping[Tuple[str, int], str] = {}

# Mapping of (logical topic names, slice id) pairs to broker config
# This is only for sliced Kafka topics
SLICED_KAFKA_BROKER_CONFIG: Mapping[Tuple[str, int], Mapping[str, Any]] = {}


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
            assert settings_spec is not None
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
