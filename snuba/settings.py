import logging
import os
from typing import Any, Mapping, MutableMapping, Sequence, Set


LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s %(message)s"

TESTING = False
DEBUG = True

PORT = 1218

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
            "discover",
            "events",
            "events_ro",
            "migrations",
            "outcomes",
            "querylog",
            "sessions",
            "transactions",
        },
        "single_node": True,
    },
]

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

USE_RESULT_CACHE = True

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
BULK_BINARY_LOAD_CHUNK = 2 ** 22  # 4 MB

# Processor/Writer Options
# DEPRECATED, please use BROKER_CONFIG instead
DEFAULT_BROKERS: Sequence[str] = []
# DEPRECATED, please use STORAGE_BROKER_CONFIG instead
DEFAULT_STORAGE_BROKERS: Mapping[str, Sequence[str]] = {}

BROKER_CONFIG: Mapping[str, Any] = {
    # See snuba/utils/streams/backends/kafka.py for the supported options
    "bootstrap.servers": os.environ.get("DEFAULT_BROKERS", "localhost:9092"),
}

# DEPRECATED, please use KAFKA_BROKER_CONFIG instead
STORAGE_BROKER_CONFIG: Mapping[str, Mapping[str, Any]] = {}

# DEPRECATED, please use KAFKA_TOPIC_MAP instead
STORAGE_TOPICS: Mapping[str, Mapping[str, Any]] = {}

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

DEFAULT_RETENTION_DAYS = 90
RETENTION_OVERRIDES: Mapping[int, int] = {}

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
REPLACER_IMMEDIATE_OPTIMIZE = False

TURBO_SAMPLE_RATE = 0.1

PROJECT_STACKTRACE_BLACKLIST: Set[int] = set()

TOPIC_PARTITION_COUNTS: Mapping[str, int] = {}  # (topic name, # of partitions)

ERRORS_ROLLOUT_ALL: bool = True
ERRORS_ROLLOUT_WRITABLE_STORAGE: bool = True

COLUMN_SPLIT_MIN_COLS = 6
COLUMN_SPLIT_MAX_LIMIT = 1000
COLUMN_SPLIT_MAX_RESULTS = 5000

# Migrations in skipped groups will not be run
SKIPPED_MIGRATION_GROUPS: Set[str] = {"querylog", "spans_experimental"}


def _load_settings(obj: MutableMapping[str, Any] = locals()) -> None:
    """Load settings from the path provided in the SNUBA_SETTINGS environment
    variable. Defaults to `./snuba/settings_base.py`. Users can provide a
    short name like `test` that will be expanded to `settings_test.py` in the
    main Snuba directory, or they can provide a full absolute path such as
    `/foo/bar/my_settings.py`."""

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
            settings_module = importlib.import_module(module_format % settings, "snuba")

        for attr in dir(settings_module):
            if attr.isupper():
                obj[attr] = getattr(settings_module, attr)


# Rudimentary validation function
def _validate_settings() -> None:
    logger = logging.getLogger("snuba.settings")

    if QUERIES_TOPIC != "snuba-queries":
        raise ValueError("QUERIES_TOPIC is deprecated. Use KAFKA_TOPIC_MAP instead.")

    if STORAGE_TOPICS:
        logger.warning(
            "DEPRECATED: STORAGE_TOPICS is derpecated. Use KAFKA_TOPIC_MAP instead."
        )

    if STORAGE_BROKER_CONFIG:
        logger.warning(
            "DEPRECATED: STORAGE_BROKER_CONFIG is derpecated. Use KAFKA_BROKER_CONFIG instead."
        )

    from snuba.utils.streams.topics import Topic

    default_topic_names = {t.value for t in Topic}

    for key in KAFKA_TOPIC_MAP.keys():
        if key not in default_topic_names:
            raise ValueError(f"Invalid topic value: {key}")


_load_settings()
_validate_settings()
