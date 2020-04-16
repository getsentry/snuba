import os
from typing import Any, Mapping, MutableMapping, Sequence, Set


LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s %(message)s"

TESTING = False
DEBUG = True

PORT = 1218

DEFAULT_DATASET_NAME = "events"
DISABLED_DATASETS: Set[str] = {"querylog"}
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

USE_RESULT_CACHE = False

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
DEFAULT_DATASET_BROKERS: Mapping[str, Sequence[str]] = {}
DEFAULT_STORAGE_BROKERS: Mapping[str, Sequence[str]] = {}

DEFAULT_MAX_BATCH_SIZE = 50000
DEFAULT_MAX_BATCH_TIME_MS = 2 * 1000
DEFAULT_QUEUED_MAX_MESSAGE_KBYTES = 10000
DEFAULT_QUEUED_MIN_MESSAGES = 10000
DISCARD_OLD_EVENTS = True
CLICKHOUSE_HTTP_CHUNK_SIZE = 1

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

TURBO_SAMPLE_RATE = 0.1

PROJECT_STACKTRACE_BLACKLIST: Set[int] = set()

TOPIC_PARTITION_COUNTS: Mapping[str, int] = {}  # (topic name, # of partitions)


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
            module_format = ".%s" if settings.startswith("settings_") else ".settings_%s"
            settings_module = importlib.import_module(module_format % settings, "snuba")

        for attr in dir(settings_module):
            if attr.isupper():
                obj[attr] = getattr(settings_module, attr)


_load_settings()


# Snuba currently only supports a single cluster to which all storage sets are assigned.
# In future this will be configurable.
CLUSTERS: Sequence[Mapping[str, Any]] = [
    {
        "host": CLICKHOUSE_HOST,
        "port": CLICKHOUSE_PORT,
        "http_port": CLICKHOUSE_HTTP_PORT,
        "storage_sets": {"events", "groupassignees", "outcomes", "querylog", "sessions", "transactions"},
    },
]
