import os
from typing import Any, Mapping, MutableMapping, Optional, Sequence, Set


LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s %(message)s"

TESTING = False
DEBUG = True

PORT = 1218

DEFAULT_DATASET_NAME = "events"
DISABLED_DATASETS: Set[str] = {"querylog"}

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
            "events",
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

# Processor/Writer Options
DEFAULT_BROKERS = ["localhost:9092"]
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
REPLACER_IMMEDIATE_OPTIMIZE = False

TURBO_SAMPLE_RATE = 0.1

PROJECT_STACKTRACE_BLACKLIST: Set[int] = set()

TOPIC_PARTITION_COUNTS: Mapping[str, int] = {}  # (topic name, # of partitions)

AST_DATASET_ROLLOUT: Mapping[str, int] = {
    "outcomes": 100,
}  # (dataset name: percentage)
AST_REFERRER_ROLLOUT: Mapping[str, Mapping[Optional[str], int]] = {
    "events": {
        # Simple queries without splitting or user customizations
        "Group.filter_by_event_id": 100,
        "api.group-hashes": 100,
        # Simple time bucketed queries
        "api.organization-events-stats": 100,
        "incidents.get_incident_event_stats": 100,
        "incidents.get_incident_aggregates": 100,
        # Queries with tags resolution
        "tagstore.get_tag_value_paginator_for_projects": 100,
        "tagstore.get_group_tag_value_iter": 100,
        # Time/column split queries
        "api.organization-events-direct-hit": 100,
        "eventstore.get_unfetched_events": 100,
        "api.organization-events": 100,
        "api.group-events": 100,
    },
    "transactions": {
        # Simple time bucketed queries
        "incidents.get_incident_event_stats": 100,
        # Queries with tags resolution
        "incidents.get_incident_aggregates": 100,
    },
}  # (dataset name: (referrer: percentage))


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


_load_settings()
