import os
from typing import Any, Mapping, Sequence

from snuba.settings.settings_test import *  # noqa

CLUSTERS: Sequence[Mapping[str, Any]] = [
    {
        "host": os.environ.get("CLICKHOUSE_HOST_MIGRATIONS", "clickhouse-query"),
        "port": int(os.environ.get("CLICKHOUSE_PORT", 9000)),
        "user": os.environ.get("CLICKHOUSE_USER", "default"),
        "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
        "database": os.environ.get("CLICKHOUSE_DATABASE", "snuba_test"),
        "http_port": int(os.environ.get("CLICKHOUSE_HTTP_PORT", 8229)),
        "storage_sets": {},
        "single_node": False,
        "cluster_name": "query_cluster",
        "distributed_cluster_name": "query_cluster",
    },
    {
        "host": os.environ.get("CLICKHOUSE_HOST_MIGRATIONS", "clickhouse-query"),
        "port": int(os.environ.get("CLICKHOUSE_PORT", 9000)),
        "user": os.environ.get("CLICKHOUSE_USER", "default"),
        "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
        "database": os.environ.get("CLICKHOUSE_DATABASE", "snuba_test"),
        "http_port": int(os.environ.get("CLICKHOUSE_HTTP_PORT", 8229)),
        "storage_sets": {
            "migrations",
        },
        "single_node": False,
        "cluster_name": "migrations_cluster",
        "distributed_cluster_name": "query_cluster",
    },
    {
        "host": os.environ.get("CLICKHOUSE_HOST_MIGRATIONS", "clickhouse-query"),
        "port": int(os.environ.get("CLICKHOUSE_PORT", 9000)),
        "user": os.environ.get("CLICKHOUSE_USER", "default"),
        "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
        "database": os.environ.get("CLICKHOUSE_DATABASE", "snuba_test"),
        "http_port": int(os.environ.get("CLICKHOUSE_HTTP_PORT", 8229)),
        "storage_sets": {
            "cdc",
            "discover",
            "events",
            "events_ro",
            "metrics",
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
        "single_node": False,
        "cluster_name": "storage_cluster",
        "distributed_cluster_name": "query_cluster",
    },
]
