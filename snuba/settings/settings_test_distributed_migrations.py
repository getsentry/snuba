import os
from typing import Any, Mapping, Sequence

from snuba.settings.settings_test import *  # noqa

CLUSTERS: Sequence[Mapping[str, Any]] = [
    {
        "host": "clickhouse-query",
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
        "host": "clickhouse-query",
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
        "host": "clickhouse-query",
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
            "errors_v2",
            "errors_v2_ro",
            "profiles",
            "functions",
            "replays",
            "generic_metrics_sets",
            "generic_metrics_distributions",
        },
        "single_node": False,
        "cluster_name": "storage_cluster",
        "distributed_cluster_name": "query_cluster",
    },
]
