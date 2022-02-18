import os

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 9000))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")
CLICKHOUSE_HTTP_PORT = int(os.environ.get("CLICKHOUSE_HTTP_PORT", 8123))

CLUSTERS = [
    {
        "host": CLICKHOUSE_HOST,
        "port": CLICKHOUSE_PORT,
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD,
        "database": CLICKHOUSE_DATABASE,
        "http_port": CLICKHOUSE_HTTP_PORT,
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
        },
        "single_node": False,
        "cluster_name": "cluster_one_sh",
        "distributed_cluster_name": "cluster_one_sh",
    },
    {
        "host": os.environ.get("CLICKHOUSE_PROFILING_HOST", CLICKHOUSE_HOST),
        "port": int(os.environ.get("CLICKHOUSE_PROFILING_PORT", CLICKHOUSE_PORT)),
        "user": os.environ.get("CLICKHOUSE_PROFILING_USER", CLICKHOUSE_USER),
        "password": os.environ.get(
            "CLICKHOUSE_PROFILING_PASSWORD", CLICKHOUSE_PASSWORD
        ),
        "database": os.environ.get(
            "CLICKHOUSE_PROFILING_DATABASE", CLICKHOUSE_DATABASE
        ),
        "http_port": int(
            os.environ.get("CLICKHOUSE_PROFILING_HTTP_PORT", CLICKHOUSE_HTTP_PORT)
        ),
        "storage_sets": {"profiles"},
        "single_node": False,
        "cluster_name": "profiling_cluster",
        "distributed_cluster_name": "profiling_cluster",
    },
]
