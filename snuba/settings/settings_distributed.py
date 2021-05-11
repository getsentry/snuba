import os

DIST = True

CLUSTERS = [
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
        "single_node": False,
        "cluster_name": "test_shard_localhost",
        "distributed_cluster_name": "test_shard_localhost",
    },
]
