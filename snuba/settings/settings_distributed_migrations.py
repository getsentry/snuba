import os

# Three cluster set up, one for storage nodes, one for dedicated query node,
# and one for the single node migrations cluster.
CLUSTERS = [
    {
        "host": os.environ.get("CLICKHOUSE_HOST", "localhost"),
        "port": int(os.environ.get("CLICKHOUSE_PORT", 9005)),
        "user": os.environ.get("CLICKHOUSE_USER", "default"),
        "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
        "database": os.environ.get("CLICKHOUSE_DATABASE", "default"),
        "http_port": int(os.environ.get("CLICKHOUSE_HTTP_PORT", 8225)),
        "storage_sets": {},
        "single_node": False,
        "cluster_name": "cluster_example_one_query",
        "distributed_cluster_name": "cluster_example_one_query",
        "cluster_nodes": {
            "cluster_example_one_query": [
                (
                    "localhost",
                    9005,
                )
            ]
        },
    },
    {
        "host": os.environ.get("CLICKHOUSE_HOST", "localhost"),
        "port": int(os.environ.get("CLICKHOUSE_PORT", 9005)),
        "user": os.environ.get("CLICKHOUSE_USER", "default"),
        "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
        "database": os.environ.get("CLICKHOUSE_DATABASE", "default"),
        "http_port": int(os.environ.get("CLICKHOUSE_HTTP_PORT", 8225)),
        "storage_sets": {
            "cdc",
            "discover",
            "events",
            "events_ro",
            "metrics",
            # migrations,
            "outcomes",
            "querylog",
            "sessions",
            "transactions",
            "transactions_ro",
            "transactions_v2",
            "errors_v2",
            "errors_v2_ro",
            "profiles",
            "functions",
            "replays",
            "generic_metrics_sets",
            "generic_metrics_distributions",
        },
        "single_node": False,
        "cluster_name": "cluster_example_one",
        "distributed_cluster_name": "cluster_example_one_query",
        "cluster_nodes": {
            "cluster_example_one_query": [("localhost", 9005)],
            "cluster_example_one": [
                ("localhost", 9051, 1, 1),
                ("localhost", 9052, 1, 2),
                ("localhost", 9053, 2, 1),
                # ("localhost", 9055, 2, 2),
            ],
        },
    },
    {
        "host": os.environ.get("CLICKHOUSE_HOST", "localhost"),
        "port": int(os.environ.get("CLICKHOUSE_PORT", 9054)),
        "user": os.environ.get("CLICKHOUSE_USER", "default"),
        "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
        "database": os.environ.get("CLICKHOUSE_DATABASE", "default"),
        "http_port": int(os.environ.get("CLICKHOUSE_HTTP_PORT", 8254)),
        "storage_sets": {"migrations"},
        "single_node": True,
    },
]
