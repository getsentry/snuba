import importlib
import os
from copy import deepcopy

from snuba import settings
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters import cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import runner


def setup_function() -> None:
    settings.CLUSTERS = [
        {
            **deepcopy(settings.CLUSTERS[0]),
            **{
                "single_node": False,
                "cluster_name": "local_hosts",
                "distributed_cluster_name": "dist_hosts",
            },
        }
    ]
    importlib.reload(cluster)
    importlib.reload(runner)

    # Drop all tables
    for c in cluster.CLUSTERS:
        connection = c.get_query_connection(cluster.ClickhouseClientSettings.MIGRATE)
        database = c.get_database()

        data = connection.execute(
            f"SELECT name FROM system.tables WHERE database = '{database}'"
        )
        for (table,) in data:
            connection.execute(f"DROP TABLE IF EXISTS {table}")


def teardown_function() -> None:
    importlib.reload(settings)
    importlib.reload(cluster)
    importlib.reload(runner)


def test_add_node() -> None:
    host_name = os.environ.get("CLICKHOUSE_HOST", "localhost")
    port = int(os.environ.get("CLICKHOUSE_PORT", 9000))
    user = "default"
    password = ""
    database = os.environ.get("CLICKHOUSE_DATABASE", "default")

    client = ClickhousePool(host_name, port, user, password, database,)

    assert set(client.execute("SHOW TABLES")) == set()

    runner.Runner.add_node(
        node_type=cluster.ClickhouseNodeType.LOCAL,
        storage_sets=[StorageSetKey.OUTCOMES],
        host_name=host_name,
        port=port,
        user=user,
        password=password,
        database=database,
    )

    assert set(client.execute("SHOW TABLES")) == {
        ("outcomes_raw_local",),
        ("outcomes_hourly_local",),
        ("outcomes_mv_hourly_local",),
    }
