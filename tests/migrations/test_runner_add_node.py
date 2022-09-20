import importlib
import os
import time
from copy import deepcopy

import pytest

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
        ).results
        for (table,) in data:
            connection.execute(f"DROP TABLE IF EXISTS {table} SYNC")


def teardown_function() -> None:
    importlib.reload(settings)
    importlib.reload(cluster)
    importlib.reload(runner)


@pytest.mark.ci_only
def test_add_node() -> None:
    host_name = os.environ.get("CLICKHOUSE_HOST", "localhost")
    port = int(os.environ.get("CLICKHOUSE_PORT", 9004))
    user = "default"
    password = ""
    database = os.environ.get("CLICKHOUSE_DATABASE", "default")
    print("running test_add_node")
    print("connection", host_name, port, database)
    client = ClickhousePool(
        host_name,
        port,
        user,
        password,
        database,
    )
    time.sleep(60)
    assert set(client.execute("SHOW TABLES").results) == set()

    runner.Runner.add_node(
        node_type=cluster.ClickhouseNodeType.LOCAL,
        storage_sets=[StorageSetKey.OUTCOMES],
        host_name=host_name,
        port=port,
        user=user,
        password=password,
        database=database,
    )
    print("added node")
    time.sleep(3600)
    assert set(client.execute("SHOW TABLES").results) == {
        ("outcomes_raw_local",),
        ("outcomes_hourly_local",),
        ("outcomes_mv_hourly_local",),
    }


def test_nope() -> None:
    print("running test_nope")
    time.sleep(300)
    assert False
