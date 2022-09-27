import importlib
import os

import pytest

from snuba import settings
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters import cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import runner
from snuba.settings import settings_test_distributed_migrations


def setup_function() -> None:
    settings.CLUSTERS = settings_test_distributed_migrations.CLUSTERS
    importlib.reload(cluster)
    importlib.reload(runner)


def teardown_function() -> None:
    importlib.reload(settings)
    importlib.reload(cluster)
    importlib.reload(runner)


@pytest.mark.ci_only
def test_add_node() -> None:
    print("starting test add node")
    host_name = "clickhouse-04"
    port = int(os.environ.get("CLICKHOUSE_PORT", 9000))
    user = "default"
    password = ""
    database = os.environ.get("CLICKHOUSE_DATABASE", "default")

    client = ClickhousePool(
        host_name,
        port,
        user,
        password,
        database,
    )

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

    assert set(client.execute("SHOW TABLES").results) == {
        ("outcomes_raw_local",),
        ("outcomes_hourly_local",),
        ("outcomes_mv_hourly_local",),
    }
