import importlib
import json
import time

import pytest

from snuba import settings
from snuba.clusters import cluster
from snuba.clusters.cluster import ClickhouseClientSettings, ClickhouseCluster
from snuba.environment import setup_sentry
from snuba.migrations import runner
from snuba.settings import settings_test_distributed_migrations


@pytest.mark.ci_only
def pytest_configure() -> None:
    """
    Set up the Sentry SDK to avoid errors hidden by configuration.
    Ensure the snuba_test database exists
    """
    assert settings.TESTING, "settings.TESTING is False, try `SNUBA_SETTINGS=test` or `make test`"

    print("setting up test cluster!")

    setup_sentry()

    settings.CLUSTERS = settings_test_distributed_migrations.CLUSTERS

    importlib.reload(cluster)
    importlib.reload(runner)

    print(
        "\nclusters:\n",
        json.dumps(
            settings.CLUSTERS,
            indent=4,
            default=lambda x: list(x) if isinstance(x, set) else x.__dict__,
        ),
    )
    print("waiting 10 seconds for clickhouse to start")
    time.sleep(10)  # wait for clickhouse to start

    for cluster_node in settings.CLUSTERS:
        clickhouse_cluster = ClickhouseCluster(
            host=cluster_node["host"],
            port=cluster_node["port"],
            user="default",
            password="",
            database="default",
            http_port=cluster_node["http_port"],
            storage_sets=cluster_node["storage_sets"],
            single_node=cluster_node["single_node"],
            cluster_name=cluster_node["cluster_name"] if "cluster_name" in cluster_node else None,
            distributed_cluster_name=(
                cluster_node["distributed_cluster_name"]
                if "distributed_cluster_name" in cluster_node
                else None
            ),
            secure=cluster_node.get("secure", False),
            ca_certs=cluster_node.get("ca_certs", None),
            verify=cluster_node.get("verify", False),
        )

        database_name = cluster_node["database"]

        # create the test database
        clickhouse_cluster.get_query_connection(ClickhouseClientSettings.MIGRATE).execute(
            f"CREATE DATABASE IF NOT EXISTS {database_name} ON CLUSTER {clickhouse_cluster.get_clickhouse_cluster_name()};"
        )

    run_setup_migrations()


def run_setup_migrations() -> None:
    from snuba.migrations.runner import Runner

    print("running setup migrations for distributed tests")
    Runner().run_all(force=True)
