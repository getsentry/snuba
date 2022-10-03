import os
from typing import Iterator

import pytest


@pytest.mark.ci_only
def pytest_configure() -> None:
    """
    Set up the Sentry SDK to avoid errors hidden by configuration.
    Ensure the snuba_test database exists
    """
    # assert (
    #     settings.TESTING
    # ), "settings.TESTING is False, try `SNUBA_SETTINGS=test` or `make test`"

    # print("setting up test cluster!")

    # setup_sentry()
    # if os.environ.get("SNUBA_SETTINGS") != "test_distributed_migrations":
    #     return

    # settings.CLUSTERS = settings_test_distributed_migrations.CLUSTERS

    # importlib.reload(cluster)
    # importlib.reload(runner)

    # print(
    #     "\nclusters:\n",
    #     json.dumps(
    #         settings.CLUSTERS,
    #         indent=4,
    #         default=lambda x: list(x) if isinstance(x, set) else x.__dict__,
    #     ),
    # )
    # print("waiting 30 seconds for clickhouse to start")
    # time.sleep(30)  # wait for clickhouse to start

    # for cluster_node in settings.CLUSTERS:
    # clickhouse_cluster = ClickhouseCluster(
    #     host=cluster_node["host"],
    #     port=cluster_node["port"],
    #     user="default",
    #     password="",
    #     database="default",
    #     http_port=cluster_node["http_port"],
    #     storage_sets=cluster_node["storage_sets"],
    #     single_node=cluster_node["single_node"],
    #     cluster_name=cluster_node["cluster_name"]
    #     if "cluster_name" in cluster_node
    #     else None,
    #     distributed_cluster_name=cluster_node["distributed_cluster_name"]
    #     if "distributed_cluster_name" in cluster_node
    #     else None,
    # )

    # database_name = cluster_node["database"]

    # ClickhouseClientSettings.MIGRATE.value.timeout = 50000
    # # create the test database
    # clickhouse_cluster.get_query_connection(
    #     ClickhouseClientSettings.MIGRATE
    # ).execute(
    #     f"CREATE DATABASE IF NOT EXISTS {database_name} ON CLUSTER {clickhouse_cluster.get_clickhouse_cluster_name()};"
    # )
    if os.environ.get("SNUBA_SETTINGS") != "test_distributed_migrations":
        run_setup_migrations()


def run_setup_migrations() -> None:
    from snuba.migrations.runner import Runner

    Runner().run_all(force=True)


# override the default fixture to run migrations
@pytest.fixture(autouse=True)
def run_migrations() -> Iterator[None]:
    yield


@pytest.fixture(autouse=True)
def clear_recorded_metrics() -> Iterator[None]:
    yield
    return


# def pytest_collection_modifyitems(items):
#     for item in items:
#         item.add_marker("distributed_multi_node")


@pytest.fixture(autouse=True)
def skip_not_distributed() -> None:
    if os.environ.get("SNUBA_SETTINGS") != "test_distributed_migrations":
        pytest.skip("skipped distributed multi node test")
