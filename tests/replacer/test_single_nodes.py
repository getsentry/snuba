from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor
from typing import List, Mapping, MutableMapping, Sequence

import pytest

from snuba.clickhouse.native import ClickhousePool
from snuba.clusters import cluster
from snuba.clusters.cluster import ClickhouseNode
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.errors_replacer import NEEDS_FINAL, LegacyReplacement
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.replacer import DistributedTableExecutor, LocalTableExecutor, ReplacerWorker
from snuba.state import set_config
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from tests.clusters.fake_cluster import (
    FakeClickhouseCluster,
    FakeClickhousePool,
    FakeFailingClickhousePool,
    ServerExplodedException,
)

LOCAL_QUERY = """\
INSERT INTO errors_local (project_id, timestamp, event_id)
SELECT project_id, timestamp, event_id, group_id, primary_hash
FROM errors_local FINAL
WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'
"""

DIST_QUERY = """\
INSERT INTO errors_dist (project_id, timestamp, event_id)
SELECT project_id, timestamp, event_id, group_id, primary_hash
FROM errors_dist FINAL
WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'
"""

TEST_CASES = [
    pytest.param(
        "[100,1]",
        {
            "query_node": [
                "SELECT count() FROM errors_dist FINAL WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'",
            ],
            "storage-0-0": [LOCAL_QUERY],
            "storage-0-1": [],
            "storage-1-0": [LOCAL_QUERY],
            "storage-1-1": [],
            "storage-2-0": [LOCAL_QUERY],
            "storage-2-1": [],
        },
        id="Replacements through storage nodes",
    ),
    pytest.param(
        "",
        {
            "query_node": [
                "SELECT count() FROM errors_dist FINAL WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'",
                DIST_QUERY,
            ]
        },
        id="Replacements through query node",
    ),
    pytest.param(
        "[10]",
        {
            "query_node": [
                "SELECT count() FROM errors_dist FINAL WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'",
                DIST_QUERY,
            ]
        },
        id="Replacements through query node. Wrong project",
    ),
]

COUNT_QUERY_TEAMPLATE = "SELECT count() FROM %(table_name)s FINAL WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'"
INSERT_QUERY_TEMPLATE = """\
INSERT INTO %(table_name)s (%(required_columns)s)
SELECT %(select_columns)s
FROM %(table_name)s FINAL
WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'
"""

FINAL_QUERY_TEMPLATE = {
    "required_columns": "project_id, timestamp, event_id",
    "select_columns": "project_id, timestamp, event_id, group_id, primary_hash",
}


def _build_cluster(healthy: bool = True) -> FakeClickhouseCluster:
    return FakeClickhouseCluster(
        host="query_node",
        port=9000,
        user="default",
        password="",
        database="default",
        http_port=8123,
        storage_sets={"events"},
        single_node=False,
        cluster_name="my_cluster",
        distributed_cluster_name="my_distributed_cluster",
        nodes=[
            ClickhouseNode("storage-0-0", 9000, 1, 1),
            ClickhouseNode("storage-0-1", 9000, 1, 2),
            ClickhouseNode("storage-1-0", 9000, 2, 1),
            ClickhouseNode("storage-1-1", 9000, 2, 2),
            ClickhouseNode("storage-2-0", 9000, 3, 1),
            ClickhouseNode("storage-2-1", 9000, 3, 2),
        ],
        healthy=healthy,
    )


@pytest.mark.parametrize(
    "write_node_replacements_projects, expected_queries", TEST_CASES
)
def test_write_single_node(
    write_node_replacements_projects: str,
    expected_queries: Mapping[str, Sequence[str]],
) -> None:
    """
    Test the execution of replacement queries on both storage nodes and
    query nodes.
    """
    set_config("write_node_replacements_projects", write_node_replacements_projects)
    test_cluster = _build_cluster()
    # Cannot use patch properly for this as I cannot patch the use case
    # with a new instance for each parameter in the parametrized test.
    current_clusters = cluster.CLUSTERS
    current_mapping = cluster._STORAGE_SET_CLUSTER_MAP
    cluster.CLUSTERS = [test_cluster]
    cluster._STORAGE_SET_CLUSTER_MAP = {StorageSetKey.EVENTS: cluster.CLUSTERS[0]}

    replacer = ReplacerWorker(
        get_writable_storage(StorageKey.ERRORS), DummyMetricsBackend()
    )

    try:
        replacer.flush_batch(
            [
                LegacyReplacement(
                    COUNT_QUERY_TEAMPLATE,
                    INSERT_QUERY_TEMPLATE,
                    FINAL_QUERY_TEMPLATE,
                    (NEEDS_FINAL, 1),
                )
            ]
        )

        queries = test_cluster.get_queries()
        assert queries == expected_queries
    finally:
        cluster.CLUSTERS = current_clusters
        cluster._STORAGE_SET_CLUSTER_MAP = current_mapping


def test_failing_query() -> None:
    """
    Test the execution of replacement queries on single node
    when the query fails.
    """
    set_config("write_node_replacements_projects", "[1]")
    test_cluster = _build_cluster(healthy=False)

    current_clusters = cluster.CLUSTERS
    current_mapping = cluster._STORAGE_SET_CLUSTER_MAP
    cluster.CLUSTERS = [test_cluster]
    cluster._STORAGE_SET_CLUSTER_MAP = {StorageSetKey.EVENTS: cluster.CLUSTERS[0]}

    replacer = ReplacerWorker(
        get_writable_storage(StorageKey.ERRORS), DummyMetricsBackend()
    )

    with pytest.raises(ServerExplodedException):
        replacer.flush_batch(
            [
                LegacyReplacement(
                    COUNT_QUERY_TEAMPLATE,
                    INSERT_QUERY_TEMPLATE,
                    FINAL_QUERY_TEMPLATE,
                    (NEEDS_FINAL, 1),
                )
            ]
        )

    cluster.CLUSTERS = current_clusters
    cluster._STORAGE_SET_CLUSTER_MAP = current_mapping


TEST_LOCAL_EXECUTOR = [
    pytest.param(
        {1: [FakeClickhousePool("snuba-errors-0-0")]},
        FakeClickhousePool("snuba-query"),
        {"snuba-errors-0-0": [LOCAL_QUERY]},
        id="1 Node successful query",
    ),
    pytest.param(
        {
            1: [FakeClickhousePool("snuba-errors-0-0")],
            2: [FakeClickhousePool("snuba-errors-1-0")],
            3: [FakeClickhousePool("snuba-errors-2-0")],
        },
        FakeClickhousePool("snuba-query"),
        {
            "snuba-errors-0-0": [LOCAL_QUERY],
            "snuba-errors-1-0": [LOCAL_QUERY],
            "snuba-errors-2-0": [LOCAL_QUERY],
        },
        id="Multiple successful shards",
    ),
    pytest.param(
        {
            1: [
                FakeClickhousePool("snuba-errors-0-0"),
                FakeClickhousePool("snuba-errors-0-1"),
            ]
        },
        FakeClickhousePool("snuba-query"),
        {"snuba-errors-0-0": [LOCAL_QUERY]},
        id="Multiple replicas, first is successful",
    ),
    pytest.param(
        {
            1: [
                FakeFailingClickhousePool("snuba-errors-0-0"),
                FakeClickhousePool("snuba-errors-0-1"),
            ]
        },
        FakeClickhousePool("snuba-query"),
        {"snuba-errors-0-1": [LOCAL_QUERY]},
        id="Multiple replicas, first fails",
    ),
    pytest.param(
        {
            1: [
                FakeFailingClickhousePool("snuba-errors-0-0"),
                FakeClickhousePool("snuba-errors-0-1"),
            ],
            2: [
                FakeClickhousePool("snuba-errors-1-0"),
                FakeFailingClickhousePool("snuba-errors-1-1"),
            ],
        },
        FakeClickhousePool("snuba-query"),
        {"snuba-errors-0-1": [LOCAL_QUERY], "snuba-errors-1-0": [LOCAL_QUERY]},
        id="Multiple shards, one successful query per shard",
    ),
    pytest.param(
        {
            1: [
                FakeFailingClickhousePool("snuba-errors-0-0"),
                FakeFailingClickhousePool("snuba-errors-0-1"),
            ]
        },
        FakeClickhousePool("snuba-query"),
        {"snuba-query": [DIST_QUERY]},
        id="All replicas fail, use backup",
    ),
    pytest.param(
        {
            1: [
                FakeFailingClickhousePool("snuba-errors-0-0"),
                FakeFailingClickhousePool("snuba-errors-0-1"),
            ],
            2: [FakeClickhousePool("snuba-errors-1-0")],
        },
        FakeClickhousePool("snuba-query"),
        {"snuba-errors-1-0": [LOCAL_QUERY], "snuba-query": [DIST_QUERY]},
        id="One shard only succeeds. Use backup and duplicate query",
    ),
]


@pytest.mark.parametrize(
    "connection_pools, backup_connection, expected_queries", TEST_LOCAL_EXECUTOR
)
def test_local_executor(
    connection_pools: Mapping[int, Sequence[ClickhousePool]],
    backup_connection: ClickhousePool,
    expected_queries: Mapping[str, Sequence[str]],
) -> None:
    queries: MutableMapping[str, List[str]] = defaultdict(list)

    def run_query(
        connection: ClickhousePool,
        query: str,
        records_count: int,
        metrics: MetricsBackend,
    ) -> None:
        connection.execute_robust(query)
        queries[connection.get_host()].append(query)

    insert_executor = LocalTableExecutor(
        runner=run_query,
        thread_pool=ThreadPoolExecutor(),
        main_connections=connection_pools,
        main_table="errors_local",
        backup_executor=DistributedTableExecutor(
            runner=run_query,
            connection=backup_connection,
            table="errors_dist",
            metrics=DummyMetricsBackend(),
        ),
        metrics=DummyMetricsBackend(),
    )

    insert_executor.execute(
        replacement=LegacyReplacement(
            COUNT_QUERY_TEAMPLATE,
            INSERT_QUERY_TEMPLATE,
            FINAL_QUERY_TEMPLATE,
            (NEEDS_FINAL, 1),
        ),
        records_count=1,
    )

    assert queries == expected_queries
