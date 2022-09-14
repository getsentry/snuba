from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor
from typing import (
    Any,
    Callable,
    Generator,
    List,
    Mapping,
    MutableMapping,
    Sequence,
    Tuple,
)

import pytest

from snuba.clickhouse.native import ClickhousePool
from snuba.clusters import cluster
from snuba.clusters.cluster import ClickhouseNode
from snuba.clusters.storage_set_key import StorageSetKey
from snuba.datasets.errors_replacer import NEEDS_FINAL, LegacyReplacement
from snuba.datasets.events_processor_base import ReplacementType
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.replacer import (
    InOrderConnectionPool,
    QueryNodeExecutor,
    ReplacerWorker,
    ShardedExecutor,
)
from snuba.replacers.replacer_processor import ReplacementMessageMetadata
from snuba.state import set_config
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from tests.clusters.fake_cluster import (
    FakeClickhouseCluster,
    FakeClickhousePool,
    ServerExplodedException,
)


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
            (ClickhouseNode("query_node", 9000, None, None), healthy),
            (ClickhouseNode("storage-0-0", 9000, 1, 1), healthy),
            (ClickhouseNode("storage-0-1", 9000, 1, 2), healthy),
            (ClickhouseNode("storage-1-0", 9000, 2, 1), healthy),
            (ClickhouseNode("storage-1-1", 9000, 2, 2), healthy),
            (ClickhouseNode("storage-2-0", 9000, 3, 1), healthy),
            (ClickhouseNode("storage-2-1", 9000, 3, 2), healthy),
        ],
    )


@pytest.fixture
def override_cluster() -> Generator[
    Callable[[bool], FakeClickhouseCluster], None, None
]:
    current_clusters = cluster.CLUSTERS
    current_mapping = cluster._STORAGE_SET_CLUSTER_MAP

    def override(healthy: bool) -> FakeClickhouseCluster:
        test_cluster = _build_cluster(healthy=healthy)
        cluster.CLUSTERS = [test_cluster]
        cluster._STORAGE_SET_CLUSTER_MAP = {StorageSetKey.EVENTS: cluster.CLUSTERS[0]}
        return test_cluster

    try:
        yield override
    finally:
        cluster.CLUSTERS = current_clusters
        cluster._STORAGE_SET_CLUSTER_MAP = current_mapping


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
        "override_cluster",
        "[100,1]",
        {
            "query_node": [
                "SELECT count() FROM errors_dist FINAL WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'",
            ],
            "storage-0-0": [LOCAL_QUERY],
            "storage-1-0": [LOCAL_QUERY],
            "storage-2-0": [LOCAL_QUERY],
        },
        id="Replacements through storage nodes",
    ),
    pytest.param(
        "override_cluster",
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
        "override_cluster",
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

COUNT_QUERY_TEMPLATE = "SELECT count() FROM %(table_name)s FINAL WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'"
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

REPLACEMENT_TYPE = (
    ReplacementType.EXCLUDE_GROUPS
)  # Arbitrary replacement type, no impact on tests

REPLACEMENT_MESSAGE_METADATA = ReplacementMessageMetadata(0, 0, "")


@pytest.mark.parametrize(
    "override_fixture, write_node_replacements_projects, expected_queries", TEST_CASES
)
def test_write_each_node(
    override_fixture: Callable[[bool], FakeClickhouseCluster],
    write_node_replacements_projects: str,
    expected_queries: Mapping[str, Sequence[str]],
    request: Any,
) -> None:
    """
    Test the execution of replacement queries on both storage nodes and
    query nodes.
    """
    set_config("write_node_replacements_projects", write_node_replacements_projects)
    override_func = request.getfixturevalue(override_fixture)
    test_cluster = override_func(True)

    replacer = ReplacerWorker(
        get_writable_storage(StorageKey.ERRORS),
        "consumer_group",
        DummyMetricsBackend(),
    )

    replacer.flush_batch(
        [
            LegacyReplacement(
                COUNT_QUERY_TEMPLATE,
                INSERT_QUERY_TEMPLATE,
                FINAL_QUERY_TEMPLATE,
                (NEEDS_FINAL, 1),
                REPLACEMENT_TYPE,
                REPLACEMENT_MESSAGE_METADATA,
            )
        ]
    )

    queries = test_cluster.get_queries()
    assert queries == expected_queries


def test_failing_query(
    override_cluster: Callable[[bool], FakeClickhouseCluster]
) -> None:
    """
    Test the execution of replacement queries on single node
    when the query fails.
    """
    set_config("write_node_replacements_projects", "[1]")
    override_cluster(False)

    replacer = ReplacerWorker(
        get_writable_storage(StorageKey.ERRORS),
        "consumer_group",
        DummyMetricsBackend(),
    )

    with pytest.raises(ServerExplodedException):
        replacer.flush_batch(
            [
                LegacyReplacement(
                    COUNT_QUERY_TEMPLATE,
                    INSERT_QUERY_TEMPLATE,
                    FINAL_QUERY_TEMPLATE,
                    (NEEDS_FINAL, 1),
                    REPLACEMENT_TYPE,
                    REPLACEMENT_MESSAGE_METADATA,
                )
            ]
        )


def test_load_balancing(
    override_cluster: Callable[[bool], FakeClickhouseCluster]
) -> None:
    """
    Test running two replacements in a row and verify the queries
    are properly load balanced on different nodes.
    """
    set_config("write_node_replacements_projects", "[1]")
    cluster = override_cluster(True)

    replacer = ReplacerWorker(
        get_writable_storage(StorageKey.ERRORS),
        "consumer_group",
        DummyMetricsBackend(),
    )
    replacement = LegacyReplacement(
        COUNT_QUERY_TEMPLATE,
        INSERT_QUERY_TEMPLATE,
        FINAL_QUERY_TEMPLATE,
        (NEEDS_FINAL, 1),
        REPLACEMENT_TYPE,
        REPLACEMENT_MESSAGE_METADATA,
    )
    replacer.flush_batch([replacement, replacement])

    assert cluster.get_queries() == {
        "query_node": [
            "SELECT count() FROM errors_dist FINAL WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'",
            "SELECT count() FROM errors_dist FINAL WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'",
        ],
        "storage-0-0": [LOCAL_QUERY, LOCAL_QUERY],
        "storage-1-0": [LOCAL_QUERY, LOCAL_QUERY],
        "storage-2-0": [LOCAL_QUERY, LOCAL_QUERY],
    }


TEST_LOCAL_EXECUTOR = [
    pytest.param(
        {1: [(ClickhouseNode("snuba-errors-0-0", 9000, 1, 1), True)]},
        FakeClickhousePool("snuba-query"),
        {"snuba-errors-0-0": [LOCAL_QUERY]},
        id="1 Node successful query",
    ),
    pytest.param(
        {
            1: [(ClickhouseNode("snuba-errors-0-0", 9000, 1, 1), True)],
            2: [(ClickhouseNode("snuba-errors-1-0", 9000, 2, 1), True)],
            3: [(ClickhouseNode("snuba-errors-2-0", 9000, 3, 1), True)],
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
                (ClickhouseNode("snuba-errors-0-0", 9000, 1, 1), True),
                (ClickhouseNode("snuba-errors-0-1", 9000, 1, 2), True),
            ]
        },
        FakeClickhousePool("snuba-query"),
        {"snuba-errors-0-0": [LOCAL_QUERY]},
        id="Multiple replicas, first is successful",
    ),
    pytest.param(
        {
            1: [
                (ClickhouseNode("snuba-errors-0-0", 9000, 1, 1), False),
                (ClickhouseNode("snuba-errors-0-1", 9000, 1, 2), True),
            ]
        },
        FakeClickhousePool("snuba-query"),
        {"snuba-errors-0-1": [LOCAL_QUERY]},
        id="Multiple replicas, first fails",
    ),
    pytest.param(
        {
            1: [
                (ClickhouseNode("snuba-errors-0-0", 9000, 1, 1), False),
                (ClickhouseNode("snuba-errors-0-1", 9000, 1, 2), True),
            ],
            2: [
                (ClickhouseNode("snuba-errors-1-0", 9000, 2, 1), True),
                (ClickhouseNode("snuba-errors-1-1", 9000, 2, 2), False),
            ],
        },
        FakeClickhousePool("snuba-query"),
        {"snuba-errors-0-1": [LOCAL_QUERY], "snuba-errors-1-0": [LOCAL_QUERY]},
        id="Multiple shards, one successful query per shard",
    ),
    pytest.param(
        {
            1: [
                (ClickhouseNode("snuba-errors-0-0", 9000, 1, 1), False),
                (ClickhouseNode("snuba-errors-0-1", 9000, 1, 2), False),
            ]
        },
        FakeClickhousePool("snuba-query"),
        {"snuba-query": [DIST_QUERY]},
        id="All replicas fail, use backup",
    ),
    pytest.param(
        {
            1: [
                (ClickhouseNode("snuba-errors-0-0", 9000, 1, 1), False),
                (ClickhouseNode("snuba-errors-0-1", 9000, 1, 2), False),
            ],
            2: [(ClickhouseNode("snuba-errors-1-0", 9000, 2, 1), True)],
        },
        FakeClickhousePool("snuba-query"),
        {"snuba-errors-1-0": [LOCAL_QUERY], "snuba-query": [DIST_QUERY]},
        id="One shard only succeeds. Use backup and duplicate query",
    ),
]


@pytest.mark.parametrize(
    "nodes, backup_connection, expected_queries", TEST_LOCAL_EXECUTOR
)
def test_local_executor(
    nodes: Mapping[int, Sequence[Tuple[ClickhouseNode, bool]]],
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
        queries[connection.host].append(query)

    all_nodes: List[Tuple[ClickhouseNode, bool]] = []
    for shard_nodes in nodes.values():
        all_nodes.extend(shard_nodes)

    cluster = FakeClickhouseCluster(
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
        nodes=all_nodes,
    )
    insert_executor = ShardedExecutor(
        cluster=cluster,
        runner=run_query,
        thread_pool=ThreadPoolExecutor(),
        main_connection_pool=InOrderConnectionPool(cluster),
        local_table_name="errors_local",
        backup_executor=QueryNodeExecutor(
            runner=run_query,
            connection=backup_connection,
            table="errors_dist",
            metrics=DummyMetricsBackend(),
        ),
        metrics=DummyMetricsBackend(),
    )

    insert_executor.execute(
        replacement=LegacyReplacement(
            COUNT_QUERY_TEMPLATE,
            INSERT_QUERY_TEMPLATE,
            FINAL_QUERY_TEMPLATE,
            (NEEDS_FINAL, 1),
            REPLACEMENT_TYPE,
            REPLACEMENT_MESSAGE_METADATA,
        ),
        records_count=1,
    )

    assert queries == expected_queries
