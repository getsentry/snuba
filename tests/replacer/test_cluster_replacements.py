from __future__ import annotations

from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor
from typing import (
    Any,
    Callable,
    Generator,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
)

import pytest

from snuba.clickhouse.native import ClickhousePool
from snuba.clusters import cluster
from snuba.clusters.cluster import ClickhouseNode
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.processor import ReplacementType
from snuba.replacer import (
    InOrderConnectionPool,
    QueryNodeExecutor,
    ReplacerWorker,
    ShardedExecutor,
)
from snuba.replacers.errors_replacer import (
    NeedsFinal,
    QueryTimeFlags,
    Replacement,
    ReplacementContext,
)
from snuba.replacers.replacer_processor import (
    ReplacementMessage,
    ReplacementMessageMetadata,
)
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
def override_cluster(
    monkeypatch: pytest.MonkeyPatch,
    clickhouse_db: None,
    redis_db: None,
) -> Generator[Callable[[bool], FakeClickhouseCluster], None, None]:
    with monkeypatch.context() as m:

        def override(healthy: bool) -> FakeClickhouseCluster:
            test_cluster = _build_cluster(healthy=healthy)
            m.setattr(cluster, "CLUSTERS", [test_cluster])
            m.setattr(
                cluster,
                "_STORAGE_SET_CLUSTER_MAP",
                {StorageSetKey.EVENTS: cluster.CLUSTERS[0]},
            )
            return test_cluster

        yield override


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


class DummyReplacement(Replacement):
    @classmethod
    def parse_message(
        cls,
        message: ReplacementMessage[Any],
        context: ReplacementContext,
    ) -> Optional[DummyReplacement]:
        return cls()

    def get_count_query(self, table_name: str) -> Optional[str]:
        return f"SELECT count() FROM {table_name} FINAL WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'"

    def get_insert_query(self, table_name: str) -> Optional[str]:
        required_columns = "project_id, timestamp, event_id"
        select_columns = "project_id, timestamp, event_id, group_id, primary_hash"

        return (
            f"INSERT INTO {table_name} ({required_columns})\n"
            f"SELECT {select_columns}\n"
            f"FROM {table_name} FINAL\n"
            f"WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'\n"
        )

    def get_query_time_flags(self) -> QueryTimeFlags:
        return NeedsFinal()

    @classmethod
    def get_replacement_type(cls) -> ReplacementType:
        # Arbitrary replacement type, no impact on tests
        return ReplacementType.EXCLUDE_GROUPS

    def get_project_id(self) -> int:
        return 1


@pytest.mark.parametrize(
    "override_fixture, write_node_replacements_projects, expected_queries", TEST_CASES
)
@pytest.mark.redis_db
@pytest.mark.clickhouse_db
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

    replacer.flush_batch([(ReplacementMessageMetadata(0, 0, ""), DummyReplacement())])

    queries = test_cluster.get_queries()
    assert queries == expected_queries


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
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
            [(ReplacementMessageMetadata(0, 0, ""), DummyReplacement())]
        )


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
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
    replacement = DummyReplacement()
    replacer.flush_batch(
        [
            (ReplacementMessageMetadata(0, 0, ""), replacement),
            (ReplacementMessageMetadata(0, 0, ""), replacement),
        ]
    )

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
@pytest.mark.redis_db
@pytest.mark.clickhouse_db
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
        replacement=DummyReplacement(),
        records_count=1,
    )

    assert queries == expected_queries
