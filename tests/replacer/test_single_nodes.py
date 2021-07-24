from typing import Mapping, Sequence, Tuple

import pytest

from snuba.clusters import cluster
from snuba.clusters.cluster import ClickhouseClientSettings, ClickhouseNode
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.errors_replacer import NEEDS_FINAL, LegacyReplacement
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.replacer import ReplacerWorker
from snuba.state import set_config
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from tests.clusters.fake_cluster import FakeClickhouseCluster, ServerExplodedException

TEST_CASES = [
    pytest.param(
        "[100,1]",
        {
            "query_node": [
                "SELECT count() FROM errors_dist FINAL WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'",
            ],
            "storage-0-0": [
                """\
INSERT INTO errors_local (project_id, timestamp, event_id)
SELECT project_id, timestamp, event_id, group_id, primary_hash
FROM errors_local FINAL
WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'
""",
            ],
            "storage-1-0": [
                """\
INSERT INTO errors_local (project_id, timestamp, event_id)
SELECT project_id, timestamp, event_id, group_id, primary_hash
FROM errors_local FINAL
WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'
""",
            ],
            "storage-2-0": [
                """\
INSERT INTO errors_local (project_id, timestamp, event_id)
SELECT project_id, timestamp, event_id, group_id, primary_hash
FROM errors_local FINAL
WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'
""",
            ],
        },
        id="Replacements through storage nodes",
    ),
    pytest.param(
        "",
        {
            "query_node": [
                "SELECT count() FROM errors_dist FINAL WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'",
                """\
INSERT INTO errors_dist (project_id, timestamp, event_id)
SELECT project_id, timestamp, event_id, group_id, primary_hash
FROM errors_dist FINAL
WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'
""",
            ]
        },
        id="Replacements through query node",
    ),
    pytest.param(
        "[10]",
        {
            "query_node": [
                "SELECT count() FROM errors_dist FINAL WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'",
                """\
INSERT INTO errors_dist (project_id, timestamp, event_id)
SELECT project_id, timestamp, event_id, group_id, primary_hash
FROM errors_dist FINAL
WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'
""",
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
    expected_queries: Mapping[
        Tuple[ClickhouseNode, ClickhouseClientSettings], Sequence[str]
    ],
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
