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
from tests.clusters.fake_cluster import FakeClickhouseCluster

TEST_CASES = [
    pytest.param(
        False,
        {
            (
                ClickhouseNode("query_node", 9000, None, None),
                ClickhouseClientSettings.REPLACE,
            ): [
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
        True,
        {
            (
                ClickhouseNode("query_node", 9000, None, None),
                ClickhouseClientSettings.REPLACE,
            ): [
                "SELECT count() FROM errors_dist FINAL WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'",
            ],
            (
                ClickhouseNode("storage-0-0", 9000, 1, 1),
                ClickhouseClientSettings.REPLACE,
            ): [
                """\
INSERT INTO errors_local (project_id, timestamp, event_id)
SELECT project_id, timestamp, event_id, group_id, primary_hash
FROM errors_local FINAL
WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'
""",
            ],
            (
                ClickhouseNode("storage-1-0", 9000, 2, 1),
                ClickhouseClientSettings.REPLACE,
            ): [
                """\
INSERT INTO errors_local (project_id, timestamp, event_id)
SELECT project_id, timestamp, event_id, group_id, primary_hash
FROM errors_local FINAL
WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'
""",
            ],
            (
                ClickhouseNode("storage-2-0", 9000, 3, 1),
                ClickhouseClientSettings.REPLACE,
            ): [
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
]


@pytest.mark.parametrize("single_node_config, expected_queries", TEST_CASES)
def test_write_single_node(
    single_node_config: bool,
    expected_queries: Mapping[
        Tuple[ClickhouseNode, ClickhouseClientSettings], Sequence[str]
    ],
) -> None:
    """
    Test the execution of replacement queries on both storage nodes and
    query nodes.
    """

    test_cluster = FakeClickhouseCluster(
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
    )
    # Cannot use patch properly for this as I cannot patch the use case
    # with a new instance for each parameter in the parametrized test.
    current_clusters = cluster.CLUSTERS
    current_mapping = cluster._STORAGE_SET_CLUSTER_MAP
    cluster.CLUSTERS = [test_cluster]
    cluster._STORAGE_SET_CLUSTER_MAP = {StorageSetKey.EVENTS: cluster.CLUSTERS[0]}

    set_config("write_node_replacements_projects", str(int(single_node_config)))
    replacer = ReplacerWorker(
        get_writable_storage(StorageKey.ERRORS), DummyMetricsBackend()
    )
    count_query_template = "SELECT count() FROM %(table_name)s FINAL WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'"

    insert_query_template = """\
INSERT INTO %(table_name)s (%(required_columns)s)
SELECT %(select_columns)s
FROM %(table_name)s FINAL
WHERE event_id = '6f0ccc03-6efb-4f7c-8005-d0c992106b31'
"""

    final_query_args = {
        "required_columns": "project_id, timestamp, event_id",
        "select_columns": "project_id, timestamp, event_id, group_id, primary_hash",
    }

    replacer.flush_batch(
        [
            LegacyReplacement(
                count_query_template,
                insert_query_template,
                final_query_args,
                (NEEDS_FINAL, 1),
            )
        ]
    )

    queries = test_cluster.get_queries()
    assert queries == expected_queries

    cluster.CLUSTERS = current_clusters
    cluster._STORAGE_SET_CLUSTER_MAP = current_mapping
