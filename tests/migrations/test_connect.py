from functools import reduce

import pytest

from snuba.clusters.cluster import CLUSTERS, ClickhouseCluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.readiness_state import ReadinessState
from snuba.migrations.connect import (
    get_clickhouse_clusters_for_migration_group,
    get_clusters_for_readiness_states,
)
from snuba.migrations.groups import MigrationGroup


def test_get_clickhouse_clusters_for_migration_group():
    clusters = get_clickhouse_clusters_for_migration_group(MigrationGroup.EVENTS)
    assert len(clusters) == 1
    assert clusters[0] == CLUSTERS[0]


TEST_CLUSTERS = [
    ClickhouseCluster(**c)
    for c in [
        {
            "host": "host_1",
            "port": "1",
            "user": "",
            "password": "",
            "database": "default",
            "http_port": "420",
            "storage_sets": {
                "querylog",
            },
            "single_node": True,
        },
        {
            "host": "host_2",
            "port": "2",
            "user": "",
            "password": "",
            "database": "default",
            "http_port": "420",
            "storage_sets": {
                "events",
            },
            "single_node": True,
        },
    ]
]


@pytest.mark.parametrize(
    ["readiness_states", "clusters", "expected_clusters", "expected_storage_set_keys"],
    [
        pytest.param(
            [ReadinessState.PARTIAL],
            TEST_CLUSTERS,
            [TEST_CLUSTERS[0]],
            set([StorageSetKey.QUERYLOG]),
            id="partial only",
        ),
        pytest.param(
            [ReadinessState.COMPLETE],
            TEST_CLUSTERS,
            [TEST_CLUSTERS[1]],
            set([StorageSetKey.EVENTS]),
            id="complete only",
        ),
        pytest.param(
            [ReadinessState.COMPLETE, ReadinessState.PARTIAL],
            TEST_CLUSTERS,
            TEST_CLUSTERS,
            set([StorageSetKey.EVENTS, StorageSetKey.QUERYLOG]),
            id="complete and partial",
        ),
    ],
)
def test_get_clusters_for_readiness_states(
    readiness_states, clusters, expected_clusters, expected_storage_set_keys
) -> None:

    result_clusters = get_clusters_for_readiness_states(readiness_states, clusters)
    assert result_clusters == expected_clusters
    assert (
        reduce(set.union, [rc.get_storage_set_keys() for rc in result_clusters])
        == expected_storage_set_keys
    )
