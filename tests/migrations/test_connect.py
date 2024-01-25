from __future__ import annotations

from functools import reduce
from typing import Generator

import pytest

from snuba.clusters import cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.readiness_state import ReadinessState
from snuba.migrations.connect import (
    get_clickhouse_clusters_for_migration_group,
    get_clusters_for_readiness_states,
)
from snuba.migrations.groups import MigrationGroup

_ALL_STORAGE_SET_KEYS = set([s.value for s in StorageSetKey])
_REMAINING_STORAGE_SET_KEYS = _ALL_STORAGE_SET_KEYS - {"events", "querylog"}

_QUERYLOG_CLUSTER = cluster.ClickhouseCluster(
    **{
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
    }
)

_EVENTS_CLUSTER = cluster.ClickhouseCluster(
    **{
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
    }
)

_REST_CLUSTER = cluster.ClickhouseCluster(
    **{
        "host": "host_3",
        "port": "3",
        "user": "",
        "password": "",
        "database": "default",
        "http_port": "420",
        "storage_sets": _REMAINING_STORAGE_SET_KEYS,
        "single_node": True,
    }
)


TEST_CLUSTERS = [
    _QUERYLOG_CLUSTER,
    _EVENTS_CLUSTER,
    _REST_CLUSTER,
]


@pytest.fixture
def override_cluster(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator:
    with monkeypatch.context() as m:
        m.setattr(cluster, "CLUSTERS", TEST_CLUSTERS)
        m.setattr(
            cluster,
            "_STORAGE_SET_CLUSTER_MAP",
            {
                StorageSetKey.QUERYLOG: _QUERYLOG_CLUSTER,
                StorageSetKey.EVENTS: _EVENTS_CLUSTER,
                **{
                    StorageSetKey(s): _REST_CLUSTER for s in _REMAINING_STORAGE_SET_KEYS
                },
            },
        )

        yield


def test_get_clickhouse_clusters_for_migration_group(override_cluster) -> None:
    clusters = get_clickhouse_clusters_for_migration_group(MigrationGroup.QUERYLOG)
    assert len(clusters) == 1
    assert clusters[0] == _QUERYLOG_CLUSTER


@pytest.mark.parametrize(
    ["readiness_states", "clusters", "expected_clusters", "expected_storage_set_keys"],
    [
        pytest.param(
            [ReadinessState.PARTIAL],
            [_QUERYLOG_CLUSTER, _EVENTS_CLUSTER],
            [_QUERYLOG_CLUSTER],
            set([StorageSetKey.QUERYLOG]),
            id="partial only",
        ),
        pytest.param(
            [ReadinessState.COMPLETE],
            [_QUERYLOG_CLUSTER, _EVENTS_CLUSTER],
            [_EVENTS_CLUSTER],
            set([StorageSetKey.EVENTS]),
            id="complete only",
        ),
        pytest.param(
            [ReadinessState.COMPLETE, ReadinessState.PARTIAL],
            [_QUERYLOG_CLUSTER, _EVENTS_CLUSTER],
            [_QUERYLOG_CLUSTER, _EVENTS_CLUSTER],
            set([StorageSetKey.EVENTS, StorageSetKey.QUERYLOG]),
            id="complete and partial",
        ),
    ],
)
def test_get_clusters_for_readiness_states(
    readiness_states: list[ReadinessState],
    clusters: list[cluster.ClickhouseCluster],
    expected_clusters: list[cluster.ClickhouseCluster],
    expected_storage_set_keys: set[ReadinessState],
) -> None:

    result_clusters = get_clusters_for_readiness_states(readiness_states, clusters)
    assert result_clusters == expected_clusters
    assert (
        reduce(set.union, [rc.get_storage_set_keys() for rc in result_clusters])
        == expected_storage_set_keys
    )
