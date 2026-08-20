from __future__ import annotations

from functools import reduce
from typing import Any
from unittest import mock

import pytest

from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.pool import ClickhouseResult
from snuba.clusters import cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.readiness_state import ReadinessState
from snuba.migrations.connect import (
    check_clickhouse_connections,
    get_clickhouse_clusters_for_migration_group,
    get_clusters_for_readiness_states,
)
from snuba.migrations.errors import InvalidClickhouseVersion
from snuba.migrations.groups import MigrationGroup

_ALL_STORAGE_SET_KEYS = {s.value for s in StorageSetKey}
_REMAINING_STORAGE_SET_KEYS = _ALL_STORAGE_SET_KEYS - {"events", "querylog"}

_QUERYLOG_CLUSTER = cluster.ClickhouseCluster(
    host="host_1",
    port=1,
    user="",
    password="",
    database="default",
    secure=False,
    ca_certs=None,
    verify=False,
    storage_sets={
        "querylog",
    },
    single_node=True,
)

_EVENTS_CLUSTER = cluster.ClickhouseCluster(
    host="host_2",
    port=2,
    user="",
    password="",
    database="default",
    secure=False,
    ca_certs=None,
    verify=False,
    storage_sets={
        "events",
    },
    single_node=True,
)

_REST_CLUSTER = cluster.ClickhouseCluster(
    host="host_3",
    port=3,
    user="",
    password="",
    database="default",
    secure=False,
    ca_certs=None,
    verify=False,
    storage_sets=_REMAINING_STORAGE_SET_KEYS,
    single_node=True,
)


TEST_CLUSTERS = [
    _QUERYLOG_CLUSTER,
    _EVENTS_CLUSTER,
    _REST_CLUSTER,
]


@pytest.fixture
def override_cluster(
    monkeypatch: pytest.MonkeyPatch,
) -> Any:
    with monkeypatch.context() as m:
        m.setattr(cluster, "CLUSTERS", TEST_CLUSTERS)
        m.setattr(
            cluster,
            "_STORAGE_SET_CLUSTER_MAP",
            {
                StorageSetKey.QUERYLOG: _QUERYLOG_CLUSTER,
                StorageSetKey.EVENTS: _EVENTS_CLUSTER,
                **{StorageSetKey(s): _REST_CLUSTER for s in _REMAINING_STORAGE_SET_KEYS},
            },
        )
        yield


def test_get_clickhouse_clusters_for_migration_group(override_cluster: Any) -> None:
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
            {StorageSetKey.QUERYLOG},
            id="partial only",
        ),
        pytest.param(
            [ReadinessState.COMPLETE],
            [_QUERYLOG_CLUSTER, _EVENTS_CLUSTER],
            [_EVENTS_CLUSTER],
            {StorageSetKey.EVENTS},
            id="complete only",
        ),
        pytest.param(
            [ReadinessState.COMPLETE, ReadinessState.PARTIAL],
            [_QUERYLOG_CLUSTER, _EVENTS_CLUSTER],
            [_QUERYLOG_CLUSTER, _EVENTS_CLUSTER],
            {StorageSetKey.EVENTS, StorageSetKey.QUERYLOG},
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


def _cluster_with_pool(pool: mock.Mock) -> mock.Mock:
    cluster_mock = mock.Mock()
    cluster_mock.get_query_connection.return_value = pool
    return cluster_mock


def _pool_returning_version(version: str = "25.8.16.10001") -> mock.Mock:
    pool = mock.Mock()
    pool.execute.return_value = ClickhouseResult(results=[[version]])
    return pool


def test_check_clickhouse_connections_retries_transient_errors_then_succeeds() -> None:
    pool = mock.Mock()
    pool.execute.side_effect = [
        ClickhouseError("connection reset by peer", code=-1),
        ClickhouseResult(results=[["25.8.16.10001"]]),
    ]

    with mock.patch("snuba.migrations.connect.time.sleep") as sleep:
        check_clickhouse_connections(
            [_cluster_with_pool(pool)], max_attempts=3, retry_delay_seconds=0
        )

    assert pool.execute.call_count == 2
    sleep.assert_called_once_with(0)


def test_check_clickhouse_connections_retries_are_per_cluster() -> None:
    flaky = mock.Mock()
    flaky.execute.side_effect = [
        ClickhouseError("connection reset by peer", code=-1),
        ClickhouseResult(results=[["25.8.16.10001"]]),
    ]

    with mock.patch("snuba.migrations.connect.time.sleep"):
        check_clickhouse_connections(
            [_cluster_with_pool(flaky), _cluster_with_pool(_pool_returning_version())],
            max_attempts=2,
            retry_delay_seconds=0,
        )

    assert flaky.execute.call_count == 2


def test_check_clickhouse_connections_raises_after_max_attempts() -> None:
    pool = mock.Mock()
    pool.execute.side_effect = ClickhouseError("connection reset by peer", code=-1)

    with (
        mock.patch("snuba.migrations.connect.time.sleep"),
        pytest.raises(ClickhouseError, match="connection reset by peer"),
    ):
        check_clickhouse_connections(
            [_cluster_with_pool(pool)], max_attempts=3, retry_delay_seconds=0
        )

    assert pool.execute.call_count == 3


@pytest.mark.parametrize(
    "version",
    ["10.0.0.0", "23.8.11.29", "25.3.8.10041"],
    ids=["ancient", "former-min-23.8", "former-supported-25.3"],
)
def test_check_clickhouse_connections_does_not_retry_invalid_version(version: str) -> None:
    pool = _pool_returning_version(version)

    with (
        mock.patch("snuba.migrations.connect.time.sleep") as sleep,
        pytest.raises(InvalidClickhouseVersion),
    ):
        check_clickhouse_connections(
            [_cluster_with_pool(pool)], max_attempts=5, retry_delay_seconds=0
        )

    assert pool.execute.call_count == 1
    sleep.assert_not_called()
