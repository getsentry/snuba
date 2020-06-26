import pytest
from typing import Any
from unittest.mock import patch

from snuba.clusters.cluster import ClickhouseCluster
from snuba.clusters.storage_sets import StorageSetKey


@patch("snuba.clusters.cluster.get_cluster")
def test_table_engines(get_cluster_mock: Any) -> None:
    single_node_cluster = ClickhouseCluster(
        "host", 9000, "user", "pass", "default", 3000, {"events"}, True
    )
    multi_node_cluster = ClickhouseCluster(
        "host",
        9000,
        "user",
        "pass",
        "default",
        3000,
        {"transactions"},
        False,
        "cluster_1",
        "dist_cluster_1",
    )

    get_cluster_mock.side_effect = (
        lambda storage_set: single_node_cluster
        if storage_set == StorageSetKey.EVENTS
        else multi_node_cluster
    )

    from snuba.migrations.table_engines import (
        Distributed,
        MergeTree,
        ReplacingMergeTree,
    )

    for merge_test_case in [
        (
            MergeTree(order_by="timestamp"),
            "MergeTree() ORDER BY timestamp",
            "ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard})/test_table', '{replica}') ORDER BY timestamp",
        ),
        (
            MergeTree(order_by="date", settings={"index_granularity": "256"}),
            "MergeTree() ORDER BY date SETTINGS index_granularity=256",
            "ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard})/test_table', '{replica}') ORDER BY date SETTINGS index_granularity=256",
        ),
        (
            ReplacingMergeTree(
                version_column="timestamp",
                order_by="timestamp",
                partition_by="(toMonday(timestamp))",
                sample_by="id",
                ttl="timestamp + INTERVAL 1 MONTH",
            ),
            "ReplacingMergeTree(timestamp) ORDER BY timestamp PARTITION BY (toMonday(timestamp)) SAMPLE BY id TTL timestamp + INTERVAL 1 MONTH",
            "ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard})/test_table', '{replica}', timestamp) ORDER BY timestamp PARTITION BY (toMonday(timestamp)) SAMPLE BY id TTL timestamp + INTERVAL 1 MONTH",
        ),
    ]:
        assert (
            merge_test_case[0].get_sql(StorageSetKey.EVENTS, "test_table")
            == merge_test_case[1]
        )
        assert (
            merge_test_case[0].get_sql(StorageSetKey.TRANSACTIONS, "test_table")
            == merge_test_case[2]
        )

    for dist_test_case in [
        (
            Distributed(local_table_name="test_table_local", sharding_key="event_id"),
            "Distributed(cluster_1, default, test_table_local, event_id)",
        )
    ]:
        with pytest.raises(AssertionError):
            dist_test_case[0].get_sql(StorageSetKey.EVENTS, "test_table")
        assert (
            dist_test_case[0].get_sql(StorageSetKey.TRANSACTIONS, "test_table")
            == dist_test_case[1]
        )
