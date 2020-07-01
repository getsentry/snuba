import pytest

from snuba.clusters.cluster import ClickhouseCluster
from snuba.migrations import table_engines


single_node_cluster = ClickhouseCluster(
    host="host_1",
    port=9000,
    user="default",
    password="",
    database="default",
    http_port=8123,
    storage_sets={"events"},
    single_node=True,
)

multi_node_cluster = ClickhouseCluster(
    host="host_2",
    port=9000,
    user="default",
    password="",
    database="default",
    http_port=8123,
    storage_sets={"events"},
    single_node=False,
    cluster_name="cluster_1",
    distributed_cluster_name="dist_hosts",
)

merge_test_cases = [
    pytest.param(
        table_engines.MergeTree(order_by="timestamp"),
        "MergeTree() ORDER BY timestamp",
        "ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard})/test_table', '{replica}') ORDER BY timestamp",
        id="Merge tree",
    ),
    pytest.param(
        table_engines.MergeTree(order_by="date", settings={"index_granularity": "256"}),
        "MergeTree() ORDER BY date SETTINGS index_granularity=256",
        "ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard})/test_table', '{replica}') ORDER BY date SETTINGS index_granularity=256",
        id="Merge tree with settings",
    ),
    pytest.param(
        table_engines.ReplacingMergeTree(
            version_column="timestamp",
            order_by="timestamp",
            partition_by="(toMonday(timestamp))",
            sample_by="id",
            ttl="timestamp + INTERVAL 1 MONTH",
        ),
        "ReplacingMergeTree(timestamp) ORDER BY timestamp PARTITION BY (toMonday(timestamp)) SAMPLE BY id TTL timestamp + INTERVAL 1 MONTH",
        "ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard})/test_table', '{replica}', timestamp) ORDER BY timestamp PARTITION BY (toMonday(timestamp)) SAMPLE BY id TTL timestamp + INTERVAL 1 MONTH",
        id="Replicated merge tree with partition, sample, ttl clauses",
    ),
]


@pytest.mark.parametrize("engine, single_node_sql, multi_node_sql", merge_test_cases)
def test_merge_table(
    engine: table_engines.TableEngine, single_node_sql: str, multi_node_sql: str
) -> None:
    assert engine.get_sql(single_node_cluster, "test_table") == single_node_sql
    assert engine.get_sql(multi_node_cluster, "test_table") == multi_node_sql


dist_test_cases = [
    pytest.param(
        table_engines.Distributed(
            local_table_name="test_table_local", sharding_key="event_id"
        ),
        "Distributed(cluster_1, default, test_table_local, event_id)",
        id="Disributed",
    )
]


@pytest.mark.parametrize("engine, sql", dist_test_cases)
def test_distributed(engine: table_engines.TableEngine, sql: str) -> None:
    with pytest.raises(AssertionError):
        engine.get_sql(single_node_cluster, "test_table")
    assert engine.get_sql(multi_node_cluster, "test_table") == sql
