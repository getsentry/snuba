import pytest

from snuba.migrations.table_engines import MergeTree, ReplacingMergeTree, TableEngine


test_cases = [
    pytest.param(
        MergeTree(order_by="timestamp"),
        "MergeTree() ORDER BY timestamp",
        "ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard})/test_table', '{replica}') ORDER BY timestamp",
        id="Merge tree",
    ),
    pytest.param(
        MergeTree(order_by="date", settings={"index_granularity": "256"}),
        "MergeTree() ORDER BY date SETTINGS index_granularity=256",
        "ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard})/test_table', '{replica}') ORDER BY date SETTINGS index_granularity=256",
        id="Merge tree with settings",
    ),
    pytest.param(
        ReplacingMergeTree(
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


@pytest.mark.parametrize("engine, single_node_sql, multi_node_sql", test_cases)
def test_table_engines(
    engine: TableEngine, single_node_sql: str, multi_node_sql: str
) -> None:
    assert engine.get_sql(True, "test_table") == single_node_sql
    assert engine.get_sql(False, "test_table") == multi_node_sql
