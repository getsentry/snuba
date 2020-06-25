from snuba.migrations.table_engines import MergeTree, ReplacingMergeTree


def test_table_engines() -> None:
    assert (
        MergeTree(order_by="timestamp",).get_sql() == "MergeTree() ORDER BY timestamp"
    )

    assert (
        MergeTree(order_by="date", settings={"index_granularity": "256"}).get_sql()
        == "MergeTree() ORDER BY date SETTINGS index_granularity=256"
    )

    assert (
        ReplacingMergeTree(
            version_column="timestamp",
            order_by="timestamp",
            partition_by="(toMonday(timestamp))",
            sample_by="id",
            ttl="timestamp + INTERVAL 1 MONTH",
        ).get_sql()
        == "ReplacingMergeTree(timestamp) ORDER BY timestamp PARTITION BY (toMonday(timestamp)) SAMPLE BY id TTL timestamp + INTERVAL 1 MONTH"
    )
