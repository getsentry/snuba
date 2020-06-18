from snuba.clickhouse.columns import Column, Nullable, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.operations import (
    CreateTable,
    DropTable,
    ReplacingMergeTree,
)


def test_create_table() -> None:
    columns = [
        Column("id", String()),
        Column("name", Nullable(String())),
        Column("version", UInt(64)),
    ]

    assert (
        CreateTable(
            StorageSetKey.EVENTS,
            "test_table",
            columns,
            ReplacingMergeTree("version"),
            "version",
        ).format_sql()
        == "CREATE TABLE IF NOT EXISTS test_table (id String, name Nullable(String), version UInt64) ENGINE ReplacingMergeTree(version) ORDER BY version;"
    )


def test_drop_table() -> None:
    assert (
        DropTable(StorageSetKey.EVENTS, "test_table").format_sql()
        == "DROP TABLE IF EXISTS test_table;"
    )
