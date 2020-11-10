from snuba.clickhouse.columns import Column
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.operations import (
    AddColumn,
    AddIndex,
    CreateMaterializedView,
    CreateTable,
    DropColumn,
    DropIndex,
    DropTable,
    InsertIntoSelect,
    ModifyColumn,
    RenameTable,
)
from snuba.migrations.table_engines import ReplacingMergeTree


def test_create_table() -> None:
    columns = [
        Column("id", String()),
        Column("name", String(Modifiers(nullable=True))),
        Column("version", UInt(64)),
    ]

    assert (
        CreateTable(
            StorageSetKey.EVENTS,
            "test_table",
            columns,
            ReplacingMergeTree(
                storage_set=StorageSetKey.EVENTS,
                version_column="version",
                order_by="version",
                settings={"index_granularity": "256"},
            ),
        ).format_sql()
        == "CREATE TABLE IF NOT EXISTS test_table (id String, name Nullable(String), version UInt64) ENGINE ReplacingMergeTree(version) ORDER BY version SETTINGS index_granularity=256;"
    )


def test_create_materialized_view() -> None:
    assert (
        CreateMaterializedView(
            StorageSetKey.EVENTS,
            "test_table_mv",
            "test_table_dest",
            [Column("id", String())],
            "SELECT id, count() as count FROM test_table_local GROUP BY id",
        ).format_sql()
        == "CREATE MATERIALIZED VIEW IF NOT EXISTS test_table_mv TO test_table_dest (id String) AS SELECT id, count() as count FROM test_table_local GROUP BY id;"
    )


def test_rename_table() -> None:
    assert (
        RenameTable(StorageSetKey.EVENTS, "old_table", "new_table").format_sql()
        == "RENAME TABLE old_table TO new_table;"
    )


def test_drop_table() -> None:
    assert (
        DropTable(StorageSetKey.EVENTS, "test_table").format_sql()
        == "DROP TABLE IF EXISTS test_table;"
    )


def test_add_column() -> None:
    assert (
        AddColumn(
            StorageSetKey.EVENTS,
            "test_table",
            Column("test", String(Modifiers(nullable=True))),
            after="id",
        ).format_sql()
        == "ALTER TABLE test_table ADD COLUMN IF NOT EXISTS test Nullable(String) AFTER id;"
    )


def test_drop_column() -> None:
    assert (
        DropColumn(StorageSetKey.EVENTS, "test_table", "test").format_sql()
        == "ALTER TABLE test_table DROP COLUMN IF EXISTS test;"
    )


def test_modify_column() -> None:
    assert (
        ModifyColumn(
            StorageSetKey.EVENTS, "test_table", Column("test", String())
        ).format_sql()
        == "ALTER TABLE test_table MODIFY COLUMN test String;"
    )


def test_add_index() -> None:
    assert (
        AddIndex(
            StorageSetKey.EVENTS,
            "test_table",
            "index_1",
            "timestamp",
            "minmax",
            3,
            after=None,
        ).format_sql()
        == "ALTER TABLE test_table ADD INDEX index_1 timestamp TYPE minmax GRANULARITY 3;"
    )


def test_drop_index() -> None:
    assert (
        DropIndex(StorageSetKey.EVENTS, "test_table", "index_1").format_sql()
        == "ALTER TABLE test_table DROP INDEX IF EXISTS index_1;"
    )


def test_insert_into_select() -> None:
    assert (
        InsertIntoSelect(
            StorageSetKey.EVENTS, "dest", ["a2", "b2"], "src", ["a1", "b1"]
        ).format_sql()
        == "INSERT INTO dest (a2, b2) SELECT a1, b1 FROM src;"
    )
