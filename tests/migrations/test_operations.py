import pytest

from snuba.clickhouse.columns import Column, Nullable, String, UInt
from snuba.migrations.errors import InvalidStorageSet
from snuba.migrations.operations import (
    CreateMaterializedView,
    CreateTable,
    DropTable,
    RunSql,
)
from snuba.migrations.table_engines import ReplacingMergeTree


def test_create_table() -> None:
    columns = [
        Column("id", String()),
        Column("name", Nullable(String())),
        Column("version", UInt(64)),
    ]

    assert (
        CreateTable(
            "events",
            "test_table",
            columns,
            ReplacingMergeTree(
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
            "events",
            "test_table_mv",
            "test_table_dest",
            [Column("id", String())],
            "SELECT id, count() as count FROM test_table_local GROUP BY id",
        ).format_sql()
        == "CREATE MATERIALIZED VIEW IF NOT EXISTS test_table_mv TO test_table_dest (id String) AS SELECT id, count() as count FROM test_table_local GROUP BY id;"
    )


def test_drop_table() -> None:
    assert (
        DropTable("events", "test_table").format_sql()
        == "DROP TABLE IF EXISTS test_table;"
    )


def test_invalid_storage_set() -> None:
    with pytest.raises(InvalidStorageSet):
        RunSql("invalid_storage_set", "SHOW TABLES;").format_sql()
