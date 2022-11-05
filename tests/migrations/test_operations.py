import os
from logging import Logger
from typing import Callable, Sequence
from unittest.mock import Mock

import pytest

from snuba.clickhouse.columns import Column, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.context import Context
from snuba.migrations.groups import MigrationGroup, get_group_loader
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
    ModifyTableTTL,
    RemoveTableTTL,
    RenameTable,
    SqlOperation,
    TruncateTable,
)
from snuba.migrations.table_engines import ReplacingMergeTree
from snuba.migrations.validator import validate_migration_order
from snuba.utils.schemas import DateTime


def test_create_table() -> None:
    database = os.environ.get("CLICKHOUSE_DATABASE", "default")
    columns: Sequence[Column[Modifiers]] = [
        Column("id", String()),
        Column("name", String(Modifiers(nullable=True))),
        Column("version", UInt(64)),
    ]

    assert CreateTable(
        StorageSetKey.EVENTS,
        "test_table",
        columns,
        ReplacingMergeTree(
            storage_set=StorageSetKey.EVENTS,
            version_column="version",
            order_by="version",
            settings={"index_granularity": "256"},
        ),
    ).format_sql() in [
        "CREATE TABLE IF NOT EXISTS test_table (id String, name Nullable(String), version UInt64) ENGINE ReplacingMergeTree(version) ORDER BY version SETTINGS index_granularity=256;",
        "CREATE TABLE IF NOT EXISTS test_table (id String, name Nullable(String), version UInt64) ENGINE ReplicatedReplacingMergeTree('/clickhouse/tables/events/{shard}/"
        + f"{database}/test_table'"
        + ", '{replica}', version) ORDER BY version SETTINGS index_granularity=256;",
    ]


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


def test_truncate_table() -> None:
    assert (
        TruncateTable(StorageSetKey.EVENTS, "test_table").format_sql()
        == "TRUNCATE TABLE IF EXISTS test_table;"
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
        == "ALTER TABLE test_table ADD INDEX IF NOT EXISTS index_1 timestamp TYPE minmax GRANULARITY 3;"
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


def test_modify_ttl() -> None:
    """
    Test that modifying and removing of TTLs are formatted correctly.
    """
    columns: Sequence[Column[Modifiers]] = [
        Column("id", String()),
        Column("name", String(Modifiers(nullable=True))),
        Column("version", UInt(64)),
        Column("timestamp", DateTime()),
    ]

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
    )

    assert (
        ModifyTableTTL(
            StorageSetKey.EVENTS,
            "test_table",
            "timestamp",
            90,
        ).format_sql()
        == "ALTER TABLE test_table MODIFY TTL timestamp + toIntervalDay(90);"
    )

    assert (
        RemoveTableTTL(
            StorageSetKey.EVENTS,
            "test_table",
        ).format_sql()
        == "ALTER TABLE test_table REMOVE TTL;"
    )


def test_specify_order() -> None:
    """
    Test that specifying the migration order works when changing forwards_local_first
    """
    create_local_op = Mock(CreateTable)
    create_dist_op = Mock(CreateTable)
    drop_local_op = Mock(DropTable)
    drop_dist_op = Mock(DropTable)
    logger = Logger("test")
    context = Context("001", logger, lambda x: None)

    class TestMigration(migration.ClickhouseNodeMigration):
        blocking = False

        def forwards_local(self) -> Sequence[SqlOperation]:
            return [create_local_op]

        def backwards_local(self) -> Sequence[SqlOperation]:
            return [drop_local_op]

        def forwards_dist(self) -> Sequence[SqlOperation]:
            return [create_dist_op]

        def backwards_dist(self) -> Sequence[SqlOperation]:
            return [drop_dist_op]

    ops = [create_local_op, create_dist_op, drop_local_op, drop_dist_op]
    order = []
    for op in ops:

        def effect(op: Mock) -> Callable[[bool], None]:
            def add_op(local: bool) -> None:
                order.append(op)

            return add_op

        op.execute.side_effect = effect(op)

    test_migration = TestMigration()
    test_migration.forwards(context)
    assert order == [create_local_op, create_dist_op]
    order.clear()
    test_migration.backwards(context, False)
    assert order == [drop_dist_op, drop_local_op]
    order.clear()
    test_migration.forwards_local_first = False
    test_migration.forwards(context)
    assert order == [create_dist_op, create_local_op]
    order.clear()
    test_migration.backwards_local_first = True
    test_migration.backwards(context, False)
    assert order == [drop_local_op, drop_dist_op]


@pytest.mark.ci_only
def test_validate_migrations() -> None:
    for group in MigrationGroup:
        group_loader = get_group_loader(group)

        for migration_id in group_loader.get_migrations():
            snuba_migration = group_loader.load_migration(migration_id)
            if isinstance(snuba_migration, migration.ClickhouseNodeMigration):
                validate_migration_order(snuba_migration)
