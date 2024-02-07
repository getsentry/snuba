import os
from logging import Logger
from typing import Callable, Sequence
from unittest.mock import Mock, patch

import pytest

from snuba.clickhouse.columns import Column, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.context import Context
from snuba.migrations.errors import NodesNotFound
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
    ModifyTableSettings,
    ModifyTableTTL,
    OperationTarget,
    RemoveTableTTL,
    RenameTable,
    ResetTableSettings,
    SqlOperation,
    TruncateTable,
)
from snuba.migrations.table_engines import ReplacingMergeTree
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


def test_create_table_with_column_ttl() -> None:
    database = os.environ.get("CLICKHOUSE_DATABASE", "default")
    columns: Sequence[Column[Modifiers]] = [
        Column("id", String()),
        Column("name", String(Modifiers(nullable=True))),
        Column("version", UInt(64)),
        Column(
            "restricted",
            String(Modifiers(low_cardinality=True, ttl="timestamp + toIntervalDay(1)")),
        ),
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
        "CREATE TABLE IF NOT EXISTS test_table (id String, name Nullable(String), version UInt64, restricted LowCardinality(String) TTL timestamp + toIntervalDay(1)) ENGINE ReplacingMergeTree(version) ORDER BY version SETTINGS index_granularity=256;",
        "CREATE TABLE IF NOT EXISTS test_table (id String, name Nullable(String), version UInt64, restricted LowCardinality(String) TTL timestamp + toIntervalDay(1)) ENGINE ReplicatedReplacingMergeTree('/clickhouse/tables/events/{shard}/"
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

    class TestMigration(migration.ClickhouseNodeMigrationLegacy):
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

        def effect(op: Mock) -> Callable[[], None]:
            def add_op() -> None:
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


def test_new_create_table() -> None:
    database = os.environ.get("CLICKHOUSE_DATABASE", "default")
    columns: Sequence[Column[Modifiers]] = [
        Column("id", String()),
        Column("name", String(Modifiers(nullable=True))),
        Column("version", UInt(64)),
    ]

    op = CreateTable(
        StorageSetKey.EVENTS,
        "test_table",
        columns,
        ReplacingMergeTree(
            storage_set=StorageSetKey.EVENTS,
            version_column="version",
            order_by="version",
            settings={"index_granularity": "256"},
        ),
        target=OperationTarget.DISTRIBUTED,
    )

    assert op.format_sql() in [
        "CREATE TABLE IF NOT EXISTS test_table (id String, name Nullable(String), version UInt64) ENGINE ReplacingMergeTree(version) ORDER BY version SETTINGS index_granularity=256;",
        "CREATE TABLE IF NOT EXISTS test_table (id String, name Nullable(String), version UInt64) ENGINE ReplicatedReplacingMergeTree('/clickhouse/tables/events/{shard}/"
        + f"{database}/test_table'"
        + ", '{replica}', version) ORDER BY version SETTINGS index_granularity=256;",
    ]


def test_new_add_column() -> None:
    dist_op = AddColumn(
        StorageSetKey.EVENTS,
        "test_table",
        Column("new_column", String()),
        after="id",
        target=OperationTarget.DISTRIBUTED,
    )

    assert (
        dist_op.format_sql()
        == "ALTER TABLE test_table ADD COLUMN IF NOT EXISTS new_column String AFTER id;"
    )

    local_op = AddColumn(
        StorageSetKey.EVENTS,
        "test_table",
        Column("new_column", String()),
        after="id",
        target=OperationTarget.LOCAL,
    )

    assert (
        local_op.format_sql()
        == "ALTER TABLE test_table ADD COLUMN IF NOT EXISTS new_column String AFTER id;"
    )


def test_new_drop_column() -> None:
    local_op = DropColumn(
        StorageSetKey.EVENTS,
        "test_table",
        "test_column",
        target=OperationTarget.LOCAL,
    )

    assert (
        local_op.format_sql()
        == "ALTER TABLE test_table DROP COLUMN IF EXISTS test_column;"
    )

    dist_op = DropColumn(
        StorageSetKey.EVENTS,
        "test_dist_table",
        "test_column",
        target=OperationTarget.DISTRIBUTED,
    )
    assert (
        dist_op.format_sql()
        == "ALTER TABLE test_dist_table DROP COLUMN IF EXISTS test_column;"
    )


def test_refactored_migration() -> None:
    """
    Test that specifying the migration order works when changing
    """
    create_local_op = Mock(CreateTable)
    create_dist_op = Mock(CreateTable)
    drop_local_op = Mock(DropTable)
    drop_dist_op = Mock(DropTable)
    logger = Logger("test")
    context = Context("001", logger, lambda x: None)

    class TestMigration(migration.ClickhouseNodeMigration):
        blocking = False

        def forwards_ops(self) -> Sequence[SqlOperation]:
            return [create_local_op, create_dist_op]

        def backwards_ops(self) -> Sequence[SqlOperation]:
            return [drop_dist_op, drop_local_op]

    ops = [create_local_op, create_dist_op, drop_local_op, drop_dist_op]
    order = []
    for op in ops:

        def effect(op: Mock) -> Callable[[], None]:
            def add_op() -> None:
                order.append(op)

            return add_op

        op.execute.side_effect = effect(op)

    test_migration = TestMigration()
    test_migration.forwards(context)
    assert order == [create_local_op, create_dist_op]
    order.clear()
    test_migration.backwards(context, False)
    assert order == [drop_dist_op, drop_local_op]


def test_modify_settings() -> None:
    assert (
        ModifyTableSettings(
            StorageSetKey.EVENTS,
            "test_table",
            {
                "test_key": "test_val",
                "test_int_key": 0,
            },
        ).format_sql()
        == "ALTER TABLE test_table MODIFY SETTING test_key = test_val, test_int_key = 0;"
    )


def test_reset_settings() -> None:
    assert (
        ResetTableSettings(
            StorageSetKey.EVENTS,
            "test_table",
            [
                "setting_a",
                "setting_b",
            ],
        ).format_sql()
        == "ALTER TABLE test_table RESET SETTING setting_a, setting_b;"
    )


@patch("snuba.migrations.operations.get_cluster")
def test_no_nodes_found(mock_get_cluster: Mock) -> None:
    cluster = Mock()
    mock_get_cluster.return_value = cluster
    cluster.get_host.return_value = "127.0.0.1"
    cluster.get_port.return_value = "9000"
    cluster.get_clickhouse_cluster_name.return_value = "query_cluster"

    cluster.get_local_nodes.return_value = []
    cluster.get_distributed_nodes.return_value = []
    # Local op must have local nodes present
    local_op = RenameTable(
        StorageSetKey.EVENTS, "old_table", "new_table", OperationTarget.LOCAL
    )
    with pytest.raises(NodesNotFound):
        local_op.execute()

    # Dist op can have missing dist nodes if in single node mode
    dist_op = RenameTable(
        StorageSetKey.EVENTS, "old_table", "new_table", OperationTarget.DISTRIBUTED
    )
    dist_op.execute()

    # Dist op must have dist nodes present if NOT single node mode
    cluster.is_single_node.return_value = False
    with pytest.raises(NodesNotFound):
        dist_op.execute()
