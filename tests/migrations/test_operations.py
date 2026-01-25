import os
from logging import Logger
from typing import Callable, Sequence
from unittest import mock
from unittest.mock import Mock, patch

import pytest

from snuba.clickhouse.columns import Column, String, UInt
from snuba.clusters.cluster import ClickhouseCluster, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.context import Context
from snuba.migrations.operations import (
    AddColumn,
    AddIndex,
    AddIndices,
    AddIndicesData,
    CreateMaterializedView,
    CreateTable,
    DropColumn,
    DropIndex,
    DropIndices,
    DropTable,
    InsertIntoSelect,
    ModifyColumn,
    ModifyTableSettings,
    ModifyTableTTL,
    OperationMissingNodes,
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
        == "DROP TABLE IF EXISTS test_table SYNC;"
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
        ModifyColumn(StorageSetKey.EVENTS, "test_table", Column("test", String())).format_sql()
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


def test_drop_index_async() -> None:
    assert (
        DropIndex(StorageSetKey.EVENTS, "test_table", "index_1", run_async=True).format_sql()
        == "ALTER TABLE test_table DROP INDEX IF EXISTS index_1 SETTINGS mutations_sync=0;"
    )


def test_drop_indices() -> None:
    assert (
        DropIndices(StorageSetKey.EVENTS, "test_table", ["index_1", "index_2"]).format_sql()
        == "ALTER TABLE test_table DROP INDEX IF EXISTS index_1, DROP INDEX IF EXISTS index_2;"
    )


def test_drop_indices_async() -> None:
    assert (
        DropIndices(
            StorageSetKey.EVENTS, "test_table", ["index_1", "index_2"], run_async=True
        ).format_sql()
        == "ALTER TABLE test_table DROP INDEX IF EXISTS index_1, DROP INDEX IF EXISTS index_2 SETTINGS mutations_sync=0, alter_sync=0;"
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

    assert local_op.format_sql() == "ALTER TABLE test_table DROP COLUMN IF EXISTS test_column;"

    dist_op = DropColumn(
        StorageSetKey.EVENTS,
        "test_dist_table",
        "test_column",
        target=OperationTarget.DISTRIBUTED,
    )
    assert dist_op.format_sql() == "ALTER TABLE test_dist_table DROP COLUMN IF EXISTS test_column;"


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


@mock.patch("snuba.clusters.cluster.ClickhouseCluster.get_local_nodes", return_value=[])
@pytest.mark.custom_clickhouse_db
def test_missing_nodes_for_operation(mock_get_local_nodes: Mock) -> None:
    with pytest.raises(OperationMissingNodes):
        TruncateTable(StorageSetKey.EVENTS, "blah_table", target=OperationTarget.LOCAL).get_nodes()

    cluster = get_cluster(StorageSetKey.EVENTS)
    if cluster.is_single_node():
        # in single node mode get_distributed_nodes returning [] is okay
        assert (
            TruncateTable(
                StorageSetKey.EVENTS, "blah_table", target=OperationTarget.DISTRIBUTED
            ).get_nodes()
            == []
        )
    else:
        # in multi node mode get_distributed_nodes should have nodes
        assert TruncateTable(
            StorageSetKey.EVENTS, "blah_table", target=OperationTarget.DISTRIBUTED
        ).get_nodes()


def _make_mock_cluster(
    single_node: bool = False,
    cluster_name: str = "test_cluster",
    distributed_cluster_name: str = "test_cluster",
) -> Mock:
    """Create a mock cluster with the specified configuration."""
    mock_cluster = Mock(spec=ClickhouseCluster)
    mock_cluster.is_single_node.return_value = single_node
    mock_cluster.get_clickhouse_cluster_name.return_value = cluster_name
    mock_cluster.get_clickhouse_distributed_cluster_name.return_value = distributed_cluster_name
    return mock_cluster


class TestOnCluster:
    """Tests for ON CLUSTER DDL syntax in multi-node clusters."""

    @patch("snuba.migrations.operations.get_cluster")
    def test_create_table_on_cluster(self, mock_get_cluster: Mock) -> None:
        mock_get_cluster.return_value = _make_mock_cluster(single_node=False)
        columns: Sequence[Column[Modifiers]] = [
            Column("id", String()),
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
            ),
        )
        sql = op.format_sql()
        assert "ON CLUSTER 'test_cluster'" in sql
        assert sql.startswith("CREATE TABLE IF NOT EXISTS test_table ON CLUSTER")

    @patch("snuba.migrations.operations.get_cluster")
    def test_create_table_single_node_no_on_cluster(self, mock_get_cluster: Mock) -> None:
        mock_get_cluster.return_value = _make_mock_cluster(single_node=True)
        columns: Sequence[Column[Modifiers]] = [
            Column("id", String()),
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
            ),
        )
        sql = op.format_sql()
        assert "ON CLUSTER" not in sql

    @patch("snuba.migrations.operations.get_cluster")
    def test_create_materialized_view_on_cluster(self, mock_get_cluster: Mock) -> None:
        mock_get_cluster.return_value = _make_mock_cluster(single_node=False)
        op = CreateMaterializedView(
            StorageSetKey.EVENTS,
            "test_mv",
            "test_dest",
            [Column("id", String())],
            "SELECT id FROM test_table",
        )
        sql = op.format_sql()
        assert "ON CLUSTER 'test_cluster'" in sql
        assert "CREATE MATERIALIZED VIEW IF NOT EXISTS test_mv ON CLUSTER" in sql

    @patch("snuba.migrations.operations.get_cluster")
    def test_drop_table_on_cluster(self, mock_get_cluster: Mock) -> None:
        mock_get_cluster.return_value = _make_mock_cluster(single_node=False)
        op = DropTable(StorageSetKey.EVENTS, "test_table")
        sql = op.format_sql()
        assert "ON CLUSTER 'test_cluster'" in sql
        assert sql == "DROP TABLE IF EXISTS test_table ON CLUSTER 'test_cluster' SYNC;"

    @patch("snuba.migrations.operations.get_cluster")
    def test_truncate_table_on_cluster(self, mock_get_cluster: Mock) -> None:
        mock_get_cluster.return_value = _make_mock_cluster(single_node=False)
        op = TruncateTable(StorageSetKey.EVENTS, "test_table")
        sql = op.format_sql()
        assert "ON CLUSTER 'test_cluster'" in sql
        assert sql == "TRUNCATE TABLE IF EXISTS test_table ON CLUSTER 'test_cluster';"

    @patch("snuba.migrations.operations.get_cluster")
    def test_rename_table_on_cluster(self, mock_get_cluster: Mock) -> None:
        mock_get_cluster.return_value = _make_mock_cluster(single_node=False)
        op = RenameTable(StorageSetKey.EVENTS, "old_table", "new_table")
        sql = op.format_sql()
        assert "ON CLUSTER 'test_cluster'" in sql
        assert sql == "RENAME TABLE old_table TO new_table ON CLUSTER 'test_cluster';"

    @patch("snuba.migrations.operations.get_cluster")
    def test_add_column_on_cluster(self, mock_get_cluster: Mock) -> None:
        mock_get_cluster.return_value = _make_mock_cluster(single_node=False)
        op = AddColumn(
            StorageSetKey.EVENTS,
            "test_table",
            Column("new_col", String()),
            after="id",
        )
        sql = op.format_sql()
        assert "ON CLUSTER 'test_cluster'" in sql
        assert "ALTER TABLE test_table ON CLUSTER 'test_cluster' ADD COLUMN" in sql

    @patch("snuba.migrations.operations.get_cluster")
    def test_drop_column_on_cluster(self, mock_get_cluster: Mock) -> None:
        mock_get_cluster.return_value = _make_mock_cluster(single_node=False)
        op = DropColumn(StorageSetKey.EVENTS, "test_table", "old_col")
        sql = op.format_sql()
        assert "ON CLUSTER 'test_cluster'" in sql
        assert (
            sql == "ALTER TABLE test_table ON CLUSTER 'test_cluster' DROP COLUMN IF EXISTS old_col;"
        )

    @patch("snuba.migrations.operations.get_cluster")
    def test_modify_column_on_cluster(self, mock_get_cluster: Mock) -> None:
        mock_get_cluster.return_value = _make_mock_cluster(single_node=False)
        op = ModifyColumn(StorageSetKey.EVENTS, "test_table", Column("col", String()))
        sql = op.format_sql()
        assert "ON CLUSTER 'test_cluster'" in sql
        assert sql == "ALTER TABLE test_table ON CLUSTER 'test_cluster' MODIFY COLUMN col String;"

    @patch("snuba.migrations.operations.get_cluster")
    def test_modify_ttl_on_cluster(self, mock_get_cluster: Mock) -> None:
        mock_get_cluster.return_value = _make_mock_cluster(single_node=False)
        op = ModifyTableTTL(StorageSetKey.EVENTS, "test_table", "timestamp", 90)
        sql = op.format_sql()
        assert "ON CLUSTER 'test_cluster'" in sql
        assert "ALTER TABLE test_table ON CLUSTER 'test_cluster' MODIFY TTL" in sql

    @patch("snuba.migrations.operations.get_cluster")
    def test_remove_ttl_on_cluster(self, mock_get_cluster: Mock) -> None:
        mock_get_cluster.return_value = _make_mock_cluster(single_node=False)
        op = RemoveTableTTL(StorageSetKey.EVENTS, "test_table")
        sql = op.format_sql()
        assert "ON CLUSTER 'test_cluster'" in sql
        assert sql == "ALTER TABLE test_table ON CLUSTER 'test_cluster' REMOVE TTL;"

    @patch("snuba.migrations.operations.get_cluster")
    def test_modify_settings_on_cluster(self, mock_get_cluster: Mock) -> None:
        mock_get_cluster.return_value = _make_mock_cluster(single_node=False)
        op = ModifyTableSettings(StorageSetKey.EVENTS, "test_table", {"key": "val"})
        sql = op.format_sql()
        assert "ON CLUSTER 'test_cluster'" in sql
        assert sql == "ALTER TABLE test_table ON CLUSTER 'test_cluster' MODIFY SETTING key = val;"

    @patch("snuba.migrations.operations.get_cluster")
    def test_reset_settings_on_cluster(self, mock_get_cluster: Mock) -> None:
        mock_get_cluster.return_value = _make_mock_cluster(single_node=False)
        op = ResetTableSettings(StorageSetKey.EVENTS, "test_table", ["setting_a"])
        sql = op.format_sql()
        assert "ON CLUSTER 'test_cluster'" in sql
        assert sql == "ALTER TABLE test_table ON CLUSTER 'test_cluster' RESET SETTING setting_a;"

    @patch("snuba.migrations.operations.get_cluster")
    def test_add_index_on_cluster(self, mock_get_cluster: Mock) -> None:
        mock_get_cluster.return_value = _make_mock_cluster(single_node=False)
        op = AddIndex(
            StorageSetKey.EVENTS,
            "test_table",
            "idx_1",
            "timestamp",
            "minmax",
            3,
        )
        sql = op.format_sql()
        assert "ON CLUSTER 'test_cluster'" in sql
        assert "ALTER TABLE test_table ON CLUSTER 'test_cluster' ADD INDEX" in sql

    @patch("snuba.migrations.operations.get_cluster")
    def test_add_indices_on_cluster(self, mock_get_cluster: Mock) -> None:
        mock_get_cluster.return_value = _make_mock_cluster(single_node=False)
        op = AddIndices(
            StorageSetKey.EVENTS,
            "test_table",
            [AddIndicesData("idx_1", "col1", "bloom_filter(0.1)", 4)],
        )
        sql = op.format_sql()
        assert "ON CLUSTER 'test_cluster'" in sql
        assert "ALTER TABLE test_table ON CLUSTER 'test_cluster' ADD INDEX" in sql

    @patch("snuba.migrations.operations.get_cluster")
    def test_drop_index_on_cluster(self, mock_get_cluster: Mock) -> None:
        mock_get_cluster.return_value = _make_mock_cluster(single_node=False)
        op = DropIndex(StorageSetKey.EVENTS, "test_table", "idx_1")
        sql = op.format_sql()
        assert "ON CLUSTER 'test_cluster'" in sql
        assert sql == "ALTER TABLE test_table ON CLUSTER 'test_cluster' DROP INDEX IF EXISTS idx_1;"

    @patch("snuba.migrations.operations.get_cluster")
    def test_drop_indices_on_cluster(self, mock_get_cluster: Mock) -> None:
        mock_get_cluster.return_value = _make_mock_cluster(single_node=False)
        op = DropIndices(StorageSetKey.EVENTS, "test_table", ["idx_1", "idx_2"])
        sql = op.format_sql()
        assert "ON CLUSTER 'test_cluster'" in sql
        assert "ALTER TABLE test_table ON CLUSTER 'test_cluster' DROP INDEX IF EXISTS idx_1" in sql

    @patch("snuba.migrations.operations.get_cluster")
    def test_insert_into_select_no_on_cluster(self, mock_get_cluster: Mock) -> None:
        """InsertIntoSelect is DML, not DDL, so it should NOT use ON CLUSTER."""
        mock_get_cluster.return_value = _make_mock_cluster(single_node=False)
        op = InsertIntoSelect(
            StorageSetKey.EVENTS,
            "dest_table",
            ["a", "b"],
            "src_table",
            ["a", "b"],
        )
        sql = op.format_sql()
        assert "ON CLUSTER" not in sql

    @patch("snuba.migrations.operations.get_cluster")
    def test_local_target_uses_cluster_name(self, mock_get_cluster: Mock) -> None:
        """LOCAL target should use cluster_name for ON CLUSTER."""
        mock_get_cluster.return_value = _make_mock_cluster(
            single_node=False, cluster_name="storage_cluster"
        )
        op = DropTable(StorageSetKey.EVENTS, "test_table", target=OperationTarget.LOCAL)
        sql = op.format_sql()
        assert "ON CLUSTER 'storage_cluster'" in sql

    @patch("snuba.migrations.operations.get_cluster")
    def test_distributed_target_uses_distributed_cluster_name(self, mock_get_cluster: Mock) -> None:
        """DISTRIBUTED target should use distributed_cluster_name for ON CLUSTER."""
        mock_get_cluster.return_value = _make_mock_cluster(
            single_node=False, distributed_cluster_name="query_cluster"
        )
        op = DropTable(StorageSetKey.EVENTS, "test_table", target=OperationTarget.DISTRIBUTED)
        sql = op.format_sql()
        assert "ON CLUSTER 'query_cluster'" in sql
