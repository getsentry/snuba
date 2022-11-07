from typing import Sequence, Union
from unittest.mock import Mock, patch

from snuba.clickhouse.columns import Column, String, UInt
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, validator
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.groups import MigrationGroup, get_group_loader
from snuba.migrations.operations import AddColumn, CreateTable, DropColumn, SqlOperation
from snuba.migrations.table_engines import Distributed, ReplacingMergeTree
from snuba.migrations.validator import (
    _get_local_table_name,
    conflicts_add_column_op,
    conflicts_create_table_op,
    conflicts_drop_column_op,
    validate_migration_order,
)


def test_validate_all_migrations() -> None:
    for group in MigrationGroup:
        group_loader = get_group_loader(group)

        for migration_id in group_loader.get_migrations():
            snuba_migration = group_loader.load_migration(migration_id)
            if isinstance(snuba_migration, migration.ClickhouseNodeMigration):
                validate_migration_order(snuba_migration)


@patch.object(validator, "_get_local_table_name")
def test_out_of_order_create(mock_get_local_table_name: Mock) -> None:
    def _dist_to_local(op: Union[CreateTable, AddColumn, DropColumn]) -> str:
        if op.table_name == "test_dist_table":
            return "test_local_table"
        if op.table_name == "test_dist_table2":
            return "test_local_table2"
        return op.table_name

    mock_get_local_table_name.side_effect = _dist_to_local
    columns: Sequence[Column[Modifiers]] = [
        Column("col1", String()),
    ]
    storage = StorageSetKey.EVENTS
    # create_local_op = Mock(CreateTable)
    create_local_op = CreateTable(
        storage,
        "test_local_table",
        columns,
        ReplacingMergeTree(
            storage_set=storage,
            order_by="version",
        ),
    )
    create_dist_op = CreateTable(
        storage,
        "test_dist_table",
        columns,
        Distributed("test_local_table", None),
    )
    # create_dist_op = Mock(CreateTable)
    # drop_local_op = Mock(DropTable)??
    # drop_dist_op = Mock(DropTable)??
    # logger = Logger("test")
    # context = Context("001", logger, lambda x: None)

    class TestMigration(migration.ClickhouseNodeMigration):
        blocking = False

        def forwards_local(self) -> Sequence[SqlOperation]:
            return [create_local_op]

        def backwards_local(self) -> Sequence[SqlOperation]:
            return []

        def forwards_dist(self) -> Sequence[SqlOperation]:
            return [create_dist_op]

        def backwards_dist(self) -> Sequence[SqlOperation]:
            return []

    validate_migration_order(TestMigration())


@patch.object(validator, "_get_local_table_name")
def test_conflicts(mock_get_local_table_name: Mock) -> None:
    """
    Test that the conlicts functions detect conflicting SQL operations that target the same table.
    """
    # database = os.environ.get("CLICKHOUSE_DATABASE", "default")
    # dist_to_local: Dict[SqlOperation, str] = {}
    storage = StorageSetKey.EVENTS

    def _dist_to_local(op: Union[CreateTable, AddColumn, DropColumn]) -> str:
        if op.table_name == "test_dist_table":
            return "test_local_table"
        if op.table_name == "test_dist_table2":
            return "test_local_table2"
        return op.table_name

    mock_get_local_table_name.side_effect = _dist_to_local

    columns: Sequence[Column[Modifiers]] = [
        Column("id", String()),
        Column("name", String(Modifiers(nullable=True))),
        Column("version", UInt(64)),
    ]

    create_local_op = CreateTable(
        storage,
        "test_local_table",
        columns,
        ReplacingMergeTree(
            storage_set=storage,
            order_by="version",
        ),
    )

    create_local_op_table2 = CreateTable(
        storage,
        "test_local_table2",
        columns,
        ReplacingMergeTree(
            storage_set=storage,
            order_by="version",
        ),
    )

    create_dist_op = CreateTable(
        storage,
        "test_dist_table",
        columns,
        Distributed("test_local_table", None),
    )

    assert conflicts_create_table_op(create_local_op, create_dist_op)
    assert not conflicts_create_table_op(create_local_op_table2, create_dist_op)

    add_col1: Column[Modifiers] = Column("col1", String())
    add_col2: Column[Modifiers] = Column("col2", String())
    add_col_local_op = AddColumn(storage, "test_local_table", add_col1, None)
    add_col_dist_op = AddColumn(storage, "test_dist_table", add_col1, None)
    add_col_dist_op_col_2 = AddColumn(storage, "test_dist_table", add_col2, None)
    add_col_local_op_col_2 = AddColumn(storage, "test_local_table", add_col2, None)
    add_col_dist_op_table_2 = AddColumn(storage, "test_dist_table2", add_col1, None)
    add_col_local_op_table_2 = AddColumn(storage, "test_local_table2", add_col1, None)

    assert conflicts_add_column_op(add_col_local_op, add_col_dist_op)
    assert conflicts_add_column_op(add_col_local_op_table_2, add_col_dist_op_table_2)
    assert not conflicts_add_column_op(add_col_local_op_col_2, add_col_dist_op)
    assert not conflicts_add_column_op(add_col_local_op, add_col_dist_op_col_2)
    assert not conflicts_add_column_op(add_col_local_op, add_col_dist_op_table_2)
    assert not conflicts_add_column_op(add_col_local_op_table_2, add_col_dist_op)
    assert not conflicts_add_column_op(add_col_local_op, add_col_dist_op_col_2)

    drop_col1, drop_col2 = "col1", "col2"
    drop_col_local_op = DropColumn(storage, "test_local_table", drop_col1)
    drop_col_dist_op = DropColumn(storage, "test_dist_table", drop_col1)
    drop_col_dist_op_col_2 = DropColumn(storage, "test_dist_table", drop_col2)
    drop_col_local_op_col_2 = DropColumn(storage, "test_local_table", drop_col2)
    drop_col_dist_op_table_2 = DropColumn(storage, "test_dist_table2", drop_col1)
    drop_col_local_op_table_2 = DropColumn(storage, "test_local_table2", drop_col1)

    assert conflicts_drop_column_op(drop_col_local_op, drop_col_dist_op)
    assert conflicts_drop_column_op(drop_col_local_op_table_2, drop_col_dist_op_table_2)
    assert not conflicts_drop_column_op(drop_col_local_op_col_2, drop_col_dist_op)
    assert not conflicts_drop_column_op(drop_col_local_op, drop_col_dist_op_col_2)
    assert not conflicts_drop_column_op(drop_col_local_op, drop_col_dist_op_table_2)
    assert not conflicts_drop_column_op(drop_col_local_op_table_2, drop_col_dist_op)
    assert not conflicts_drop_column_op(drop_col_local_op, drop_col_dist_op_col_2)


@patch.object(validator, "_get_dist_connection")
def test_parse_engine(mock_get_dist_connection: Mock) -> None:
    cluster = get_cluster(StorageSetKey.MIGRATIONS)
    connection = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)
    database = connection.database
    mock_get_dist_connection.return_value = connection

    # setup
    connection.execute(f"DROP TABLE IF EXISTS {database}.test_local_table")
    connection.execute(f"DROP TABLE IF EXISTS {database}.test_dist_table")
    connection.execute(f"DROP TABLE IF EXISTS {database}.test_sharded_dist_table")

    connection.execute(
        f"CREATE TABLE {database}.test_local_table (id String) ENGINE = Merge('{database}','test_local_table')"
    )
    connection.execute(
        f"CREATE TABLE {database}.test_dist_table (id String)"
        f"ENGINE = Distributed(test_shard_localhost, {database}, test_local_table)"
    )
    mock_sql_op = Mock(spec=SqlOperation)
    mock_dist_op = mock_sql_op()

    connection.execute(
        f"CREATE TABLE {database}.test_sharded_dist_table (id String)"
        f"ENGINE = Distributed(test_shard_localhost, {database}, test_local_table, rand())"
    )

    # test parsing the local table name from engine
    mock_dist_op.table_name = "test_dist_table"
    assert _get_local_table_name(mock_dist_op) == "test_local_table"
    mock_dist_op.table_name = "test_sharded_dist_table"
    assert _get_local_table_name(mock_dist_op) == "test_local_table"

    # cleanup
    connection.execute(f"DROP TABLE {database}.test_local_table")
    connection.execute(f"DROP TABLE {database}.test_dist_table")
    connection.execute(f"DROP TABLE {database}.test_sharded_dist_table")
