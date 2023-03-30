import copy
from contextlib import _GeneratorContextManager, contextmanager
from typing import Any, Iterator, Sequence, Union
from unittest.mock import Mock, patch

import pytest

from snuba.clickhouse.columns import Column, String, UInt
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, validator
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.groups import MigrationGroup, get_group_loader
from snuba.migrations.operations import (
    AddColumn,
    CreateTable,
    DropColumn,
    OperationTarget,
    SqlOperation,
)
from snuba.migrations.table_engines import Distributed, ReplacingMergeTree
from snuba.migrations.validator import (
    DistributedEngineParseError,
    InvalidMigrationOrderError,
    _get_local_table_name,
    conflicts_add_column_op,
    conflicts_create_table_op,
    conflicts_drop_column_op,
    validate_migration_order,
)

all_migrations = []
for group in MigrationGroup:
    group_loader = get_group_loader(group)
    for migration_id in group_loader.get_migrations():
        snuba_migration = group_loader.load_migration(migration_id)
        if isinstance(snuba_migration, migration.ClickhouseNodeMigration):
            all_migrations.append((migration_id, snuba_migration))


@pytest.mark.parametrize(
    "snuba_migration",
    [
        pytest.param(snuba_migration, id=migration_id)
        for migration_id, snuba_migration in all_migrations
    ],
)
@pytest.mark.clickhouse_db
def test_validate_all_migrations(
    snuba_migration: migration.ClickhouseNodeMigration,
) -> None:
    """
    Runs the migration validator on all existing migrations.
    """
    validate_migration_order(snuba_migration)


@contextmanager
def does_not_raise() -> Iterator[None]:
    yield


class TestValidateMigrations:
    columns: Sequence[Column[Modifiers]] = [
        Column("col1", String()),
    ]
    storage = StorageSetKey.EVENTS
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
    col: Column[Modifiers] = Column("col", String())
    add_col_local_op = AddColumn(storage, "test_local_table", col, None)
    add_col_dist_op = AddColumn(storage, "test_dist_table", col, None)
    drop_col_local_op = DropColumn(storage, "test_local_table", "col")
    drop_col_dist_op = DropColumn(storage, "test_dist_table", "col")

    def _dist_to_local(self, op: Union[CreateTable, AddColumn, DropColumn]) -> str:
        if op.table_name == "test_dist_table":
            return "test_local_table"
        if op.table_name == "test_dist_table2":
            return "test_local_table2"
        return op.table_name

    test_data = [
        (True, False, [], [], [], [], does_not_raise(), ""),
        (
            True,
            False,
            [create_local_op],
            [],
            [create_dist_op],
            [],
            does_not_raise(),
            "",
        ),
        (
            False,
            False,
            [create_local_op],
            [create_dist_op],
            [],
            [],
            pytest.raises(InvalidMigrationOrderError),
            "CreateTable test_local_table operation must be applied on local table before dist",
        ),
        (
            True,
            False,
            [create_local_op, add_col_local_op],
            [create_dist_op, add_col_dist_op],
            [],
            [],
            does_not_raise(),
            "",
        ),
        (
            False,
            True,
            [add_col_local_op],
            [add_col_dist_op],
            [],
            [],
            pytest.raises(InvalidMigrationOrderError),
            "AddColumn test_local_table.col operation must be applied on local table before dist",
        ),
        (
            True,
            False,
            [create_local_op, add_col_local_op],
            [create_dist_op, add_col_dist_op],
            [drop_col_local_op],
            [drop_col_dist_op],
            does_not_raise(),
            "",
        ),
        (
            False,
            False,
            [drop_col_local_op, drop_col_local_op],
            [],
            [],
            [drop_col_dist_op, drop_col_dist_op],
            does_not_raise(),
            "",
        ),
        (
            True,
            True,
            [create_local_op, add_col_local_op],
            [create_dist_op, add_col_dist_op],
            [drop_col_local_op],
            [drop_col_dist_op],
            pytest.raises(InvalidMigrationOrderError),
            "DropColumn test_dist_table.col operation must be applied on dist table before local",
        ),
        (
            True,
            False,
            [create_local_op, drop_col_local_op],
            [create_dist_op, drop_col_dist_op],
            [add_col_local_op],
            [add_col_dist_op],
            pytest.raises(InvalidMigrationOrderError),
            "DropColumn test_dist_table.col operation must be applied on dist table before local",
        ),
        (
            False,
            False,
            [drop_col_local_op],
            [drop_col_dist_op],
            [add_col_local_op],
            [add_col_dist_op],
            pytest.raises(InvalidMigrationOrderError),
            "AddColumn test_local_table.col operation must be applied on local table before dist",
        ),
        (
            False,
            True,
            [create_local_op, drop_col_local_op],
            [create_dist_op, drop_col_dist_op],
            [add_col_local_op],
            [add_col_dist_op],
            pytest.raises(InvalidMigrationOrderError),
            "CreateTable test_local_table operation must be applied on local table before dist",
        ),
    ]

    @pytest.mark.parametrize(
        "forwards_local_first_val, backwards_local_first_val,forwards_local,forwards_dist,"
        "backwards_local, backwards_dist, expectation, err_msg",
        test_data,
    )
    @patch.object(validator, "get_cluster")
    @patch.object(validator, "_get_local_table_name")
    def test_validate_migration_order(
        self,
        mock_get_local_table_name: Mock,
        mock_get_cluster: Mock,
        forwards_local_first_val: bool,
        backwards_local_first_val: bool,
        forwards_local: Sequence[SqlOperation],
        forwards_dist: Sequence[SqlOperation],
        backwards_local: Sequence[SqlOperation],
        backwards_dist: Sequence[SqlOperation],
        expectation: Any,
        err_msg: str,
    ) -> None:

        mock_get_local_table_name.side_effect = self._dist_to_local

        storage = StorageSetKey.EVENTS
        cluster = get_cluster(storage)

        mock_cluster = Mock(spec=cluster)
        mock_cluster.is_single_node.return_value = False
        mock_cluster.get_database.return_value = cluster.get_database()
        mock_cluster.get_clickhouse_cluster_name.return_value = (
            cluster.get_clickhouse_cluster_name() or "test_cluster"
        )
        mock_get_cluster.return_value = mock_cluster

        class TestMigration(migration.ClickhouseNodeMigrationLegacy):
            blocking = False
            backwards_local_first: bool = backwards_local_first_val
            forwards_local_first: bool = forwards_local_first_val

            def forwards_local(self) -> Sequence[SqlOperation]:
                return forwards_local

            def backwards_local(self) -> Sequence[SqlOperation]:
                return backwards_local

            def forwards_dist(self) -> Sequence[SqlOperation]:
                return forwards_dist

            def backwards_dist(self) -> Sequence[SqlOperation]:
                return backwards_dist

        # reuse expected error message
        if isinstance(expectation, _GeneratorContextManager):
            expectation_new = does_not_raise()
        else:
            expectation_new = copy.deepcopy(expectation)

        with expectation as err:
            validate_migration_order(TestMigration())
        if err_msg:
            assert str(err.value) == err_msg

        # test on the new api
        for op in (*forwards_local, *backwards_local):
            op.target = OperationTarget.LOCAL
        for op in (*forwards_dist, *backwards_dist):
            op.target = OperationTarget.DISTRIBUTED

        class TestMigrationNew(migration.ClickhouseNodeMigration):
            blocking = False

            def forwards_ops(self) -> Sequence[SqlOperation]:
                if forwards_local_first_val:
                    return (*forwards_local, *forwards_dist)
                else:
                    return (*forwards_dist, *forwards_local)

            def backwards_ops(self) -> Sequence[SqlOperation]:
                if backwards_local_first_val:
                    return (*backwards_local, *backwards_dist)
                else:
                    return (*backwards_dist, *backwards_local)

        with expectation_new as err:
            validate_migration_order(TestMigrationNew())
        if err_msg:
            assert str(err.value) == err_msg


@patch.object(validator, "get_cluster")
@patch.object(validator, "_get_local_table_name")
def test_conflicts(mock_get_local_table_name: Mock, mock_get_cluster: Mock) -> None:
    """
    Test that the conlicts functions detect conflicting SQL operations that target the same table.
    """
    storage = StorageSetKey.EVENTS
    cluster = get_cluster(storage)
    mock_cluster = Mock(spec=cluster)
    mock_cluster.is_single_node.return_value = False
    mock_cluster.get_database.return_value = cluster.get_database()
    mock_cluster.get_clickhouse_cluster_name.return_value = (
        cluster.get_clickhouse_cluster_name() or "test_cluster"
    )
    mock_get_cluster.return_value = mock_cluster

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

    new_create_dist_op = CreateTable(
        storage,
        "test_dist_table",
        columns,
        Distributed("test_local_table", None),
        target=OperationTarget.DISTRIBUTED,
    )

    new_create_local_op = CreateTable(
        storage,
        "test_local_table",
        columns,
        ReplacingMergeTree(
            storage_set=storage,
            order_by="version",
        ),
        target=OperationTarget.LOCAL,
    )

    new_create_local_op2 = CreateTable(
        storage,
        "test_local_table2",
        columns,
        ReplacingMergeTree(
            storage_set=storage,
            order_by="version",
        ),
        target=OperationTarget.LOCAL,
    )
    assert conflicts_create_table_op(new_create_local_op, new_create_dist_op)
    assert not conflicts_create_table_op(new_create_local_op2, create_dist_op)


@patch.object(validator, "_get_dist_connection")
@pytest.mark.clickhouse_db
def test_parse_engine(mock_get_dist_connection: Mock) -> None:
    cluster = get_cluster(StorageSetKey.MIGRATIONS)
    connection = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)
    database = connection.database
    mock_get_dist_connection.return_value = connection

    # setup
    connection.execute(f"DROP TABLE IF EXISTS {database}.test_local_table")
    connection.execute(f"DROP TABLE IF EXISTS {database}.test_dist_table")
    connection.execute(f"DROP TABLE IF EXISTS {database}.test_sharded_dist_table")

    local_table_engine = f"Merge('{database}', 'test_local_table')"
    connection.execute(
        f"CREATE TABLE {database}.test_local_table (id String) ENGINE = {local_table_engine}"
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

    # test on not existing table
    mock_dist_op.table_name = "not_exists_table"
    with pytest.raises(DistributedEngineParseError) as parse_error:
        _get_local_table_name(mock_dist_op)
    assert str(parse_error.value) == "No engine found for table not_exists_table"

    # test on not distributed table
    mock_dist_op.table_name = "test_local_table"
    with pytest.raises(DistributedEngineParseError) as parse_error:
        _get_local_table_name(mock_dist_op)
    assert (
        str(parse_error.value)
        == f"Cannot match engine string {local_table_engine} for distributed table"
    )

    # cleanup
    connection.execute(f"DROP TABLE {database}.test_local_table")
    connection.execute(f"DROP TABLE {database}.test_dist_table")
    connection.execute(f"DROP TABLE {database}.test_sharded_dist_table")
