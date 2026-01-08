import re
from typing import Sequence, Union

from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.migrations.migration import (
    ClickhouseNodeMigration,
    ClickhouseNodeMigrationLegacy,
)
from snuba.migrations.operations import (
    AddColumn,
    CreateTable,
    DropColumn,
    OperationTarget,
    SqlOperation,
)

ENGINE_REGEX = r"^Distributed(\(.+\))$"


class InvalidMigrationOrderError(Exception):
    """
    Raised when a migration ops are not in the correct order.
    """

    pass


class NoDistClusterNodes(Exception):
    """
    The migration has no distributed cluster nodes, so we can't validate the order.
    """

    pass


class DistributedEngineParseError(Exception):
    """
    Can't parse the distributed engine string value
    """

    pass


def _conflicts_ops(local_op: SqlOperation, dist_op: SqlOperation) -> bool:
    if isinstance(local_op, CreateTable) and isinstance(dist_op, CreateTable):
        return conflicts_create_table_op(local_op, dist_op)
    elif isinstance(local_op, AddColumn) and isinstance(dist_op, AddColumn):
        return conflicts_add_column_op(local_op, dist_op)
    elif isinstance(local_op, DropColumn) and isinstance(dist_op, DropColumn):
        return conflicts_drop_column_op(local_op, dist_op)
    return False


def _validate_add_col_or_create_table(
    local_op: SqlOperation, dist_ops: Sequence[SqlOperation]
) -> None:
    if isinstance(local_op, (CreateTable, AddColumn)):
        if any(_conflicts_ops(local_op, dist_op) for dist_op in dist_ops):
            op_name = (
                f"{local_op.table_name}.{local_op.column.name}"
                if isinstance(local_op, AddColumn)
                else local_op.table_name
            )
            raise InvalidMigrationOrderError(
                f"{type(local_op).__name__} {op_name} operation "
                "must be applied on local table before dist"
            )


def _validate_drop_col(dist_op: SqlOperation, local_ops: Sequence[SqlOperation]) -> None:
    if isinstance(dist_op, (DropColumn)):
        if any(_conflicts_ops(local_op, dist_op) for local_op in local_ops):
            raise InvalidMigrationOrderError(
                f"{type(dist_op).__name__} {dist_op.table_name}.{dist_op.column_name} "
                "operation must be applied on dist table before local"
            )


def _validate_order_old(
    local_ops: Sequence[SqlOperation],
    dist_ops: Sequence[SqlOperation],
    local_first: bool,
) -> None:
    """
    Validates migration order for old style migrations ClickhouseNodeMigrationLegacy class.
    """
    if local_first:
        for dist_op in dist_ops:
            _validate_drop_col(dist_op, local_ops)
    else:
        for local_op in local_ops:
            _validate_add_col_or_create_table(local_op, dist_ops)


def _validate_order_new(
    forward_ops: Sequence[SqlOperation], backwards_ops: Sequence[SqlOperation]
) -> None:
    """
    Validates migration order for new style migrations ClickhouseNodeMigration class.
    """
    for ops in [forward_ops, backwards_ops]:
        for i, op in enumerate(ops):
            local_ops_before = [op for op in ops[:i] if op.target != OperationTarget.DISTRIBUTED]
            dist_ops_before = [op for op in ops[:i] if op.target != OperationTarget.LOCAL]
            if isinstance(op, (CreateTable, AddColumn)):
                if op.target == OperationTarget.LOCAL:
                    _validate_add_col_or_create_table(op, dist_ops_before)
            elif isinstance(op, DropColumn):
                if op.target == OperationTarget.DISTRIBUTED:
                    _validate_drop_col(op, local_ops_before)


def validate_migration_order(migration: ClickhouseNodeMigration) -> None:
    """
    Validates that the migration order is correct. Checks that the order of
    AddColumn, CreateTable and DropColumn operations are correct with regards to being
    applied on local and distributed tables.
    """

    if isinstance(migration, ClickhouseNodeMigrationLegacy):
        # old way of doing migrations
        _validate_order_old(
            migration.forwards_local(),
            migration.forwards_dist(),
            migration.forwards_local_first,
        )
        _validate_order_old(
            migration.backwards_local(),
            migration.backwards_dist(),
            migration.backwards_local_first,
        )
    else:
        # try new way of doing migrations
        forward_ops = migration.forwards_ops()
        backward_ops = migration.backwards_ops()
        _validate_order_new(forward_ops, backward_ops)


def conflicts_create_table_op(local_create: CreateTable, dist_create: CreateTable) -> bool:
    """
    Returns True if create table operation and local create table operation
    target same underlying local table.
    """
    if local_create._storage_set != dist_create._storage_set:
        return False
    try:
        cluster = get_cluster(dist_create._storage_set)
        if cluster.is_single_node():
            raise NoDistClusterNodes
        dist_engine_str = dist_create.engine.get_sql(cluster, dist_create.table_name)
        if local_create.table_name == _extract_local_table_name(dist_engine_str):
            return True
    except NoDistClusterNodes:
        # If there are no distributed nodes, we can't validate the order.
        pass

    return False


def conflicts_add_column_op(local_add: AddColumn, dist_add: AddColumn) -> bool:
    """
    Returns True if distributed add column operation and local add column operation
    target the same column and same underlying local table.
    """
    if local_add.column != dist_add.column or local_add._storage_set != dist_add._storage_set:
        return False
    try:
        if local_add.table_name == _get_local_table_name(dist_add):
            return True
    except NoDistClusterNodes:
        # If there are no distributed nodes, we can't validate the order.
        pass

    return False


def conflicts_drop_column_op(local_drop: DropColumn, dist_drop: DropColumn) -> bool:
    """
    Returns True if distributed drop column operation and local drop column operation
    target the same column and same underlying local table.
    """

    if (
        local_drop.column_name != dist_drop.column_name
        or local_drop._storage_set != dist_drop._storage_set
    ):
        return False
    try:
        if local_drop.table_name == _get_local_table_name(dist_drop):
            return True
    except NoDistClusterNodes:
        # If there are no distributed nodes, we can't validate the order.
        pass

    return False


def _get_dist_connection(dist_op: SqlOperation) -> ClickhousePool:
    cluster = get_cluster(dist_op._storage_set)
    nodes = cluster.get_distributed_nodes()
    if not nodes:
        raise NoDistClusterNodes(f"No distributed nodes for {dist_op._storage_set}")
    node = nodes[0]
    connection = cluster.get_node_connection(ClickhouseClientSettings.MIGRATE, node)
    return connection


def _get_local_table_name(dist_op: Union[CreateTable, AddColumn, DropColumn]) -> str:
    """
    Returns the local table name for a distributed table.
    """
    clickhouse = _get_dist_connection(dist_op)
    dist_table_name = dist_op.table_name
    engine = clickhouse.execute(
        f"SELECT engine_full FROM system.tables WHERE name = '{dist_table_name}' AND database = '{clickhouse.database}'"
    ).results
    if not engine:
        raise DistributedEngineParseError(f"No engine found for table {dist_table_name}")
    engine_str = engine[0][0]
    return _extract_local_table_name(engine_str)


def _extract_local_table_name(engine_str: str) -> str:
    params = re.match(ENGINE_REGEX, engine_str)
    if not params:
        raise DistributedEngineParseError(
            f"Cannot match engine string {engine_str} for distributed table"
        )

    dist_engine_args = params.group(1)[1:-1].split(",")
    local_table_name = dist_engine_args[2].strip()
    return local_table_name.strip("'")
