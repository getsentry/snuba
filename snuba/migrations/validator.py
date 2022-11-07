import re
from typing import Union

from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.migrations.migration import ClickhouseNodeMigration
from snuba.migrations.operations import AddColumn, CreateTable, DropColumn, SqlOperation

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


def validate_migration_order(migration: ClickhouseNodeMigration) -> None:
    """
    Validates that the migration order is correct. Chesk that the order of
    AddColumn, CreateTable and DropColumn operations are correct with reagards to being
    applied on local and distributed tables.
    """
    local_ops = migration.forwards_local()
    dist_ops = migration.forwards_dist()

    if migration.forwards_local_first:
        for dist_op in dist_ops:
            if isinstance(dist_op, DropColumn):
                if any(
                    conflicts_drop_column_op(local_op, dist_op)
                    for local_op in local_ops
                    if isinstance(local_op, DropColumn)
                ):
                    raise InvalidMigrationOrderError(
                        f"DropColumn {dist_op.table_name}.{dist_op.column_name} operation must applied on dist table before local"
                    )

    else:
        for local_op in local_ops:
            if isinstance(local_op, AddColumn):
                if any(
                    conflicts_add_column_op(local_op, dist_op)
                    for dist_op in dist_ops
                    if isinstance(dist_op, AddColumn)
                ):
                    raise InvalidMigrationOrderError(
                        f"AddColumn {local_op.table_name}.{local_op.column.name} operation must applied on local table before dist"
                    )

            if isinstance(local_op, CreateTable):
                if any(
                    conflicts_create_table_op(local_op, dist_op)
                    for dist_op in dist_ops
                    if isinstance(dist_op, CreateTable)
                ):
                    raise InvalidMigrationOrderError(
                        f"CreateTable {local_op.table_name} operation must applied on local table before dist"
                    )


def conflicts_create_table_op(
    local_create: CreateTable, dist_create: CreateTable
) -> bool:
    """
    Returns True if create table operation and local create table operation
    target same underlying local table.
    """
    if local_create._storage_set != dist_create._storage_set:
        return False
    try:
        if local_create.table_name == _get_local_table_name(dist_create):
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
    if (
        local_add.column != dist_add.column
        or local_add._storage_set != dist_add._storage_set
    ):
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
    engine: str = clickhouse.execute(
        f"SELECT engine_full FROM system.tables WHERE name = '{dist_table_name}' AND database = '{clickhouse.database}'"
    ).results[0][0]

    params = re.match(ENGINE_REGEX, engine)
    assert (
        params
    ), f"Cannot match engine {engine} for distributed table {dist_table_name}"

    dist_engine_args = params.group(1)[1:-1].split(",")
    local_table_name = dist_engine_args[2].strip()
    return local_table_name.strip("'")
