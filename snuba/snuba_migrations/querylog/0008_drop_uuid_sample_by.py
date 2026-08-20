from collections.abc import Sequence

from snuba.clickhouse.pool import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.migration_utilities import strip_sample_by_clause

TABLE_NAME = "querylog_local"
TABLE_NAME_NEW = "querylog_local_new"
TABLE_NAME_OLD = "querylog_local_old"

# Pre-ClickHouse 21.9 allowed SAMPLE BY on UUID columns. querylog_local used
# SAMPLE BY request_id (a UUID). ClickHouse now rejects that type, so SHOW CREATE
# / cluster expansion fails with ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER.
# See https://github.com/getsentry/snuba/issues/7216
ILLEGAL_SAMPLE_BY = "request_id"


def _local_connection() -> ClickhousePool | None:
    cluster = get_cluster(StorageSetKey.QUERYLOG)
    nodes = cluster.get_local_nodes()
    if not nodes:
        return None
    return cluster.get_node_connection(ClickhouseClientSettings.MIGRATE, nodes[0])


def _table_exists(clickhouse: ClickhousePool, table_name: str) -> bool:
    return clickhouse.execute(f"EXISTS TABLE {table_name};").results == [(1,)]


def _drop_local(table_name: str) -> operations.DropTable:
    return operations.DropTable(
        StorageSetKey.QUERYLOG,
        table_name,
        target=operations.OperationTarget.LOCAL,
    )


def rebuild_ops() -> Sequence[operations.SqlOperation]:
    """Inspect querylog_local and return rebuild ops, or [] if SAMPLE BY is already valid."""
    clickhouse = _local_connection()
    if clickhouse is None:
        return []

    database = get_cluster(StorageSetKey.QUERYLOG).get_database()
    ((sampling_key,),) = clickhouse.execute(
        f"SELECT sampling_key FROM system.tables WHERE name = '{TABLE_NAME}' AND database = '{database}'"
    ).results
    if sampling_key != ILLEGAL_SAMPLE_BY:
        return []

    ((create_table_statement,),) = clickhouse.execute(
        f"SHOW CREATE TABLE {database}.{TABLE_NAME}"
    ).results
    new_create_table_statement = strip_sample_by_clause(
        create_table_statement.replace(TABLE_NAME, TABLE_NAME_NEW)
    )
    assert "SAMPLE BY" not in new_create_table_statement.upper()

    return [
        operations.RunSql(
            StorageSetKey.QUERYLOG,
            new_create_table_statement,
            target=operations.OperationTarget.LOCAL,
        ),
        operations.InsertIntoSelect(
            storage_set=StorageSetKey.QUERYLOG,
            dest_table_name=TABLE_NAME_NEW,
            dest_columns=["*"],
            src_table_name=TABLE_NAME,
            src_columns=["*"],
            target=operations.OperationTarget.LOCAL,
        ),
        operations.RenameTable(
            StorageSetKey.QUERYLOG,
            TABLE_NAME,
            TABLE_NAME_OLD,
            target=operations.OperationTarget.LOCAL,
        ),
        operations.RenameTable(
            StorageSetKey.QUERYLOG,
            TABLE_NAME_NEW,
            TABLE_NAME,
            target=operations.OperationTarget.LOCAL,
        ),
        operations.DropTable(
            StorageSetKey.QUERYLOG,
            TABLE_NAME_OLD,
            target=operations.OperationTarget.LOCAL,
        ),
    ]


def recover_ops() -> Sequence[operations.SqlOperation]:
    """Restore querylog_local if the swap was interrupted, then drop leftover temps."""
    clickhouse = _local_connection()
    if clickhouse is None:
        return []

    has_local = _table_exists(clickhouse, TABLE_NAME)
    has_new = _table_exists(clickhouse, TABLE_NAME_NEW)
    has_old = _table_exists(clickhouse, TABLE_NAME_OLD)
    ops: list[operations.SqlOperation] = []

    if not has_local:
        if has_old:
            ops.append(
                operations.RenameTable(
                    StorageSetKey.QUERYLOG,
                    TABLE_NAME_OLD,
                    TABLE_NAME,
                    target=operations.OperationTarget.LOCAL,
                )
            )
            has_old = False
        elif has_new:
            ops.append(
                operations.RenameTable(
                    StorageSetKey.QUERYLOG,
                    TABLE_NAME_NEW,
                    TABLE_NAME,
                    target=operations.OperationTarget.LOCAL,
                )
            )
            has_new = False
        else:
            raise Exception(f"Table {TABLE_NAME} is missing")

    if has_new:
        ops.append(_drop_local(TABLE_NAME_NEW))
    if has_old:
        ops.append(_drop_local(TABLE_NAME_OLD))
    return ops


class _DeferredLocalOp(operations.SqlOperation):
    """Inspects ClickHouse at execute time, then runs the returned SqlOperations."""

    def __init__(self, description: str) -> None:
        super().__init__(StorageSetKey.QUERYLOG, target=operations.OperationTarget.LOCAL)
        self.__description = description

    def format_sql(self) -> str:
        return self.__description

    def execute(self) -> None:
        for op in self._ops():
            op.execute()

    def _ops(self) -> Sequence[operations.SqlOperation]:
        raise NotImplementedError


class DropUuidSampleBy(_DeferredLocalOp):
    def __init__(self) -> None:
        super().__init__("Rebuild querylog_local without SAMPLE BY request_id if present")

    def _ops(self) -> Sequence[operations.SqlOperation]:
        return rebuild_ops()


class RecoverQuerylogLocal(_DeferredLocalOp):
    def __init__(self) -> None:
        super().__init__("Restore querylog_local from temp tables after a failed rebuild")

    def _ops(self) -> Sequence[operations.SqlOperation]:
        return recover_ops()


class Migration(migration.ClickhouseNodeMigration):
    """
    Drop the illegal UUID SAMPLE BY request_id from querylog_local.

    Fresh installs after 0001 no longer create this clause (#4842). Existing
    clusters that still have it cannot dump or recreate the table on
    ClickHouse >= 21.9 (https://github.com/getsentry/snuba/issues/7216).
    """

    blocking = True  # Recreating the table may take time if there is data to copy

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [DropUuidSampleBy()]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [RecoverQuerylogLocal()]
