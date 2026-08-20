from collections.abc import Sequence

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


def _forwards_ops() -> Sequence[operations.SqlOperation]:
    cluster = get_cluster(StorageSetKey.QUERYLOG)
    nodes = cluster.get_local_nodes()
    if not nodes:
        return []

    clickhouse = cluster.get_node_connection(ClickhouseClientSettings.MIGRATE, nodes[0])
    database = cluster.get_database()
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


def _backwards_ops() -> Sequence[operations.SqlOperation]:
    # Temporary tables only. SAMPLE BY request_id cannot be restored on current ClickHouse.
    return [
        operations.DropTable(
            StorageSetKey.QUERYLOG,
            TABLE_NAME_NEW,
            target=operations.OperationTarget.LOCAL,
        ),
        operations.DropTable(
            StorageSetKey.QUERYLOG,
            TABLE_NAME_OLD,
            target=operations.OperationTarget.LOCAL,
        ),
    ]


class Migration(migration.ClickhouseNodeMigration):
    """
    Drop the illegal UUID SAMPLE BY request_id from querylog_local.

    Fresh installs after 0001 no longer create this clause (#4842). Existing
    clusters that still have it cannot dump or recreate the table on
    ClickHouse >= 21.9 (https://github.com/getsentry/snuba/issues/7216).
    """

    blocking = True  # Recreating the table may take time if there is data to copy

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return _forwards_ops()

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return _backwards_ops()
