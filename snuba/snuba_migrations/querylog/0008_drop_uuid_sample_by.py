import logging
import math
import time
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


def update_querylog_table(clickhouse: ClickhousePool, database: str) -> None:
    ((curr_sampling_key,),) = clickhouse.execute(
        f"SELECT sampling_key FROM system.tables WHERE name = '{TABLE_NAME}' AND database = '{database}'"
    ).results

    if curr_sampling_key != ILLEGAL_SAMPLE_BY:
        return

    ((curr_create_table_statement,),) = clickhouse.execute(
        f"SHOW CREATE TABLE {database}.{TABLE_NAME}"
    ).results

    new_create_table_statement = strip_sample_by_clause(
        curr_create_table_statement.replace(TABLE_NAME, TABLE_NAME_NEW)
    )
    assert "SAMPLE BY" not in new_create_table_statement.upper()

    clickhouse.execute(new_create_table_statement)

    [(row_count,)] = clickhouse.execute(f"SELECT count() FROM {TABLE_NAME}").results
    batch_size = 100000
    batch_count = math.ceil(row_count / batch_size)

    orderby = "toStartOfDay(timestamp), request_id"

    for i in range(batch_count):
        skip = batch_size * i
        insert_op = operations.InsertIntoSelect(
            storage_set=StorageSetKey.QUERYLOG,
            dest_table_name=TABLE_NAME_NEW,
            dest_columns=["*"],
            src_table_name=TABLE_NAME,
            src_columns=["*"],
            order_by=orderby,
            limit=batch_size,
            offset=skip,
            target=operations.OperationTarget.LOCAL,
        )
        clickhouse.execute(insert_op.format_sql())

    [(new_row_count,)] = clickhouse.execute(f"SELECT count() FROM {TABLE_NAME_NEW}").results
    assert row_count == new_row_count

    clickhouse.command(f"RENAME TABLE {TABLE_NAME} TO {TABLE_NAME_OLD};")
    clickhouse.command(f"RENAME TABLE {TABLE_NAME_NEW} TO {TABLE_NAME};")
    clickhouse.command(f"DROP TABLE {TABLE_NAME_OLD};")


def forwards(logger: logging.Logger) -> None:
    """
    Recreate querylog_local without SAMPLE BY request_id on every local node.

    ClickHouse cannot ALTER SAMPLE BY, so this copies data into a new table.
    Clusters whose querylog_local has no sampling key (or a non-UUID one) are
    left unchanged.
    """
    cluster = get_cluster(StorageSetKey.QUERYLOG)

    for node in cluster.get_local_nodes():
        connection = cluster.get_node_connection(ClickhouseClientSettings.MIGRATE, node)
        database = cluster.get_database()
        update_querylog_table(connection, database)


def backwards(logger: logging.Logger) -> None:
    """
    Clean up temporary tables if forwards failed mid-way. Does not restore
    SAMPLE BY request_id — that schema is illegal on current ClickHouse.
    """
    cluster = get_cluster(StorageSetKey.QUERYLOG)

    if not cluster.is_single_node():
        return

    clickhouse = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)
    cleanup(clickhouse, logger)


def cleanup(clickhouse: ClickhousePool, logger: logging.Logger) -> None:
    def table_exists(table_name: str) -> bool:
        return clickhouse.execute(f"EXISTS TABLE {table_name};").results == [(1,)]

    if not table_exists(TABLE_NAME):
        raise Exception(f"Table {TABLE_NAME} is missing")

    if table_exists(TABLE_NAME_NEW):
        logger.info(f"Dropping table {TABLE_NAME_NEW}")
        time.sleep(1)
        clickhouse.command(f"DROP TABLE {TABLE_NAME_NEW};")

    if table_exists(TABLE_NAME_OLD):
        logger.info(f"Dropping table {TABLE_NAME_OLD}")
        time.sleep(1)
        clickhouse.command(f"DROP TABLE {TABLE_NAME_OLD};")


class Migration(migration.CodeMigration):
    """
    Drop the illegal UUID SAMPLE BY request_id from querylog_local.

    Fresh installs after 0001 no longer create this clause (#4842). Existing
    clusters that still have it cannot dump or recreate the table on
    ClickHouse >= 21.9 (https://github.com/getsentry/snuba/issues/7216).
    """

    blocking = True  # Recreating the table may take time if there is data to copy

    def forwards_global(self) -> Sequence[operations.RunPython]:
        return [
            operations.RunPython(
                func=forwards,
                description="Drop UUID SAMPLE BY request_id from querylog_local",
            ),
        ]

    def backwards_global(self) -> Sequence[operations.RunPython]:
        return [operations.RunPython(func=backwards)]
