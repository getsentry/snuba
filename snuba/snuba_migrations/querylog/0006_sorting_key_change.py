import logging
import math
import time
from typing import Sequence

from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations

TABLE_NAME = "querylog_local"
TABLE_NAME_NEW = "querylog_local_new"
TABLE_NAME_OLD = "querylog_local_old"


def update_querylog_table(clickhouse: ClickhousePool, database: str) -> None:
    ((curr_create_table_statement,),) = clickhouse.execute(
        f"SHOW CREATE TABLE {database}.{TABLE_NAME}"
    ).results

    new_sorting_key = "dataset, referrer, toStartOfDay(timestamp), request_id"

    ((curr_sampling_key, curr_sorting_key),) = clickhouse.execute(
        f"SELECT sampling_key, sorting_key FROM system.tables WHERE name = '{TABLE_NAME}' AND database = '{database}'"
    ).results

    new_create_table_statement = curr_create_table_statement.replace(
        TABLE_NAME, TABLE_NAME_NEW
    )

    # Switch the sorting key
    if curr_sorting_key != new_sorting_key:
        assert new_create_table_statement.count(curr_sorting_key) == 1
        new_create_table_statement = new_create_table_statement.replace(
            curr_sorting_key, new_sorting_key
        )

    # Update the timestamp column
    # Clickhouse 20 does not support altering a column in the primary key so we need to do it here
    new_timestamp_type = "`timestamp` DateTime CODEC(T64, ZSTD(1))"
    if new_create_table_statement.count(new_timestamp_type) == 0:
        assert new_create_table_statement.count("`timestamp` DateTime") == 1
        new_create_table_statement = new_create_table_statement.replace(
            "`timestamp` DateTime", new_timestamp_type
        )
    assert new_timestamp_type in new_create_table_statement

    # Create the new table
    clickhouse.execute(new_create_table_statement)

    # Copy the data over
    [(row_count,)] = clickhouse.execute(f"SELECT count() FROM {TABLE_NAME}").results
    batch_size = 100000
    batch_count = math.ceil(row_count / batch_size)

    orderby = "toStartOfDay(timestamp), request_id"

    for i in range(batch_count):
        skip = batch_size * i
        insert_op = operations.InsertIntoSelect(
            storage_set=StorageSetKey.QUERYLOG,
            dest_table_name=TABLE_NAME_NEW,
            dest_columns=["*"],  # All columns
            src_table_name=TABLE_NAME,
            src_columns=["*"],  # All columns
            order_by=orderby,
            limit=batch_size,
            offset=skip,
            target=operations.OperationTarget.LOCAL,
        )
        clickhouse.execute(insert_op.format_sql())

    # Ensure each table has the same number of rows before deleting the old one
    [(new_row_count,)] = clickhouse.execute(
        f"SELECT count() FROM {TABLE_NAME_NEW}"
    ).results
    assert row_count == new_row_count

    clickhouse.execute(f"RENAME TABLE {TABLE_NAME} TO {TABLE_NAME_OLD};")
    clickhouse.execute(f"RENAME TABLE {TABLE_NAME_NEW} TO {TABLE_NAME};")
    clickhouse.execute(f"DROP TABLE {TABLE_NAME_OLD};")
    return


def forwards(logger: logging.Logger) -> None:
    """
    This migration is a bit complicated because we need to change the sorting key.
    We can't do it inplace so we need to create a new table with the modified keys,
    copy the data over, then drop the old table.
    """
    cluster = get_cluster(StorageSetKey.QUERYLOG)

    for node in cluster.get_local_nodes():
        connection = cluster.get_node_connection(ClickhouseClientSettings.MIGRATE, node)
        database = cluster.get_database()
        update_querylog_table(connection, database)


def backwards(logger: logging.Logger) -> None:
    """
    This method cleans up the temporary tables used by the forwards methods and
    returns us to the original state if the forwards method has failed somewhere
    in the middle. Otherwise it's a no-op.
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
        clickhouse.execute(f"DROP TABLE {TABLE_NAME_NEW};")

    if table_exists(TABLE_NAME_OLD):
        logger.info(f"Dropping table {TABLE_NAME_OLD}")
        time.sleep(1)
        clickhouse.execute(f"DROP TABLE {TABLE_NAME_OLD};")


class Migration(migration.CodeMigration):
    """
    Change the sorting key to dataset, referrer, timestamp, request_id, to match SaaS.
    This is a code migration because we can't change the sorting key in a single step.

    """

    blocking = True  # This migration may take some time if there is data to migrate

    def forwards_global(self) -> Sequence[operations.RunPython]:
        return [
            operations.RunPython(
                func=forwards,
                description="Sync sample by and sorting key",
            ),
        ]

    def backwards_global(self) -> Sequence[operations.RunPython]:
        return [operations.RunPython(func=backwards)]
