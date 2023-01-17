import logging
import math
import time
from typing import Sequence

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations

TABLE_NAME = "querylog_local"
TABLE_NAME_NEW = "querylog_local_new"
TABLE_NAME_OLD = "querylog_local_old"


def forwards(logger: logging.Logger) -> None:
    """
    The sample by clause for the transactions table was added in April 2020. Partition
    by has been changed twice. If the user has a table without the sample by clause,
    or the old partition by we need to recreate the table correctly and copy the
    data over before deleting the old table.
    """
    cluster = get_cluster(StorageSetKey.TRANSACTIONS)

    if not cluster.is_single_node():
        return

    clickhouse = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)
    database = cluster.get_database()

    ((curr_create_table_statement,),) = clickhouse.execute(
        f"SHOW CREATE TABLE {database}.{TABLE_NAME}"
    ).results

    new_sorting_key = "dataset, referrer, toStartOfDay(timestamp), request_id"
    new_sampling_key = "request_id TTL timestamp + toIntervalDay(30)"

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

    # Switch the sample by
    if curr_sampling_key != new_sampling_key:
        assert new_create_table_statement.count("SAMPLE BY " + curr_sampling_key) == 1
        new_create_table_statement = new_create_table_statement.replace(
            "SAMPLE BY " + curr_sampling_key, "SAMPLE BY " + new_sampling_key
        )

    # Create the new table
    print("Creating new table", new_create_table_statement)
    clickhouse.execute(new_create_table_statement)

    # Copy the data over
    [(row_count,)] = clickhouse.execute(f"SELECT count() FROM {TABLE_NAME}").results
    batch_size = 100000
    batch_count = math.ceil(row_count / batch_size)

    orderby = "toStartOfDay(finish_ts), project_id, event_id"

    for i in range(batch_count):
        skip = batch_size * i
        clickhouse.execute(
            f"""
            INSERT INTO {TABLE_NAME_NEW}
            SELECT * FROM {TABLE_NAME}
            ORDER BY {orderby}
            LIMIT {batch_size}
            OFFSET {skip};
            """
        )

    clickhouse.execute(f"RENAME TABLE {TABLE_NAME} TO {TABLE_NAME_OLD};")

    clickhouse.execute(f"RENAME TABLE {TABLE_NAME_NEW} TO {TABLE_NAME};")

    # Ensure each table has the same number of rows before deleting the old one
    assert (
        clickhouse.execute(f"SELECT COUNT() FROM {TABLE_NAME};").results
        == clickhouse.execute(f"SELECT COUNT() FROM {TABLE_NAME_OLD};").results
    )

    clickhouse.execute(f"DROP TABLE {TABLE_NAME_OLD};")


def backwards(logger: logging.Logger) -> None:
    """
    This method cleans up the temporary tables used by the forwards methods and
    returns us to the original state if the forwards method has failed somewhere
    in the middle. Otherwise it's a no-op.
    """
    cluster = get_cluster(StorageSetKey.TRANSACTIONS)

    if not cluster.is_single_node():
        return

    clickhouse = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)

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
                description="Sync sample and sorting key",
            ),
        ]

    def backwards_global(self) -> Sequence[operations.RunPython]:
        return [operations.RunPython(func=backwards)]
