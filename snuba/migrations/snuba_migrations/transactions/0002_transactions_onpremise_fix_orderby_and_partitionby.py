import math
import time
from typing import Sequence

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations

TABLE_NAME = "transactions_local"
TABLE_NAME_NEW = "transactions_local_new"
TABLE_NAME_OLD = "transactions_local_old"


def forwards() -> None:
    """
    The sample by clause for the transactions table was added in April 2020. Partition
    by has been changed twice. If the user has a table without the sample by clause,
    or the old partition by we need to recreate the table correctly and copy the
    data over before deleting the old table.
    """
    cluster = get_cluster(StorageSetKey.TRANSACTIONS)

    clickhouse = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)
    database = cluster.get_database()

    new_sampling_key = "cityHash64(span_id)"
    new_partition_key = "(retention_days, toMonday(finish_ts))"
    new_primary_key = (
        "project_id, toStartOfDay(finish_ts), transaction_name, cityHash64(span_id)"
    )

    ((curr_sampling_key, curr_partition_key, curr_primary_key),) = clickhouse.execute(
        f"SELECT sampling_key, partition_key, primary_key FROM system.tables WHERE name = '{TABLE_NAME}' AND database = '{database}'"
    )

    sampling_key_needs_update = curr_sampling_key != new_sampling_key
    partition_key_needs_update = curr_partition_key != new_partition_key
    primary_key_needs_update = curr_primary_key != new_primary_key

    if not any(
        [
            sampling_key_needs_update,
            partition_key_needs_update,
            primary_key_needs_update,
        ]
    ):
        # Already up to date
        return

    # Create transactions_local_new and insert data
    ((curr_create_table_statement,),) = clickhouse.execute(
        f"SHOW CREATE TABLE {database}.{TABLE_NAME}"
    )

    new_create_table_statement = curr_create_table_statement.replace(
        TABLE_NAME, TABLE_NAME_NEW
    )

    # Insert sample clause before TTL
    if sampling_key_needs_update:
        assert "SAMPLE BY" not in new_create_table_statement
        pos = new_create_table_statement.find("TTL")
        idx = pos if pos >= 0 else new_create_table_statement.find("SETTINGS")

        if idx >= 0:
            new_create_table_statement = (
                new_create_table_statement[:idx]
                + f" SAMPLE BY {new_sampling_key} "
                + new_create_table_statement[idx:]
            )
        else:
            new_create_table_statement = (
                f"{new_create_table_statement} SAMPLE BY {new_sampling_key}"
            )
    # Switch the partition key
    if partition_key_needs_update:
        assert new_create_table_statement.count(curr_partition_key) == 1
        new_create_table_statement = new_create_table_statement.replace(
            curr_partition_key, new_partition_key
        )

    # Switch the primary key
    if primary_key_needs_update:
        assert new_create_table_statement.count(curr_primary_key) == 1
        new_create_table_statement = new_create_table_statement.replace(
            curr_primary_key, new_primary_key
        )

    clickhouse.execute(new_create_table_statement)

    # Copy over data in batches of 100,000
    [(row_count,)] = clickhouse.execute(f"SELECT count() FROM {TABLE_NAME}")
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
    assert clickhouse.execute(
        f"SELECT COUNT() FROM {TABLE_NAME} FINAL;"
    ) == clickhouse.execute(f"SELECT COUNT() FROM {TABLE_NAME_OLD} FINAL;")

    clickhouse.execute(f"DROP TABLE {TABLE_NAME_OLD};")


def backwards() -> None:
    """
    This method cleans up the temporary tables used by the forwards methodsa and
    returns us to the original state if the forwards method has failed somewhere
    in the middle. Otherwise it's a no-op.
    """
    cluster = get_cluster(StorageSetKey.TRANSACTIONS)

    clickhouse = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)

    def table_exists(table_name: str):
        return clickhouse.execute(f"EXISTS TABLE {table_name};") == [(1,)]

    if not table_exists(TABLE_NAME):
        raise Exception(f"Table {TABLE_NAME} is missing")

    if table_exists(TABLE_NAME_NEW):
        print(f"Dropping table {TABLE_NAME_NEW}")
        time.sleep(1)
        clickhouse.execute(f"DROP TABLE {TABLE_NAME_NEW};")

    if table_exists(TABLE_NAME_OLD):
        print(f"Dropping table {TABLE_NAME_OLD}")
        time.sleep(1)
        clickhouse.execute(f"DROP TABLE {TABLE_NAME_OLD};")


class Migration(migration.MultiStepMigration):
    """
    The first of two migrations that syncs the transactions_local table for onpremise
    users migrating from versions of Snuba prior to the migration system.

    This migration recreates the table if the sampling key is not present or the
    partitioning key is not up to date. Since the sampling key was introduced later and
    the partition key was changed, it's possible the user has a version of the table
    where one or both of these is out of date. If this is the case we must recreate
    the table and ensure data is copied over.
    """

    blocking = True  # This migration may take some time if there is data to migrate

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.RunPython(func=forwards),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [operations.RunPython(func=backwards)]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return []

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return []
