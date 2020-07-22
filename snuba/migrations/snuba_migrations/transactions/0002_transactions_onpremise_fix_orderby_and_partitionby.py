import math
from typing import Sequence

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


def recreate_table() -> None:
    """
    The sample by clause for the transactions table was added in April 2020.
    If the user has a table without the sample by clause, we need to add it by
    recreating the table and copying the data over then deleting the old table.
    """
    cluster = get_cluster(StorageSetKey.TRANSACTIONS)

    clickhouse = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)
    database = cluster.get_database()

    table_name = "transactions_local"
    table_name_new = "transactions_local_new"

    new_sampling_key = "cityHash64(span_id)"
    new_partition_key = "(retention_days, toMonday(finish_ts))"

    ((curr_sampling_key, curr_partition_key),) = clickhouse.execute(
        f"SELECT sampling_key, partition_key FROM system.tables WHERE name = '{table_name}' AND database = '{database}'"
    )

    sampling_key_needs_update = curr_sampling_key != new_sampling_key
    partition_key_needs_update = curr_partition_key != new_partition_key

    if not sampling_key_needs_update and not partition_key_needs_update:
        # Already up to date
        return

    # Create transactions_local_new and insert data
    ((curr_create_table_statement,),) = clickhouse.execute(
        f"SHOW CREATE TABLE {database}.{table_name}"
    )

    new_create_table_statement = curr_create_table_statement.replace(
        table_name, table_name_new
    )

    # Insert sample clause before TTL
    if sampling_key_needs_update:
        assert "SAMPLE BY" not in new_create_table_statement
        idx = new_create_table_statement.find("TTL")

        new_create_table_statement = (
            new_create_table_statement[:idx]
            + f"SAMPLE BY {new_sampling_key} "
            + new_create_table_statement[idx:]
        )

    # Switch the partition key
    if partition_key_needs_update:
        assert new_create_table_statement.count(curr_partition_key) == 1
        new_create_table_statement = new_create_table_statement.replace(
            curr_partition_key, new_partition_key
        )

    clickhouse.execute(new_create_table_statement)

    # Copy over data in batches of 100,000
    [(row_count,)] = clickhouse.execute(f"SELECT count(*) FROM {table_name}")
    batch_size = 100000
    batch_count = math.ceil(row_count / batch_size)

    orderby = "toStartOfDay(finish_ts), project_id, event_id"

    for i in range(batch_count):
        skip = batch_size * i
        clickhouse.execute(
            f"""
            INSERT INTO {table_name_new}
            SELECT * FROM {table_name}
            ORDER BY {orderby}
            LIMIT {batch_size}
            OFFSET {skip};
            """
        )

    clickhouse.execute(f"DROP TABLE {table_name};")

    clickhouse.execute(f"RENAME TABLE {table_name_new} to {table_name};")


class Migration(migration.MultiStepMigration):
    """
    The second of two migrations that syncs the transactions_local table for onpremise
    users migrating from versions of Snuba prior to the migration system.

    This migration recreates the table if the sampling key is not present. Since the
    sampling key on the transactions table was introduced later, it's possible the user
    has a version without the sample clause set. If this is the case we must recreate
    the table and ensure data is copied over.
    """

    blocking = True  # This migration may take some time if there is data to migrate

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.RunPython(func=recreate_table),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return []

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return []

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return []
