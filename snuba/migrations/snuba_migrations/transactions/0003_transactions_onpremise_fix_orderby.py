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

    matching_sampling_keys = clickhouse.execute(
        f"SELECT sampling_key FROM system.tables WHERE name = '{table_name}' AND database = '{database}'"
    )
    if matching_sampling_keys == []:
        # Table does not exist yet
        return

    ((sampling_key,),) = matching_sampling_keys

    if sampling_key != "":
        # The sampling clause is already present
        return

    # Create transactions_local_new and insert data
    ((create_table_statement,),) = clickhouse.execute(
        f"SHOW CREATE TABLE {database}.{table_name}"
    )

    # Insert sample clause before TTL
    idx = create_table_statement.find("TTL")

    new_create_table_statement = (
        create_table_statement[:idx].replace(table_name, table_name_new)
        + f"SAMPLE BY cityHash64(span_id) "
        + create_table_statement[idx:]
    )

    clickhouse.execute(new_create_table_statement)

    clickhouse.execute(f"INSERT INTO {table_name_new} SELECT * FROM {table_name};")

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
