from typing import Sequence

from snuba.clickhouse.columns import (
    Column,
    DateTime,
    LowCardinality,
    Materialized,
    Nullable,
    String,
    UInt,
    WithDefault,
)
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


UNKNOWN_SPAN_STATUS = 2


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
    Syncs the transactions_local table for onpremise users migration from Snuba versions
    prior to the new migration system being introduced.

    Since the sampling key on the transactions table was only introduced later, the
    table must be recreated if the user has a version of the table without the sample
    clause set.
    """

    blocking = True  # This migration may take some time if there is data to migrate

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.RunPython(func=recreate_table),
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("duration", UInt(32)),
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("sdk_name", WithDefault(LowCardinality(String()), "''")),
                after="user_email",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column(
                    "sdk_version", WithDefault(LowCardinality(String()), "''")
                ),
                after="sdk_name",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column(
                    "transaction_status", WithDefault(UInt(8), str(UNKNOWN_SPAN_STATUS))
                ),
                after="transaction_op",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("_tags_flattened", String()),
                after="tags",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("_contexts_flattened", String()),
                after="contexts",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("user_hash", Materialized(UInt(64), "cityHash64(user)")),
                after="user",
            ),
            # The following columns were originally created as non low cardinality strings
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("transaction_name", LowCardinality(String())),
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("release", LowCardinality(Nullable(String()))),
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("dist", LowCardinality(Nullable(String()))),
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("sdk_name", WithDefault(LowCardinality(String()), "''")),
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column(
                    "sdk_version", WithDefault(LowCardinality(String()), "''")
                ),
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("environment", LowCardinality(Nullable(String()))),
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("message_timestamp", DateTime()),
                after="offset",
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return []

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return []

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return []
