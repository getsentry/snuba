from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


TABLE_NAME = "groupedmessage_local"
TABLE_NAME_NEW = "groupedmessage_local_new"
TABLE_NAME_OLD = "groupedmessage_local_old"


def fix_order_by() -> None:
    cluster = get_cluster(StorageSetKey.EVENTS)
    clickhouse = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)
    database = cluster.get_database()

    new_primary_key = "project_id, id"
    old_primary_key = "id"

    ((curr_primary_key,),) = clickhouse.execute(
        f"SELECT primary_key FROM system.tables WHERE name = '{TABLE_NAME}' AND database = '{database}'"
    )

    if curr_primary_key != old_primary_key:
        return

    # There shouldn't be any data in the table yet
    assert (
        clickhouse.execute(f"SELECT COUNT() FROM {TABLE_NAME} FINAL;")[0][0] == 0
    ), f"{TABLE_NAME} is not empty"

    new_order_by = f"ORDER BY ({new_primary_key})"
    old_order_by = f"ORDER BY {old_primary_key}"

    ((curr_create_table_statement,),) = clickhouse.execute(
        f"SHOW CREATE TABLE {database}.{TABLE_NAME}"
    )

    new_create_table_statement = curr_create_table_statement.replace(
        TABLE_NAME, TABLE_NAME_NEW
    ).replace(old_order_by, new_order_by)

    clickhouse.execute(new_create_table_statement)

    clickhouse.execute(f"RENAME TABLE {TABLE_NAME} TO {TABLE_NAME_OLD};")

    clickhouse.execute(f"RENAME TABLE {TABLE_NAME_NEW} TO {TABLE_NAME};")

    clickhouse.execute(f"DROP TABLE {TABLE_NAME_OLD};")


class Migration(migration.MultiStepMigration):
    """
    An earlier version of the groupedmessage table (pre September 2019) did not
    include the project ID. This migration adds the column and rebuilds that table
    if the user has the old version with the incorrect primary key.
    """

    blocking = True

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name=TABLE_NAME,
                column=Column("project_id", UInt(64)),
                after="record_deleted",
            ),
            operations.RunPython(func=fix_order_by),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        # Drop the temporary tables used by forwards_local if they exist
        return [
            operations.DropTable(
                storage_set=StorageSetKey.EVENTS, table_name=TABLE_NAME_NEW,
            ),
            operations.DropTable(
                storage_set=StorageSetKey.EVENTS, table_name=TABLE_NAME_OLD,
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return []

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return []
