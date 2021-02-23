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

    if not cluster.is_single_node():
        return

    clickhouse = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)
    database = cluster.get_database()

    new_primary_key = "project_id, id"
    old_primary_key = "id"

    ((curr_primary_key,),) = clickhouse.execute(
        f"SELECT primary_key FROM system.tables WHERE name = '{TABLE_NAME}' AND database = '{database}'"
    )

    assert curr_primary_key in [
        new_primary_key,
        old_primary_key,
    ], "Groupmessage table has invalid primary key"

    if curr_primary_key != old_primary_key:
        return

    # Add the project_id column
    add_column_sql = operations.AddColumn(
        storage_set=StorageSetKey.EVENTS,
        table_name=TABLE_NAME,
        column=Column("project_id", UInt(64)),
        after="record_deleted",
    ).format_sql()

    clickhouse.execute(add_column_sql)

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


def ensure_drop_temporary_tables() -> None:
    cluster = get_cluster(StorageSetKey.EVENTS)

    if not cluster.is_single_node():
        return

    clickhouse = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)
    clickhouse.execute(
        operations.DropTable(
            storage_set=StorageSetKey.EVENTS, table_name=TABLE_NAME_NEW,
        ).format_sql()
    )
    clickhouse.execute(
        operations.DropTable(
            storage_set=StorageSetKey.EVENTS, table_name=TABLE_NAME_OLD,
        ).format_sql()
    )


class Migration(migration.CodeMigration):
    """
    An earlier version of the groupedmessage table (pre September 2019) did not
    include the project ID. This migration adds the column and rebuilds that table
    if the user has the old version with the incorrect primary key.
    """

    blocking = True

    def forwards_global(self) -> Sequence[operations.RunPython]:
        return [
            operations.RunPython(
                func=fix_order_by, description="Sync project ID colum for onpremise"
            ),
        ]

    def backwards_global(self) -> Sequence[operations.RunPython]:
        return [
            operations.RunPython(
                func=ensure_drop_temporary_tables,
                description="Ensure temporary tables created by the migration are dropped",
            )
        ]
