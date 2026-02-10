from typing import List, Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds timestamp_ns column to eap_items_1 table and modifies the sort key
    to include it after timestamp.

    Note: This migration is non-reversible due to ClickHouse limitations.
    ClickHouse does not allow removing a column from the middle of a sorting key.
    """

    blocking = False

    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    local_table_name = "eap_items_1_local"
    dist_table_name = "eap_items_1_dist"

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops: List[operations.SqlOperation] = [
            operations.RunSql(
                storage_set=self.storage_set_key,
                statement=f"""
                    ALTER TABLE {self.local_table_name}
                    ADD COLUMN IF NOT EXISTS timestamp_ns UInt16 AFTER timestamp,
                    MODIFY ORDER BY (organization_id, project_id, item_type, timestamp, timestamp_ns, trace_id, item_id)
                """,
                target=OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                column=Column("timestamp_ns", UInt(16)),
                after="timestamp",
                target=OperationTarget.DISTRIBUTED,
            ),
        ]
        return ops

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        # Non-reversible: ClickHouse does not allow removing a column with data
        # from the sorting key because it would change the order of data on disk.
        # To reverse this migration, stop writing to timestamp_ns.
        return []
