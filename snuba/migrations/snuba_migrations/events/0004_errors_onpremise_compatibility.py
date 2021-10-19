from typing import Sequence

from snuba.clickhouse.columns import Column, DateTime
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigration):
    """
    Syncs the errors_local table for onpremise users migration from Snuba versions
    prior to the new migration system being introduced.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                column=Column("message_timestamp", DateTime()),
                after="offset",
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return []

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
