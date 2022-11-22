from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigrationLegacy):
    """
    Adds TTLs to all the aggergate tables based on retention_days columns
    """

    blocking = False
    table_names = [
        "metrics_distributions_local",
        "metrics_sets_local",
        "metrics_counters_local",
    ]

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.RunSql(
                storage_set=StorageSetKey.METRICS,
                statement=(
                    f"ALTER TABLE {table_name} MODIFY TTL "
                    "timestamp + toIntervalDay(retention_days)"
                ),
            )
            for table_name in self.table_names
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return []

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
