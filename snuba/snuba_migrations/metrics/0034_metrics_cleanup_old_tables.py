from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigrationLegacy):
    """
    Drop unused tables
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(storage_set=StorageSetKey.METRICS, table_name=table_name)
            for table_name in [
                "metrics_buckets_local",
                "metrics_counters_buckets_local",
                "metrics_counters_local",
                "metrics_distributions_buckets_local",
                "metrics_distributions_local",
                "metrics_raw_local",
                "metrics_sets_local",
            ]
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return []

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(storage_set=StorageSetKey.METRICS, table_name=table_name)
            for table_name in [
                "metrics_buckets_dist",
                "metrics_counters_buckets_dist",
                "metrics_counters_dist",
                "metrics_distributions_buckets_dist",
                "metrics_distributions_dist",
                "metrics_raw_dist",
                "metrics_sets_dist",
            ]
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
