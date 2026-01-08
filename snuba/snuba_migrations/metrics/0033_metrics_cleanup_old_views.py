from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigrationLegacy):
    """
    Drop unused materialized views
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(storage_set=StorageSetKey.METRICS, table_name=table_name)
            for table_name in [
                "metrics_counters_consolidated_mv_local",
                "metrics_counters_mv_10s_local",
                "metrics_counters_mv_3600s_local",
                "metrics_counters_mv_86400s_local",
                "metrics_counters_mv_local",
                "metrics_counters_polymorphic_mv_local",
                "metrics_counters_polymorphic_mv_v2_local",
                "metrics_counters_polymorphic_mv_v3_local",
                "metrics_distributions_consolidated_mv_local",
                "metrics_distributions_mv_10s_local",
                "metrics_distributions_mv_3600s_local",
                "metrics_distributions_mv_86400s_local",
                "metrics_distributions_mv_local",
                "metrics_distributions_polymorphic_mv_local",
                "metrics_distributions_polymorphic_mv_v2_local",
                "metrics_distributions_polymorphic_mv_v3_local",
                "metrics_sets_consolidated_mv_local",
                "metrics_sets_mv_10s_local",
                "metrics_sets_mv_3600s_local",
                "metrics_sets_mv_86400s_local",
                "metrics_sets_mv_local",
                "metrics_sets_polymorphic_mv_local",
                "metrics_sets_polymorphic_mv_v2_local",
                "metrics_sets_polymorphic_mv_v3_local",
            ]
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return []

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
