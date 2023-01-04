from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.snuba_migrations.metrics.templates import (
    get_forward_view_migration_local,
    get_migration_args_for_distributions,
    get_mv_name,
)


class Migration(migration.ClickhouseNodeMigrationLegacy):
    """
    Create a materialized view for metrics distributions with a one hour granularity.

    The backward migration does *not* delete any data from the destination tables.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return (
            get_forward_view_migration_local(
                **get_migration_args_for_distributions(granularity=60 * 60)
            ),
        )

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.METRICS,
                table_name=get_mv_name("distributions", granularity=60 * 60),
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
