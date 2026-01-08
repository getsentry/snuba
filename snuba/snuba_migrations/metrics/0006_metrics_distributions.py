from typing import Sequence

from snuba.migrations import migration, operations
from snuba.snuba_migrations.metrics.templates import (
    COL_SCHEMA_DISTRIBUTIONS,
    get_forward_migrations_dist,
    get_forward_migrations_local,
    get_migration_args_for_distributions,
    get_reverse_table_migration,
)


class Migration(migration.ClickhouseNodeMigrationLegacy):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return get_forward_migrations_local(**get_migration_args_for_distributions(granularity=60))

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            *get_reverse_table_migration("metrics_distributions_mv_local"),
            *get_reverse_table_migration("metrics_distributions_local"),
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return get_forward_migrations_dist(
            dist_table_name="metrics_distributions_dist",
            local_table_name="metrics_distributions_local",
            aggregation_col_schema=COL_SCHEMA_DISTRIBUTIONS,
        )

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return get_reverse_table_migration("metrics_distributions_dist")
