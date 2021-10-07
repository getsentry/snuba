from typing import Sequence

from snuba.migrations import migration, operations
from snuba.migrations.snuba_migrations.metrics.templates import create_views, drop_views


class Migration(migration.ClickhouseNodeMigration):
    """
    Create materialized views for metrics with a 10 second granularity
    in addition to the existing 60 seconds views.

    The backward migration does *not* delete any data from the destination tables.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return create_views(granularity=10)

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return drop_views(granularity=10)

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
