from typing import Sequence

from snuba.clickhouse.columns import Column, Float
from snuba.migrations import migration, operations
from snuba.migrations.snuba_migrations.metrics.templates import (
    get_forward_bucket_table_dist,
    get_forward_bucket_table_local,
    get_reverse_table_migration,
)


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return get_forward_bucket_table_local(
            table_name="metrics_counters_buckets_local",
            value_cols=[Column("value", Float(64))],
        )

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return get_reverse_table_migration("metrics_counters_buckets_local")

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return get_forward_bucket_table_dist(
            local_table_name="metrics_counters_buckets_local",
            dist_table_name="metrics_counters_buckets_dist",
            value_cols=[Column("value", Float(64))],
        )

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return get_reverse_table_migration("metrics_counters_buckets_dist")
