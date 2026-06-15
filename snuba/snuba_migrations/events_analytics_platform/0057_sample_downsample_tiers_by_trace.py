from typing import Sequence

from snuba.migrations import migration
from snuba.migrations.operations import SqlOperation
from snuba.snuba_migrations.events_analytics_platform.templates import (
    SAMPLING_WEIGHTS,
    downsample_mv_select,
    get_eap_items_columns,
    swap_downsample_materialized_views,
)

sampling_weights = SAMPLING_WEIGHTS
old_version = 4
new_version = old_version + 1

columns = get_eap_items_columns()


# Per-item sampling on `item_id`. Each tier picks independently so the tiers
# are not subsets of each other, and items belonging to the same trace can
# end up in different tiers.
def generate_old_materialized_view_expression(sampling_weight: int) -> str:
    return downsample_mv_select(
        columns,
        sampling_weight,
        where_predicate=f"cityHash64(item_id + {sampling_weight}) % {sampling_weight}",
    )


# Trace-based sampling that keeps each tier a strict subset of the tier
# above. Hashing `trace_id` (and not perturbing the hash per tier) makes
# every item in a trace land in the same set of tiers, and because the
# sampling weights are 8 / 64 / 512 (each divides the next), an item that
# satisfies `H % 512 == 0` also satisfies `H % 64 == 0` and `H % 8 == 0`.
def generate_new_materialized_view_expression(sampling_weight: int) -> str:
    return downsample_mv_select(
        columns,
        sampling_weight,
        where_predicate=f"cityHash64(reinterpretAsUInt128(trace_id)) % {sampling_weight}",
    )


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return swap_downsample_materialized_views(
            columns=columns,
            create_version=new_version,
            drop_version=old_version,
            query_for_weight=generate_new_materialized_view_expression,
        )

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return swap_downsample_materialized_views(
            columns=columns,
            create_version=old_version,
            drop_version=new_version,
            query_for_weight=generate_old_materialized_view_expression,
        )
