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
# master is at mv_5 (migration 0057_add_name_column_and_index added the
# `indexed_name` column to the downsample MVs), so this migration bumps 5 -> 6.
# The restored `old_version` (mv_5) keeps the perturbed hash, matching the
# definition currently on master.
old_version = 5
new_version = old_version + 1

columns = get_eap_items_columns()


# Per-item sampling on `item_id`, but the hash is perturbed by the sampling
# weight (`item_id + weight`). Because each tier hashes a different value,
# the tiers pick independently and are not subsets of each other: an item
# can land in tier 8 but not tier 64.
def generate_old_materialized_view_expression(sampling_weight: int) -> str:
    return downsample_mv_select(
        columns,
        sampling_weight,
        where_predicate=f"cityHash64(item_id + {sampling_weight}) % {sampling_weight}",
    )


# Per-item sampling on `item_id` with a single, un-perturbed hash. Sampling
# stays independent across items (so the extrapolation variance math keeps
# its Bernoulli-independence assumption), but because the sampling weights
# are 8 / 64 / 512 (each divides the next) and every tier hashes the same
# value, an item that satisfies `H % 512 == 0` also satisfies `H % 64 == 0`
# and `H % 8 == 0`. That makes the tiers strict subsets: tier 512 ⊆ tier 64
# ⊆ tier 8.
def generate_new_materialized_view_expression(sampling_weight: int) -> str:
    return downsample_mv_select(
        columns,
        sampling_weight,
        where_predicate=f"cityHash64(item_id) % {sampling_weight}",
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
