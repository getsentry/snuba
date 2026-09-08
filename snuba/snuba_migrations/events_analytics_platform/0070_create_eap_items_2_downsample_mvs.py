"""Create the eap_items_2 downsample materialized views.

Step 2 of moving eap_items onto a ReplacingMergeTree keyed on `version`:
0069 created the eap_items_2 tables, this wires up the views that populate
the downsample tiers from eap_items_2_local.

Creating a view has no effect until something inserts into its source table,
and nothing writes to eap_items_2_local yet, so this is inert on merge.

The projected column set is declared here rather than derived from the
eap_items_1 views. The tables are a clean break: if a later migration
changes what the v1 views project, v2 should not silently follow. The set is
currently identical to v1's mv_10 by construction, and the accompanying
verification asserts that, but the two are free to diverge from here.
"""

from collections.abc import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.snuba_migrations.events_analytics_platform.templates import (
    SAMPLING_WEIGHTS,
    downsample_mv_select,
    get_eap_items_columns,
)
from snuba.utils.schemas import UUID, Array, Bool, Float, Int, Map, String

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

source_table = "eap_items_2_local"
table_prefix = "eap_items_2_downsample"

# First view generation for these tables. The eap_items_1 views are on mv_10
# after nine rounds of schema retrofits; v2 starts its own numbering at 1
# since the version is per table prefix and cannot collide.
mv_version = 1

_codec = Modifiers(codecs=["ZSTD(1)"])

array_attribute_columns: list[Column[Modifiers]] = [
    Column("attributes_array_string", Map(String(), Array(String()), modifiers=_codec)),
    Column("attributes_array_int", Map(String(), Array(Int(64)), modifiers=_codec)),
    Column("attributes_array_float", Map(String(), Array(Float(64)), modifiers=_codec)),
    Column("attributes_array_bool", Map(String(), Array(Bool()), modifiers=_codec)),
]
session_id: Column[Modifiers] = Column("session_id", UUID())
ai_conversation_id: Column[Modifiers] = Column("ai_conversation_id", String(modifiers=_codec))

# Declared without the destination table's DEFAULT, matching how the other
# view columns are declared (retention_days likewise omits its DEFAULT 30).
# The view always projects an explicit value, so the default is never used.
version_column: Column[Modifiers] = Column("version", UInt(64, modifiers=_codec))


def _mv_columns() -> list[Column[Modifiers]]:
    """The columns the eap_items_2 downsample views project.

    session_id and ai_conversation_id sit after trace_id to mirror the
    physical column order of the destination tables. Ordering is cosmetic --
    ClickHouse matches a view's output to its target table by name, which is
    already relied on because downsample_mv_select() emits the transformed
    columns after the passthrough ones.
    """
    columns = get_eap_items_columns()
    columns.extend(array_attribute_columns)
    at = next(i for i, c in enumerate(columns) if c.name == "trace_id") + 1
    return columns[:at] + [session_id, ai_conversation_id] + columns[at:] + [version_column]


def _query(sampling_weight: int) -> str:
    # Hashing item_id (not trace_id) keeps the tiers nested subsets of one
    # another, as established by 0058_nest_downsample_tiers. `version` is not
    # in TRANSFORMED_COLUMNS, so it is copied straight through from the
    # source row rather than being regenerated for the derived row.
    return downsample_mv_select(
        _mv_columns(),
        sampling_weight,
        where_predicate=f"cityHash64(item_id) % {sampling_weight}",
        source_table=source_table,
    )


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        columns = _mv_columns()
        return [
            operations.CreateMaterializedView(
                storage_set=storage_set,
                view_name=f"{table_prefix}_{sampling_weight}_mv_{mv_version}",
                columns=columns,
                destination_table_name=f"{table_prefix}_{sampling_weight}_local",
                target=OperationTarget.LOCAL,
                query=_query(sampling_weight),
            )
            for sampling_weight in SAMPLING_WEIGHTS
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=storage_set,
                table_name=f"{table_prefix}_{sampling_weight}_mv_{mv_version}",
                target=OperationTarget.LOCAL,
            )
            for sampling_weight in SAMPLING_WEIGHTS
        ]
