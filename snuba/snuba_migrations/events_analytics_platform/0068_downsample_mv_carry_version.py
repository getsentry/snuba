from collections.abc import Callable

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import SqlOperation
from snuba.snuba_migrations.events_analytics_platform.templates import (
    downsample_mv_select,
    get_eap_items_columns,
    swap_downsample_materialized_views,
)
from snuba.utils.schemas import UUID, Array, Bool, Float, Int, Map, String

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

# mv_9 (migration 0061) -> mv_10: additionally project `version`.
#
# Migrations 0064-0067 added the `version` column to eap_items_1_local and to
# each downsample tier's local table, but the downsample materialized views
# were left projecting the mv_9 column set. Because an MV inserts only the
# columns its SELECT produces, every row the views write currently lands with
# `version` resolved from the destination table's DEFAULT (0) rather than the
# value on the source row.
#
# That is wrong once the tiers move to ReplacingMergeTree(version): a
# downsampled row is a copy of a source row and has to carry the source's
# version, otherwise the tiers dedupe on a column that is 0 for everything and
# the tier disagrees with the base table about which copy of an item wins.
#
# `version` is not in TRANSFORMED_COLUMNS, so adding it to the column list is
# enough for downsample_mv_select() to copy it straight through.
mv_old_version = 9
mv_new_version = mv_old_version + 1

_codec = Modifiers(codecs=["ZSTD(1)"])

# The mv_9 column set, reconstructed exactly as 0061 assembled it: the base
# columns, plus the array attribute maps from 0059, plus session_id (0060) and
# ai_conversation_id (0061) inserted after trace_id.
array_attribute_columns: list[Column[Modifiers]] = [
    Column("attributes_array_string", Map(String(), Array(String()), modifiers=_codec)),
    Column("attributes_array_int", Map(String(), Array(Int(64)), modifiers=_codec)),
    Column("attributes_array_float", Map(String(), Array(Float(64)), modifiers=_codec)),
    Column("attributes_array_bool", Map(String(), Array(Bool()), modifiers=_codec)),
]
session_id: Column[Modifiers] = Column("session_id", UUID())
ai_conversation_id: Column[Modifiers] = Column("ai_conversation_id", String(modifiers=_codec))

# Declared without the destination table's DEFAULT, matching how the existing
# view columns are declared (e.g. retention_days omits its DEFAULT 30). The
# view always projects an explicit value, so the default is never consulted.
version_column: Column[Modifiers] = Column("version", UInt(64, modifiers=_codec))


def _mv9_columns() -> list[Column[Modifiers]]:
    columns = get_eap_items_columns()
    columns.extend(array_attribute_columns)
    at = next(i for i, c in enumerate(columns) if c.name == "trace_id") + 1
    return columns[:at] + [session_id, ai_conversation_id] + columns[at:]


def _mv10_columns() -> list[Column[Modifiers]]:
    # Appended last to mirror the destination tables, where 0065-0067 added
    # `version` at the end. Ordering is cosmetic: ClickHouse matches a
    # materialized view's output to its target table by column name, which is
    # already relied on here since downsample_mv_select() emits the
    # transformed columns after the passthrough ones.
    return _mv9_columns() + [version_column]


def _query_for_weight(columns: list[Column[Modifiers]]) -> Callable[[int], str]:
    def inner(sampling_weight: int) -> str:
        return downsample_mv_select(
            columns,
            sampling_weight,
            where_predicate=f"cityHash64(item_id) % {sampling_weight}",
        )

    return inner


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> list[SqlOperation]:
        columns = _mv10_columns()
        return list(
            swap_downsample_materialized_views(
                columns=columns,
                create_version=mv_new_version,
                drop_version=mv_old_version,
                query_for_weight=_query_for_weight(columns),
            )
        )

    def backwards_ops(self) -> list[SqlOperation]:
        columns = _mv9_columns()
        return list(
            swap_downsample_materialized_views(
                columns=columns,
                create_version=mv_old_version,
                drop_version=mv_new_version,
                query_for_weight=_query_for_weight(columns),
            )
        )
