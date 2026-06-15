"""Shared helpers for events_analytics_platform migrations.

Many EAP migrations regenerate the ``eap_items`` downsample materialized
views. Historically each migration copy-pasted the full column list, the
``SELECT`` expression, and the create/drop loop. This module centralises
those pieces so a new migration only has to describe what actually changes
(usually the sampling predicate and the version it bumps to).
"""

from typing import Callable, List, Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.schemas import UUID, Bool, DateTime, Float, Int, Map, String

EAP_STORAGE_SET_KEY = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

#: Number of bucketed attribute map columns (``attributes_string_0`` ..) the
#: ``eap_items`` schema uses.
DEFAULT_NUM_ATTR_BUCKETS = 40

#: Sampling weights for the downsample tiers. Each weight divides the next,
#: which is what lets a trace-based hash keep the tiers as nested subsets.
SAMPLING_WEIGHTS = [8, 8**2, 8**3]

#: The source table the downsample materialized views read from.
EAP_ITEMS_LOCAL_TABLE = "eap_items_1_local"

#: Columns that are re-projected (transformed) by the downsample views rather
#: than copied straight through, so they are dropped from the passthrough
#: ``SELECT`` column list.
TRANSFORMED_COLUMNS = frozenset(
    {
        "sampling_weight",
        "sampling_factor",
        "retention_days",
        "client_sample_rate",
        "server_sample_rate",
    }
)


def get_eap_items_columns(
    num_attr_buckets: int = DEFAULT_NUM_ATTR_BUCKETS,
) -> List[Column[Modifiers]]:
    """Return a fresh ``eap_items`` column list.

    A new list is returned on every call so callers can freely append or
    tweak columns without mutating shared state.
    """
    columns: List[Column[Modifiers]] = [
        Column("organization_id", UInt(64)),
        Column("project_id", UInt(64)),
        Column("item_type", UInt(8)),
        Column("timestamp", DateTime(Modifiers(codecs=["DoubleDelta", "ZSTD(1)"]))),
        Column("trace_id", UUID()),
        Column("item_id", UInt(128)),
        Column("sampling_weight", UInt(64, modifiers=Modifiers(codecs=["ZSTD(1)"]))),
        Column("sampling_factor", Float(64, modifiers=Modifiers(codecs=["ZSTD(1)"]))),
        Column("retention_days", UInt(16, modifiers=Modifiers(codecs=["T64", "ZSTD(1)"]))),
        Column("attributes_bool", Map(String(), Bool())),
        Column("attributes_int", Map(String(), Int(64))),
    ]
    columns.extend(
        Column(
            f"attributes_string_{i}",
            Map(String(), String(), modifiers=Modifiers(codecs=["ZSTD(1)"])),
        )
        for i in range(num_attr_buckets)
    )
    columns.extend(
        Column(
            f"attributes_float_{i}",
            Map(String(), Float(64), modifiers=Modifiers(codecs=["ZSTD(1)"])),
        )
        for i in range(num_attr_buckets)
    )
    return columns


def downsample_mv_select(
    columns: Sequence[Column[Modifiers]],
    sampling_weight: int,
    *,
    where_predicate: str,
    source_table: str = EAP_ITEMS_LOCAL_TABLE,
    include_sample_rates: bool = True,
) -> str:
    """Build the ``SELECT`` body of a downsample materialized view.

    ``columns`` is the destination column list; the transformed columns
    (see :data:`TRANSFORMED_COLUMNS`) are projected explicitly and the rest
    are copied through.

    ``where_predicate`` is the body of the sampling filter, e.g.
    ``f"cityHash64(reinterpretAsUInt128(trace_id)) % {sampling_weight}"``;
    it is wrapped as ``WHERE ({where_predicate}) = 0``.

    ``include_sample_rates`` controls whether ``client_sample_rate`` and
    ``server_sample_rate`` are scaled in the projection (older views did not
    carry them).
    """
    passthrough = ", ".join(c.name for c in columns if c.name not in TRANSFORMED_COLUMNS)
    select_terms = [
        f"{passthrough},",
        "downsampled_retention_days AS retention_days,",
        f"sampling_weight * {sampling_weight} AS sampling_weight,",
        f"sampling_factor / {sampling_weight} AS sampling_factor"
        + ("," if include_sample_rates else ""),
    ]
    if include_sample_rates:
        select_terms.extend(
            [
                f"client_sample_rate / {sampling_weight} AS client_sample_rate,",
                f"server_sample_rate / {sampling_weight} AS server_sample_rate",
            ]
        )
    return " ".join(
        [
            "SELECT",
            *select_terms,
            f"FROM {source_table}",
            f"WHERE ({where_predicate}) = 0",
        ]
    )


def swap_downsample_materialized_views(
    *,
    columns: Sequence[Column[Modifiers]],
    create_version: int,
    drop_version: int,
    query_for_weight: Callable[[int], str],
    sampling_weights: Sequence[int] = SAMPLING_WEIGHTS,
    storage_set: StorageSetKey = EAP_STORAGE_SET_KEY,
    table_prefix: str = "eap_items_1_downsample",
) -> List[SqlOperation]:
    """Create one downsample materialized view per sampling weight and drop
    the previous version.

    Both ``forwards_ops`` and ``backwards_ops`` are this same operation with
    the create/drop versions and the query swapped, so a migration can call
    this for each direction.
    """
    ops: List[SqlOperation] = []
    for sampling_weight in sampling_weights:
        view_base = f"{table_prefix}_{sampling_weight}"
        ops.extend(
            [
                operations.CreateMaterializedView(
                    storage_set=storage_set,
                    view_name=f"{view_base}_mv_{create_version}",
                    columns=list(columns),
                    destination_table_name=f"{view_base}_local",
                    target=OperationTarget.LOCAL,
                    query=query_for_weight(sampling_weight),
                ),
                operations.DropTable(
                    storage_set=storage_set,
                    table_name=f"{view_base}_mv_{drop_version}",
                    target=OperationTarget.LOCAL,
                ),
            ]
        )
    return ops
