"""
this recreates the mv from 0033 that was reverted in 0035
but adds a group by to it to remove duplicates.
"""

from __future__ import annotations

from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.constants import ITEM_ATTRIBUTE_BUCKETS
from snuba.utils.schemas import Column, DateTime, String, UInt


class Migration(migration.ClickhouseNodeMigration):
    """
    This migration creates a table meant to store just the attributes seen in a particular org.

    * attr_type can either be "string" or "float"
    * attr_value is always an empty string for float attributes
    """

    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    granularity = "8192"

    mv = "items_attrs_1_mv"
    local_table = "items_attrs_1_local"
    dist_table = "items_attrs_1_dist"
    columns: Sequence[Column[Modifiers]] = [
        Column("organization_id", UInt(64)),
        Column("project_id", UInt(64)),
        Column("item_type", UInt(8)),
        Column("attr_key", String(modifiers=Modifiers(codecs=["ZSTD(1)"]))),
        Column("attr_type", String(Modifiers(low_cardinality=True))),
        Column(
            "timestamp",
            DateTime(modifiers=Modifiers(codecs=["DoubleDelta", "ZSTD(1)"])),
        ),
        Column("retention_days", UInt(16)),
        Column("attr_value", String(modifiers=Modifiers(codecs=["ZSTD(1)"]))),
    ]

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.mv,
                columns=self.columns,
                destination_table_name=self.local_table,
                target=OperationTarget.LOCAL,
                query=f"""
SELECT DISTINCT
    organization_id,
    project_id,
    item_type,
    attrs.1 as attr_key,
    attrs.2 as attr_value,
    attrs.3 as attr_type,
    toStartOfWeek(timestamp) AS ts,
    retention_days,
FROM eap_items_1_local
ARRAY JOIN
    arrayConcat(
        {", ".join(f"arrayMap(x -> tuple(x.1, x.2, 'string'), CAST(attributes_string_{n}, 'Array(Tuple(String, String))'))" for n in range(ITEM_ATTRIBUTE_BUCKETS))},
        {",".join(f"arrayMap(x -> tuple(x, '', 'float'), mapKeys(attributes_float_{n}))" for n in range(ITEM_ATTRIBUTE_BUCKETS))}
    ) AS attrs
""",
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.mv,
                target=OperationTarget.LOCAL,
            ),
        ]
