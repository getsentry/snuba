from __future__ import annotations

from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.constants import ITEM_ATTRIBUTE_BUCKETS
from snuba.utils.schemas import Column, DateTime, String, UInt


class Migration(migration.ClickhouseNodeMigration):
    """
    This migration creates a table meant to store just the attributes seen in a particular org.

    * attr_type can either be "string" or "float"
    * attr_value is null for float attributes
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
        Column(
            "attr_value", String(modifiers=Modifiers(codecs=["ZSTD(1)"], nullable=True))
        ),
    ]

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.local_table,
                engine=table_engines.ReplacingMergeTree(
                    storage_set=self.storage_set_key,
                    primary_key="(organization_id, project_id, timestamp, item_type, attr_key)",
                    order_by="(organization_id, project_id, timestamp, item_type, attr_key, attr_type, attr_value, retention_days)",
                    partition_by="(retention_days, toMonday(timestamp))",
                    settings={
                        "index_granularity": self.granularity,
                    },
                    ttl="timestamp + toIntervalDay(retention_days)",
                ),
                columns=self.columns,
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.dist_table,
                engine=table_engines.Distributed(
                    local_table_name=self.local_table, sharding_key=None
                ),
                columns=self.columns,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.mv,
                columns=self.columns,
                destination_table_name=self.local_table,
                target=OperationTarget.LOCAL,
                query=f"""
SELECT
    organization_id,
    project_id,
    item_type,
    attrs.1 as attr_key,
    attrs.2 as attr_value,
    attrs.3 as attr_type,
    toStartOfWeek(timestamp) AS timestamp,
    retention_days,
FROM eap_items_1_local
LEFT ARRAY JOIN
    arrayConcat(
        {", ".join(f"arrayMap(x -> tuple(x.1, x.2, 'string'), CAST(attributes_string_{n}, 'Array(Tuple(String, String))'))" for n in range(ITEM_ATTRIBUTE_BUCKETS))},
        {",".join(f"arrayMap(x -> tuple(x, Null, 'float'), mapKeys(attributes_float_{n}))" for n in range(ITEM_ATTRIBUTE_BUCKETS))}
    ) AS attrs
GROUP BY
    organization_id,
    project_id,
    item_type,
    attr_key,
    attr_value,
    attr_type,
    timestamp,
    retention_days
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
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.local_table,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.dist_table,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]
