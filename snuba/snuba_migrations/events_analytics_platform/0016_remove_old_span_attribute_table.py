from __future__ import annotations

from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.schemas import AggregateFunction, Column, DateTime, String, UInt


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

    meta_local_table_name = "spans_attributes_meta_local"
    meta_dist_table_name = "spans_attributes_meta_dist"
    meta_table_columns: Sequence[Column[Modifiers]] = [
        Column("organization_id", UInt(64)),
        Column("attribute_type", String()),
        Column("attribute_key", String()),
        Column("attribute_value", String()),
        Column("timestamp", DateTime(modifiers=Modifiers(codecs=["DoubleDelta"]))),
        Column("retention_days", UInt(16)),
        Column("count", AggregateFunction("sum", [UInt(64)])),
    ]

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.meta_local_table_name,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.meta_dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.meta_local_table_name,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=self.storage_set_key,
                    primary_key="(organization_id, attribute_key)",
                    order_by="(organization_id, attribute_key, attribute_value, timestamp)",
                    partition_by="toMonday(timestamp)",
                    settings={
                        "index_granularity": self.granularity,
                        # Since the partitions contain multiple retention periods, need to ensure
                        # that rows within partitions are dropped
                        "ttl_only_drop_parts": 0,
                    },
                    ttl="timestamp + toIntervalDay(retention_days)",
                ),
                columns=self.meta_table_columns,
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.meta_dist_table_name,
                engine=table_engines.Distributed(
                    local_table_name=self.meta_local_table_name, sharding_key=None
                ),
                columns=self.meta_table_columns,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]
