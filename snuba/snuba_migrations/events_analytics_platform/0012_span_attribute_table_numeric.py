from __future__ import annotations

from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.constants import ATTRIBUTE_BUCKETS
from snuba.utils.schemas import (
    UUID,
    Column,
    DateTime,
    Float,
    SimpleAggregateFunction,
    String,
    UInt,
)


class Migration(migration.ClickhouseNodeMigration):
    """
    This migration creates a table meant to store just the attributes seen in a particular org.
    The table is populated by a separate materialized view for each type of attribute.
    """

    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    granularity = "8192"

    meta_view_name = "spans_num_attrs_mv"
    meta_local_table_name = "spans_num_attrs_local"
    meta_dist_table_name = "spans_num_attrs_dist"
    meta_table_columns: Sequence[Column[Modifiers]] = [
        Column("organization_id", UInt(64)),
        Column(
            "trace_id", UUID()
        ),  # recommended by altinity, this lets us find traces which have k=v set
        Column("project_id", UInt(64)),
        Column("attr_key", String()),
        Column("attr_value", Float(64)),
        Column("timestamp", DateTime(modifiers=Modifiers(codecs=["ZSTD(1)"]))),
        Column("retention_days", UInt(16)),
        Column("duration_ms", SimpleAggregateFunction("max", [UInt(32)])),
        Column("count", SimpleAggregateFunction("sum", [UInt(64)])),
    ]

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.meta_local_table_name,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=self.storage_set_key,
                    primary_key="(organization_id, attr_key)",
                    order_by="(organization_id, attr_key, attr_value, timestamp, trace_id, project_id, retention_days)",
                    partition_by="toMonday(timestamp)",
                    settings={
                        "index_granularity": self.granularity,
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
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.meta_view_name,
                columns=self.meta_table_columns,
                destination_table_name=self.meta_local_table_name,
                target=OperationTarget.LOCAL,
                query=f"""
SELECT
    organization_id,
    project_id,
    trace_id,
    attrs.1 as attr_key,
    attrs.2 as attr_value,
    toStartOfDay(_sort_timestamp) AS timestamp,
    retention_days,
    1 AS count,
    maxSimpleState(duration_ms)
FROM eap_spans_local
LEFT ARRAY JOIN
    arrayConcat({",".join(f"CAST(attr_num_{n}, 'Array(Tuple(String, Float64))')" for n in range(ATTRIBUTE_BUCKETS))}) AS attrs
GROUP BY
    organization_id,
    project_id,
    trace_id,
    attr_key,
    attr_value,
    timestamp,
    retention_days
""",
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.meta_view_name,
                target=OperationTarget.LOCAL,
            ),
            operations.TruncateTable(
                storage_set=self.storage_set_key,
                table_name=self.meta_local_table_name,
                target=OperationTarget.LOCAL,
            ),
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
