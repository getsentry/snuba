from __future__ import annotations

from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.constants import ATTRIBUTE_BUCKETS
from snuba.utils.schemas import (
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

    str_mv = "ourlogs_str_attrs_1_mv"
    str_local_table = "ourlogs_str_attrs_1_local"
    str_dist_table = "ourlogs_str_attrs_1_dist"
    str_columns: Sequence[Column[Modifiers]] = [
        Column("organization_id", UInt(64)),
        Column("project_id", UInt(64)),
        Column("attr_key", String(modifiers=Modifiers(codecs=["ZSTD(1)"]))),
        Column("attr_value", String(modifiers=Modifiers(codecs=["ZSTD(1)"]))),
        Column(
            "timestamp",
            DateTime(modifiers=Modifiers(codecs=["DoubleDelta", "ZSTD(1)"])),
        ),
        Column("retention_days", UInt(16)),
        Column("count", SimpleAggregateFunction("sum", [UInt(64)])),
    ]

    num_mv = "ourlogs_num_attrs_1_mv"
    num_local_table = "ourlogs_num_attrs_1_local"
    num_dist_table = "ourlogs_num_attrs_1_dist"
    num_columns: Sequence[Column[Modifiers]] = [
        Column("organization_id", UInt(64)),
        Column("project_id", UInt(64)),
        Column("attr_key", String(modifiers=Modifiers(codecs=["ZSTD(1)"]))),
        Column("attr_min_value", SimpleAggregateFunction("min", [Float(64)])),
        Column("attr_max_value", SimpleAggregateFunction("max", [Float(64)])),
        Column(
            "timestamp",
            DateTime(modifiers=Modifiers(codecs=["DoubleDelta", "ZSTD(1)"])),
        ),
        Column("retention_days", UInt(16)),
        Column("count", SimpleAggregateFunction("sum", [UInt(64)])),
    ]

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.str_local_table,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=self.storage_set_key,
                    primary_key="(organization_id, attr_key)",
                    order_by="(organization_id, attr_key, attr_value, project_id, timestamp, retention_days)",
                    partition_by="(retention_days, toMonday(timestamp))",
                    settings={
                        "index_granularity": self.granularity,
                    },
                    ttl="timestamp + toIntervalDay(retention_days)",
                ),
                columns=self.str_columns,
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.str_dist_table,
                engine=table_engines.Distributed(
                    local_table_name=self.str_local_table,
                    sharding_key="rand()",
                ),
                columns=self.str_columns,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.str_mv,
                columns=self.str_columns,
                destination_table_name=self.str_local_table,
                target=OperationTarget.LOCAL,
                query=f"""
SELECT
    organization_id,
    project_id,
    attrs.1 as attr_key,
    attrs.2 as attr_value,
    toStartOfDay(timestamp) AS timestamp,
    retention_days,
    1 AS count
FROM ourlogs_3_local
LEFT ARRAY JOIN
    arrayConcat(
        {", ".join(f"CAST(attr_str_{n}, 'Array(Tuple(String, String))')" for n in range(ATTRIBUTE_BUCKETS))}
    ) AS attrs
GROUP BY
    organization_id,
    project_id,
    attr_key,
    attr_value,
    timestamp,
    retention_days
""",
            ),
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.num_local_table,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=self.storage_set_key,
                    primary_key="(organization_id, attr_key)",
                    order_by="(organization_id, attr_key, timestamp, project_id, retention_days)",
                    partition_by="(retention_days, toMonday(timestamp))",
                    settings={
                        "index_granularity": self.granularity,
                    },
                    ttl="timestamp + toIntervalDay(retention_days)",
                ),
                columns=self.num_columns,
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.num_dist_table,
                engine=table_engines.Distributed(
                    local_table_name=self.num_local_table, sharding_key=None
                ),
                columns=self.num_columns,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.num_mv,
                columns=self.num_columns,
                destination_table_name=self.num_local_table,
                target=OperationTarget.LOCAL,
                query=f"""
SELECT
    organization_id,
    project_id,
    attrs.1 as attr_key,
    attrs.2 as attr_min_value,
    attrs.2 as attr_max_value,
    toStartOfDay(timestamp) AS timestamp,
    retention_days,
    1 AS count
FROM ourlogs_3_local
LEFT ARRAY JOIN
    arrayConcat(
        {",".join(f"CAST(attr_num_{n}, 'Array(Tuple(String, Float64))')" for n in range(ATTRIBUTE_BUCKETS))}
    ) AS attrs
GROUP BY
    organization_id,
    project_id,
    attrs.1,
    attrs.2,
    timestamp,
    retention_days
""",
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.str_mv,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.str_local_table,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.str_dist_table,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.num_mv,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.num_local_table,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.num_dist_table,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]
